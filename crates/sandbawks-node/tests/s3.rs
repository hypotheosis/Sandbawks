//! Property-based tests for the chunked S3 sink/source.
//!
//! Plan 05 T6 (#142) requires that `S3SnapshotSink` + `S3SnapshotSource`
//! round-trip arbitrary byte vectors byte-for-byte through the
//! HEAD-before-PUT, content-addressed chunk storage layout. We exercise
//! the contract here against the in-memory [`InMemoryS3`] double rather
//! than a real MinIO container — the live S3 integration test lives in
//! Plan 05 T14.
//!
//! Two layers of coverage:
//!
//! 1. A Hegel property test on bounded random payloads (well above the
//!    4 MiB chunk boundary, but well below the 32 MiB-per-part hard cap
//!    so 256 hegel cases don't allocate gigabytes).
//! 2. An explicit end-to-end test against a single 32 MiB payload —
//!    the spec's hard upper bound — covering the production-scale
//!    chunk count without paying for it on every hegel case.

use std::sync::Arc;

use hegel::generators;
use hegel::TestCase;

use sandbawks_node::s3::{InMemoryS3, S3SnapshotSink, S3SnapshotSource, CHUNK_SIZE};
use sandbawks_types::snapshot::{aggregate_sha256, SnapshotPart, SnapshotSink, SnapshotSource};
use sandbawks_types::SnapshotManifest;

/// Per-case payload cap for the hegel test.
///
/// Two-and-a-bit chunks: large enough to exercise the multi-chunk loop
/// and the "last chunk smaller than `CHUNK_SIZE`" branch, small enough
/// that 256 hegel cases at this max only allocate ~2 GiB across the
/// whole test run (the case sizes are drawn from `0..=max`, so the
/// expected total is roughly half that).
const MAX_PAYLOAD_LEN: usize = 2 * CHUNK_SIZE + 17;

/// Per-case part-name length cap. Production part names are 10–30 chars
/// (`memory.zst`, `rootfs-delta.tar.zst`, `envd-state.json`); 24 is
/// plenty.
const MAX_NAME_LEN: usize = 24;

/// Maximum parts per generated manifest. Production manifests cap at 3
/// (memory + rootfs + envd state); 4 keeps the test bounded while
/// covering a manifest larger than the production shape.
const MAX_PARTS: usize = 4;

/// Generate a random byte vector up to [`MAX_PAYLOAD_LEN`].
#[hegel::composite]
fn payload(tc: TestCase) -> Vec<u8> {
    tc.draw(generators::vecs(generators::integers::<u8>()).max_size(MAX_PAYLOAD_LEN))
}

/// Generate a non-empty part name from a restricted ASCII alphabet so
/// keys round-trip through Vec→HashMap→string without colliding with
/// the layout's literal `chunks/` / `parts/` segments.
///
/// We use `text()` directly per the precedent in
/// `sandbawks-types/tests/snapshot_store.rs`; collisions with reserved
/// layout segments are statistically impossible at these lengths and
/// the trait doesn't enforce part-name validity anyway.
#[hegel::composite]
fn part_name(tc: TestCase) -> String {
    let raw = tc.draw(generators::text().max_size(MAX_NAME_LEN));
    if raw.is_empty() {
        "p".to_owned()
    } else {
        raw
    }
}

#[hegel::composite]
fn name_payload_pairs(tc: TestCase) -> Vec<(String, Vec<u8>)> {
    // Dedup by name so multiple `put_part(same_name, ...)` calls don't
    // step on each other (last-write-wins is the trait's contract but
    // not what we're testing here).
    let pairs: Vec<(String, Vec<u8>)> =
        tc.draw(generators::vecs(generators::tuples!(part_name(), payload())).max_size(MAX_PARTS));
    let mut seen = std::collections::HashSet::<String>::new();
    pairs
        .into_iter()
        .filter(|(name, _)| seen.insert(name.clone()))
        .collect()
}

#[hegel::test]
fn parts_round_trip_byte_identical(tc: TestCase) {
    // tokio runtime is set up per case so the async trait methods drive
    // without sprinkling `#[tokio::test]` on every hegel invocation.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    rt.block_on(async {
        let pairs = tc.draw(name_payload_pairs());
        let store = Arc::new(InMemoryS3::new());
        let sink = S3SnapshotSink::new(store.clone(), "test-bucket", "ns/sbx-prop/2026");
        let source = S3SnapshotSource::new(store.clone(), "ns/sbx-prop/2026");

        let mut recorded: Vec<SnapshotPart> = Vec::with_capacity(pairs.len());
        for (name, bytes) in &pairs {
            let part = sink
                .put_part(name, bytes.clone())
                .await
                .expect("S3SnapshotSink::put_part should succeed against InMemoryS3");
            assert_eq!(part, SnapshotPart::from_bytes(name, bytes));
            assert_eq!(part.size_bytes as usize, bytes.len());
            recorded.push(part);
        }

        // Byte-identity check on every part.
        for (name, bytes) in &pairs {
            let fetched = source
                .get_part(name)
                .await
                .expect("S3SnapshotSource::get_part should succeed");
            assert_eq!(&fetched, bytes, "byte identity for part {name}");
        }

        // Manifest round-trip: the sink-recorded parts + aggregate
        // sha256 must survive a put_manifest / get_manifest cycle.
        let aggregate = aggregate_sha256(&recorded);
        let manifest = SnapshotManifest {
            envd_sha256: String::new(),
            runsc_version: String::new(),
            envd_captured: false,
            sandbox_id: "sbx-prop".into(),
            runtime_class: "gvisor".into(),
            sha256: aggregate,
            created_at: "2026-05-25T00:00:00Z".into(),
            parts: recorded,
            snapshot_ref: "s3://test-bucket/ns/sbx-prop/2026/manifest.json".into(),
            template_ref: String::new(),
            template_image: String::new(),
        };
        sink.put_manifest(&manifest).await.expect("put_manifest");
        let fetched = source.get_manifest().await.expect("get_manifest");
        assert_eq!(fetched, manifest);
    });
}

#[hegel::test]
fn idempotent_retry_is_a_noop(tc: TestCase) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    rt.block_on(async {
        let pairs = tc.draw(name_payload_pairs());
        let store = Arc::new(InMemoryS3::new());
        let sink = S3SnapshotSink::new(store.clone(), "test-bucket", "ns/sbx-retry/2026");

        // First pass.
        for (name, bytes) in &pairs {
            sink.put_part(name, bytes.clone()).await.expect("first put");
        }
        let after_first = store.len();

        // Second pass — same inputs, must HEAD-then-skip every chunk
        // and overwrite (without growing) every part index.
        for (name, bytes) in &pairs {
            sink.put_part(name, bytes.clone()).await.expect("retry put");
        }
        let after_second = store.len();
        assert_eq!(
            after_first, after_second,
            "idempotent retry must not grow the store",
        );
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn round_trip_at_32_mib_hard_cap() {
    // The Plan 05 T6 spec calls out 32 MiB as the per-part upper bound
    // the chunked transfer is required to handle. Exercise that on a
    // single explicit case — outside the hegel loop so we only pay the
    // 32 MiB allocation once.
    const TOTAL_LEN: usize = 32 * 1024 * 1024;
    // Deterministic but not all-zeros so a single-byte shuffling bug at
    // the assembly site would be observable.
    let payload: Vec<u8> = (0..TOTAL_LEN).map(|i| (i % 251) as u8).collect();

    let store = Arc::new(InMemoryS3::new());
    let sink = S3SnapshotSink::new(store.clone(), "test-bucket", "ns/sbx-32mib/2026");
    let source = S3SnapshotSource::new(store.clone(), "ns/sbx-32mib/2026");

    let part = sink
        .put_part("memory.zst", payload.clone())
        .await
        .expect("put_part 32 MiB");
    assert_eq!(part.size_bytes as usize, TOTAL_LEN);
    assert_eq!(
        part.sha256,
        SnapshotPart::from_bytes("memory.zst", &payload).sha256
    );

    // Eight 4 MiB chunks + one part index = 9 objects total.
    assert_eq!(store.len(), 9);

    let got = source
        .get_part("memory.zst")
        .await
        .expect("get_part 32 MiB");
    assert_eq!(got.len(), TOTAL_LEN);
    assert_eq!(got, payload);
}
