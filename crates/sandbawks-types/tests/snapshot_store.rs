//! Property-based tests for the snapshot store contract.
//!
//! Plan 05 T3 (#139) requires the `FakeSnapshotStore` round-trip to
//! preserve byte identity *and* the per-part / aggregate sha256s. The
//! test below uses `hegel` to drive arbitrary `(part-name, bytes)`
//! pairs through `SnapshotSink::put_part` + `SnapshotSource::get_part`
//! and asserts both invariants.
//!
//! The test is gated by the `testing` cargo feature because
//! `FakeSnapshotStore` is itself feature-gated. Run with:
//!
//! ```sh
//! cargo test -p sandbawks-types --all-features
//! ```

#![cfg(feature = "testing")]

use std::collections::HashMap;

use hegel::generators;
use hegel::TestCase;

use sandbawks_types::snapshot::{aggregate_sha256, SnapshotPart, SnapshotSink, SnapshotSource};
use sandbawks_types::testing::FakeSnapshotStore;
use sandbawks_types::SnapshotManifest;

/// Cap on the length of generated part-name strings. Production part
/// names live in the 10-30 char range (`memory.zst`, `rootfs-delta.tar.zst`);
/// 32 is a comfortable upper bound that keeps each test case bounded.
const MAX_NAME_LEN: usize = 32;

/// Cap on the length of generated part payloads in bytes.
///
/// Real CRIU memory images are MB-sized, but the property we're testing
/// (round-trip + sha256 stability) is structural — 256 bytes per part
/// is plenty to exercise the hash function and HashMap storage. Keeping
/// this small keeps the 256-default-case run cheap.
const MAX_BYTES_LEN: usize = 256;

/// Cap on the number of parts per generated manifest. The largest
/// production manifest is 3 parts (`memory.zst`, `rootfs-delta.tar.zst`,
/// `envd-state.json`); 6 doubles that with headroom.
const MAX_PARTS: usize = 6;

/// Generate a `Vec<u8>` of arbitrary bytes up to [`MAX_BYTES_LEN`].
#[hegel::composite]
fn payload(tc: TestCase) -> Vec<u8> {
    tc.draw(generators::vecs(generators::integers::<u8>()).max_size(MAX_BYTES_LEN))
}

/// Generate a part name. We allow the full Unicode `text()` range
/// because part names are opaque strings to the trait (the production
/// naming convention is enforced by the `S3SnapshotSink` impl, not the
/// trait itself).
#[hegel::composite]
fn part_name(tc: TestCase) -> String {
    tc.draw(generators::text().max_size(MAX_NAME_LEN))
}

/// Generate a list of `(name, bytes)` pairs, deduplicated by name (a
/// `HashMap` collect keeps the last-written value per key — matching
/// the `FakeSnapshotStore`'s last-write-wins semantics for `put_part`).
#[hegel::composite]
fn name_byte_pairs(tc: TestCase) -> Vec<(String, Vec<u8>)> {
    let pairs: Vec<(String, Vec<u8>)> =
        tc.draw(generators::vecs(generators::tuples!(part_name(), payload())).max_size(MAX_PARTS));
    let dedup: HashMap<String, Vec<u8>> = pairs.into_iter().collect();
    dedup.into_iter().collect()
}

#[hegel::test]
fn parts_round_trip_byte_identical(tc: TestCase) {
    // tokio runtime is set up per test case so we can drive the async
    // trait methods without sprinkling `#[tokio::test]` on every
    // hegel-generated invocation.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    rt.block_on(async {
        let pairs = tc.draw(name_byte_pairs());
        let store = FakeSnapshotStore::new();

        let mut expected_parts: Vec<SnapshotPart> = Vec::with_capacity(pairs.len());
        for (name, bytes) in &pairs {
            let part = store
                .put_part(name, bytes.clone())
                .await
                .expect("FakeSnapshotStore::put_part should succeed");

            // Per-part sha256 must match a fresh hash over the original
            // payload. This is the invariant the controller relies on
            // when deciding whether to short-circuit a re-upload.
            assert_eq!(part, SnapshotPart::from_bytes(name, bytes));
            assert_eq!(part.size_bytes as usize, bytes.len());
            expected_parts.push(part);
        }

        // Byte-identity check: every part comes back unchanged.
        for (name, bytes) in &pairs {
            let fetched = store
                .get_part(name)
                .await
                .expect("FakeSnapshotStore::get_part should succeed");
            assert_eq!(&fetched, bytes, "byte identity for part {name}");
        }

        // Aggregate sha256 over the recorded parts must match what
        // `aggregate_sha256` would produce de novo — i.e. the
        // SnapshotManifest can rely on the same helper.
        let agg = aggregate_sha256(&expected_parts);
        let manifest = SnapshotManifest {
            envd_sha256: String::new(),
            runsc_version: String::new(),
            envd_captured: false,
            sandbox_id: "sbx-prop".into(),
            runtime_class: "fake".into(),
            sha256: agg.clone(),
            created_at: "1970-01-01T00:00:00Z".into(),
            parts: expected_parts,
            snapshot_ref: "s3://test/manifest.json".into(),
            template_ref: String::new(),
            template_image: String::new(),
        };

        // Manifest round-trip through the store preserves equality
        // (and therefore preserves the aggregate sha256).
        store
            .put_manifest(&manifest)
            .await
            .expect("put_manifest should succeed");
        let fetched = store
            .get_manifest()
            .await
            .expect("get_manifest should succeed");
        assert_eq!(manifest, fetched);
    });
}
