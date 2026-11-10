//! Unit test for `GvisorAdapter::pause` (Plan 05 T7 / issue #143).
//!
//! The test exercises the full pause workflow against:
//!
//! - [`FakeSnapshotStore`] as the [`SnapshotSink`] (no S3 round-trip).
//! - [`fake::FakeContainerd`] returning canned `PodResolution` +
//!   rootfs-delta bytes (no real containerd socket).
//! - [`fake::FakeEnvdState`] returning canned envd state JSON (no
//!   reqwest HTTP call).
//! - [`fake::FixedClock`] pinning the work-dir timestamp.
//! - A real `tokio::process::Command` driving a shell-script stub of
//!   `runsc` written to a `tempfile::tempdir`, with `GvisorAdapter`'s
//!   `runsc_binary` pointed at that script's absolute path. The script
//!   writes the same `*.img` files that `runsc checkpoint --image-path`
//!   would produce for gVisor 20240603+ (`checkpoint.img`, `pages.img`),
//!   so the subsequent tar+zstd of the work-dir picks them up via the
//!   real `*.img` glob the adapter uses (see issue #193).
//!
//! After running `pause`, the test asserts:
//!
//! 1. All three parts (`memory.zst`, `rootfs-delta.tar.zst`,
//!    `envd-state.json`) land in the `FakeSnapshotStore`.
//! 2. The per-part sha256s recorded in the returned [`SnapshotManifest`]
//!    match a fresh hash over the stored bytes.
//! 3. The aggregate sha256 on the manifest matches
//!    `aggregate_sha256(&manifest.parts)`.
//! 4. The `manifest.created_at` equals the `FixedClock`'s `now`.
//! 5. The `envd-state.json` part bytes equal the canned envd JSON.
//! 6. The `rootfs-delta.tar.zst` part bytes equal the canned rootfs
//!    delta bytes.
//! 7. The `memory.zst` part decompresses + un-tars to the expected
//!    set of checkpoint `.img` file names + bytes.
//! 8. The fakes' call recorders see exactly one invocation each.

use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use sandbawks_node::gvisor::{
    fake::{FakeContainerd, FakeEnvdState, FixedClock},
    GvisorAdapter, PodResolution, SpecMount,
};
use sandbawks_types::runtime::{RuntimeAdapter, RuntimeError, SnapshotManifest};
use sandbawks_types::snapshot::{
    aggregate_sha256, sha256_hex, SnapshotPart, SnapshotSink, SnapshotSource,
};
use sandbawks_types::testing::FakeSnapshotStore;
use sandbawks_types::STAGED_MARKER_FILENAME;

/// Canned envd `/__state` body. Bytes are opaque to the adapter, so we
/// pick a recognizable JSON string and assert byte-identity round-trip.
const ENVD_STATE_BYTES: &[u8] =
    b"{\"revision\":\"test-rev-1\",\"processes\":{\"bash\":{\"pid\":1}}}";

/// Canned rootfs-delta bytes. Opaque at the trait layer; we just want
/// to confirm they survive the trip through `SnapshotSink::put_part`.
const ROOTFS_DELTA_BYTES: &[u8] = b"this-pretends-to-be-tar-zstd-bytes";

/// Fixed timestamp pinned by [`FixedClock`]. Mirrors the RFC 3339 shape
/// that `chrono::Utc::now().to_rfc3339()` would emit; the `:` characters
/// confirm the work-dir path sanitizer is exercised.
const FIXED_NOW: &str = "2026-05-25T12:34:56+00:00";

/// Checkpoint files the stubbed `runsc` writes into the image-path.
/// Names mirror what real `runsc checkpoint --image-path` produces for
/// gVisor 20240603+ (see issue #193): `checkpoint.img` (sentry state),
/// `pages.img` (MemoryFile pages), and `pages_meta.img` (MemoryFile
/// metadata index). All three are written because #232's fail-closed
/// checkpoint postcondition rejects a pause that leaves any of them
/// missing or empty (see [`CHECKPOINT_FILE_3`]).
const CHECKPOINT_FILE_1: (&str, &[u8]) = ("checkpoint.img", b"sentry-state-bytes");
const CHECKPOINT_FILE_2: (&str, &[u8]) = ("pages.img", b"memoryfile-pages-bytes");

/// Build a shell script that emulates `runsc checkpoint --image-path <dir> <cid>`.
///
/// `runsc checkpoint` takes the third positional argument as
/// `--image-path` and the fourth as the container ID. The stub
/// inspects `$@`, extracts the `--image-path` value (the literal arg
/// after `--image-path`), writes the three checkpoint `.img` files real
/// gVisor would write (`checkpoint.img` + `pages.img` + `pages_meta.img`)
/// into it, and exits 0. All three are required so the pause clears
/// #232's checkpoint postcondition.
///
/// Returns the path to the script, kept alive by the `tempfile::TempDir`
/// the test owns.
fn write_runsc_stub(tmp: &std::path::Path) -> std::path::PathBuf {
    write_runsc_stub_with_marker(tmp, None)
}

/// Render a bash line that truncate-creates `marker` (a no-op `: >` redirect),
/// or the empty string when `marker` is `None`. Shared by the checkpoint and
/// restore stub builders to plant an ordering marker. tempdir-derived paths
/// never contain single quotes on supported platforms, so a single-quoted
/// shell literal is a safe escape; we assert that precondition.
fn marker_touch_line(marker: Option<&std::path::Path>) -> String {
    match marker {
        Some(path) => {
            let s = path.to_str().expect("marker path must be utf8");
            assert!(
                !s.contains('\''),
                "marker path must not contain a single quote"
            );
            format!(": > '{s}'\n")
        }
        None => String::new(),
    }
}

/// Build a `runsc checkpoint` stub that writes the three checkpoint files
/// into `--image-path`, and — when `checkpoint_marker` is `Some` — `touch`es
/// that marker on every invocation. The marker lets a test prove temporal
/// ordering: the fake's `quiesce` asserts the marker is still absent when it
/// fires, so a regression moving quiesce after `runsc checkpoint` fails the
/// test (#403). `write_runsc_stub` is the no-marker shorthand.
fn write_runsc_stub_with_marker(
    tmp: &std::path::Path,
    checkpoint_marker: Option<&std::path::Path>,
) -> std::path::PathBuf {
    let script_path = tmp.join("runsc-stub.sh");
    let marker_line = marker_touch_line(checkpoint_marker);
    // Quoting: the heredoc embeds literal `$@`/`$1`/`$2` etc — bash
    // sees the script verbatim from disk so we want shell variable
    // expansion at script-run time, not Rust-format-string time.
    let script_body = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
{marker_line}# `runsc --version` is shelled by the pause path to stamp the snapshot's
# runsc version (#545). Answer it with a realistic two-line release banner
# before the checkpoint arg-walk so the capture path is exercised end to end.
if [ "${{1:-}}" = "--version" ]; then
    echo "runsc version release-20260525.0"
    echo "spec: 1.1.0-rc.1"
    exit 0
fi
# Expected invocation:
#   runsc checkpoint --image-path <work_dir> <container_id>
# Walk argv looking for --image-path; capture the next arg as the dir.
image_path=""
i=1
while [ $i -le $# ]; do
    arg="${{!i}}"
    if [ "$arg" = "--image-path" ]; then
        next=$((i+1))
        image_path="${{!next}}"
        break
    fi
    i=$((i+1))
done
if [ -z "$image_path" ]; then
    echo "stub: missing --image-path" >&2
    exit 64
fi
mkdir -p "$image_path"
printf %s '{f1_bytes}' > "$image_path/{f1_name}"
printf %s '{f2_bytes}' > "$image_path/{f2_name}"
printf %s '{f3_bytes}' > "$image_path/{f3_name}"
exit 0
"#,
        f1_name = CHECKPOINT_FILE_1.0,
        f1_bytes = std::str::from_utf8(CHECKPOINT_FILE_1.1).expect("ascii bytes"),
        f2_name = CHECKPOINT_FILE_2.0,
        f2_bytes = std::str::from_utf8(CHECKPOINT_FILE_2.1).expect("ascii bytes"),
        f3_name = CHECKPOINT_FILE_3.0,
        f3_bytes = std::str::from_utf8(CHECKPOINT_FILE_3.1).expect("ascii bytes"),
    );
    std::fs::write(&script_path, script_body).expect("write runsc stub");
    let mut perms = std::fs::metadata(&script_path)
        .expect("stat runsc stub")
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script_path, perms).expect("chmod runsc stub");
    script_path
}

/// Build a stub that answers ONLY `runsc --version` with the given release
/// token and exits non-zero on any other invocation. Models the **host** runsc
/// binary (#545): the pause path shells it only for `--version`, never for
/// `checkpoint` (that goes to the bundled `runsc_binary`).
fn write_runsc_version_stub(
    tmp: &std::path::Path,
    file_name: &str,
    release: &str,
) -> std::path::PathBuf {
    assert!(
        !release.contains('\''),
        "release token must not contain a single quote"
    );
    let script_path = tmp.join(file_name);
    let script_body = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
if [ "${{1:-}}" = "--version" ]; then
    echo 'runsc version {release}'
    echo 'spec: 1.1.0-rc.1'
    exit 0
fi
echo "version-only stub: unexpected args: $*" >&2
exit 64
"#,
    );
    std::fs::write(&script_path, script_body).expect("write runsc version stub");
    let mut perms = std::fs::metadata(&script_path)
        .expect("stat runsc version stub")
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script_path, perms).expect("chmod runsc version stub");
    script_path
}

/// Create a minimal runsc root directory that satisfies `root_holds_container`.
///
/// `resolve_runsc_root` probes each candidate root for the container's state —
/// either a `<root>/<cid>.state` file or a legacy `<root>/<cid>/` subdirectory.
/// Tests that exercise `pause` / `resume` past the checkpoint/restore call must
/// seed this structure so the resolution succeeds; otherwise the adapter returns
/// `RunscRootUnresolved` before even invoking the runsc stub.
///
/// The legacy dir layout (`<root>/<cid>/`) is the cheapest to create in a
/// temp-dir test environment and is always detected by `root_holds_container`,
/// so we use it here regardless of the format the real shim produces.
fn seed_runsc_root(tmp: &std::path::Path, container_id: &str) -> std::path::PathBuf {
    let root = tmp.join("runsc-root");
    std::fs::create_dir_all(root.join(container_id))
        .expect("create runsc root + container state dir");
    root
}

#[tokio::test]
async fn pause_emits_three_parts_and_manifest() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let runsc_stub = write_runsc_stub(tmp.path());

    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("create work_root");
    let runsc_root = seed_runsc_root(tmp.path(), "abc123def");

    let resolution = PodResolution {
        container_id: "abc123def".to_owned(),
        pod_ip: "10.42.0.7".to_owned(),
        sandbox_id: "sbx-test-7".to_owned(),
    };
    let containerd = Arc::new(FakeContainerd::new(
        resolution.clone(),
        ROOTFS_DELTA_BYTES.to_vec(),
    ));
    let envd = Arc::new(FakeEnvdState::new(ENVD_STATE_BYTES.to_vec()));
    let clock = Arc::new(FixedClock::new(FIXED_NOW));

    let adapter = GvisorAdapter::new(containerd.clone(), envd.clone(), &work_root)
        .with_runsc_binary(runsc_stub)
        .with_runsc_root(runsc_root)
        .with_clock(clock)
        .with_post_probe_drain(std::time::Duration::ZERO);

    let store = FakeSnapshotStore::new();

    let manifest = adapter
        .pause("pod-uid-aaa-bbb-ccc", &store)
        .await
        .expect("pause should succeed against fakes");

    // 1. Three parts in declared order: memory, rootfs, envd-state.
    assert_eq!(manifest.parts.len(), 3, "expected three parts");
    let part_names: Vec<&str> = manifest.parts.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(
        part_names,
        vec!["memory.zst", "rootfs-delta.tar.zst", "envd-state.json"],
    );

    // 2. Per-part sha256 matches the fresh hash over the stored bytes.
    for part in &manifest.parts {
        let stored = store
            .get_part(&part.name)
            .await
            .unwrap_or_else(|_| panic!("FakeSnapshotStore missing part {}", part.name));
        let fresh = SnapshotPart::from_bytes(&part.name, &stored);
        assert_eq!(
            part.sha256, fresh.sha256,
            "sha256 mismatch for part {}",
            part.name,
        );
        assert_eq!(
            part.size_bytes as usize,
            stored.len(),
            "size mismatch for part {}",
            part.name,
        );
    }

    // 3. Aggregate sha256 on the manifest matches a fresh aggregate.
    assert_eq!(manifest.sha256, aggregate_sha256(&manifest.parts));

    // 4. created_at == FixedClock now.
    assert_eq!(manifest.created_at, FIXED_NOW);
    assert_eq!(manifest.sandbox_id, "sbx-test-7");
    assert_eq!(manifest.runtime_class, "gvisor");

    // 4b. The host runsc release the checkpoint was taken against is captured
    //     from `runsc --version` (#545, Capability B) — the controller folds
    //     this into status and gates the next restore Pod's nodeAffinity on it.
    assert_eq!(
        manifest.runsc_version, "release-20260525.0",
        "pause must stamp the snapshot with the host runsc release"
    );

    // 5. envd-state bytes round-trip verbatim through the sink.
    let stored_envd = store.get_part("envd-state.json").await.expect("envd");
    assert_eq!(stored_envd, ENVD_STATE_BYTES);

    // 6. rootfs-delta bytes round-trip verbatim.
    let stored_rootfs = store
        .get_part("rootfs-delta.tar.zst")
        .await
        .expect("rootfs");
    assert_eq!(stored_rootfs, ROOTFS_DELTA_BYTES);

    // 7. memory.zst decompresses + un-tars to the three known checkpoint
    //    images the stub wrote (all three are required by #232's
    //    checkpoint postcondition).
    let stored_memory = store.get_part("memory.zst").await.expect("memory");
    let decoder = zstd::stream::read::Decoder::new(stored_memory.as_slice()).expect("zstd dec");
    let mut archive = tar::Archive::new(decoder);
    let mut got: Vec<(String, Vec<u8>)> = Vec::new();
    for entry in archive.entries().expect("tar entries") {
        let mut entry = entry.expect("tar entry");
        let name = entry.path().expect("path").to_string_lossy().into_owned();
        let mut bytes = Vec::new();
        std::io::copy(&mut entry, &mut bytes).expect("copy tar entry");
        got.push((name, bytes));
    }
    got.sort();
    assert_eq!(
        got,
        vec![
            (CHECKPOINT_FILE_1.0.to_owned(), CHECKPOINT_FILE_1.1.to_vec()),
            (CHECKPOINT_FILE_2.0.to_owned(), CHECKPOINT_FILE_2.1.to_vec()),
            (CHECKPOINT_FILE_3.0.to_owned(), CHECKPOINT_FILE_3.1.to_vec()),
        ],
    );

    // 8. Each fake was called exactly once and saw the right argument.
    // Clone-and-drop the guards immediately rather than holding them
    // across the subsequent `await` (clippy::await_holding_lock).
    let resolves = containerd
        .resolve_calls
        .lock()
        .expect("not poisoned")
        .clone();
    assert_eq!(resolves.as_slice(), &["pod-uid-aaa-bbb-ccc".to_owned()]);
    let deltas = containerd.delta_calls.lock().expect("not poisoned").clone();
    assert_eq!(deltas.as_slice(), &["abc123def".to_owned()]);
    let fetches = envd.fetch_calls.lock().expect("not poisoned").clone();
    assert_eq!(fetches.as_slice(), &["10.42.0.7".to_owned()]);

    // 9. The manifest itself is also persisted to the sink.
    let fetched_manifest = store.get_manifest().await.expect("get_manifest");
    assert_eq!(fetched_manifest, manifest);
}

/// #545 regression: the snapshot's `runsc_version` must be read from the
/// **host** runsc binary (the one that ran the sandbox and that Capability A
/// labels the node with), NOT the image-bundled `runsc_binary` used for
/// `runsc checkpoint`. On a #546 `BundledMismatch` node the two differ, and
/// recording the bundled version would emit a gate that matches no node's
/// host-derived label, stranding every restore `Pending`.
#[tokio::test]
async fn pause_stamps_host_runsc_version_not_bundled() {
    let tmp = tempfile::tempdir().expect("tempdir");
    // Bundled binary: drives `runsc checkpoint` and reports release-20260525.0
    // on `--version` (the `write_runsc_stub` baked-in version).
    let bundled_stub = write_runsc_stub(tmp.path());
    // Host binary: reports a DIFFERENT release, mimicking a drifted host.
    let host_stub =
        write_runsc_version_stub(tmp.path(), "runsc-host-stub.sh", "release-20260608.0");

    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("create work_root");
    let runsc_root = seed_runsc_root(tmp.path(), "abc123def");

    let resolution = PodResolution {
        container_id: "abc123def".to_owned(),
        pod_ip: "10.42.0.7".to_owned(),
        sandbox_id: "sbx-host-version".to_owned(),
    };
    let containerd = Arc::new(FakeContainerd::new(
        resolution.clone(),
        ROOTFS_DELTA_BYTES.to_vec(),
    ));
    let envd = Arc::new(FakeEnvdState::new(ENVD_STATE_BYTES.to_vec()));
    let clock = Arc::new(FixedClock::new(FIXED_NOW));

    let adapter = GvisorAdapter::new(containerd, envd, &work_root)
        .with_runsc_binary(bundled_stub)
        .with_host_runsc_binary(host_stub)
        .with_runsc_root(runsc_root)
        .with_clock(clock)
        .with_post_probe_drain(std::time::Duration::ZERO);

    let store = FakeSnapshotStore::new();
    let manifest = adapter
        .pause("pod-uid-host-ver", &store)
        .await
        .expect("pause should succeed against fakes");

    assert_eq!(
        manifest.runsc_version, "release-20260608.0",
        "pause must stamp the HOST runsc version, not the bundled checkpoint binary's"
    );
}

#[tokio::test]
async fn pause_surfaces_runsc_nonzero_exit_as_checkpoint_failed() {
    let tmp = tempfile::tempdir().expect("tempdir");
    // A stub that exits non-zero with a stderr message.
    let script_path = tmp.path().join("runsc-fail.sh");
    std::fs::write(
        &script_path,
        "#!/usr/bin/env bash\necho 'simulated runsc failure' 1>&2\nexit 2\n",
    )
    .expect("write fail stub");
    let mut perms = std::fs::metadata(&script_path).expect("stat").permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script_path, perms).expect("chmod");

    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("create work_root");
    let runsc_root = seed_runsc_root(tmp.path(), "cid");

    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "cid".into(),
            pod_ip: "10.0.0.1".into(),
            sandbox_id: "sbx".into(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));

    let adapter = GvisorAdapter::new(containerd, envd, &work_root)
        .with_runsc_binary(script_path)
        .with_runsc_root(runsc_root)
        .with_post_probe_drain(std::time::Duration::ZERO);

    let store = FakeSnapshotStore::new();
    let err = adapter
        .pause("any-uid", &store)
        .await
        .expect_err("pause should fail when runsc exits non-zero");

    match err {
        sandbawks_types::runtime::RuntimeError::CheckpointFailed(msg) => {
            assert!(
                msg.contains("simulated runsc failure"),
                "expected stderr to surface in error, got: {msg}",
            );
        }
        other => panic!("expected CheckpointFailed, got {other:?}"),
    }
    // Nothing should have been uploaded — the failure surfaced before
    // any `put_part` call.
    assert!(store.is_empty(), "no parts should be uploaded on failure");
}

#[tokio::test]
async fn pause_fails_closed_when_checkpoint_image_missing() {
    // #232: a `runsc checkpoint` that exits 0 but leaves an incomplete
    // image set (here: no `pages_meta.img`) must fail the pause *before*
    // any upload, so a non-restorable snapshot is never staged.
    let tmp = tempfile::tempdir().expect("tempdir");
    let script_path = tmp.path().join("runsc-partial.sh");
    // Exits 0 and writes only two of the three required images.
    let script_body = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
image_path=""
i=1
while [ $i -le $# ]; do
    arg="${{!i}}"
    if [ "$arg" = "--image-path" ]; then
        next=$((i+1)); image_path="${{!next}}"; break
    fi
    i=$((i+1))
done
mkdir -p "$image_path"
printf %s '{f1_bytes}' > "$image_path/{f1_name}"
printf %s '{f2_bytes}' > "$image_path/{f2_name}"
exit 0
"#,
        f1_name = CHECKPOINT_FILE_1.0,
        f1_bytes = std::str::from_utf8(CHECKPOINT_FILE_1.1).expect("ascii"),
        f2_name = CHECKPOINT_FILE_2.0,
        f2_bytes = std::str::from_utf8(CHECKPOINT_FILE_2.1).expect("ascii"),
    );
    std::fs::write(&script_path, script_body).expect("write partial stub");
    let mut perms = std::fs::metadata(&script_path).expect("stat").permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script_path, perms).expect("chmod");

    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("create work_root");
    let runsc_root = seed_runsc_root(tmp.path(), "cid");

    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "cid".into(),
            pod_ip: "10.0.0.1".into(),
            sandbox_id: "sbx".into(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    let adapter = GvisorAdapter::new(containerd, envd, &work_root)
        .with_runsc_binary(script_path)
        .with_runsc_root(runsc_root)
        .with_post_probe_drain(std::time::Duration::ZERO);

    let store = FakeSnapshotStore::new();
    let err = adapter
        .pause("any-uid", &store)
        .await
        .expect_err("pause must fail closed on an incomplete checkpoint");
    match err {
        sandbawks_types::runtime::RuntimeError::CheckpointFailed(msg) => {
            assert!(
                msg.contains("pages_meta.img") && msg.contains("postcondition"),
                "error should name the missing image and the postcondition, got: {msg}",
            );
        }
        other => panic!("expected CheckpointFailed, got {other:?}"),
    }
    assert!(
        store.is_empty(),
        "no parts may be uploaded when the checkpoint postcondition fails"
    );
}

#[tokio::test]
async fn pause_fails_closed_on_out_of_overlay_writable_mount() {
    // #237: a sandbox whose OCI spec mounts writable state outside the
    // overlay (here a `bind`) is unrestorable from a memory-only
    // checkpoint — the bind's writes never reach `pages.img`. The pause
    // must fail-closed *before* the checkpoint, leaving the store empty
    // and never even probing envd.
    let tmp = tempfile::tempdir().expect("tempdir");
    // A runsc stub that fails loudly: it must never run, because the
    // mount-set assertion short-circuits ahead of `runsc checkpoint`.
    let runsc_stub = tmp.path().join("runsc-must-not-run.sh");
    std::fs::write(
        &runsc_stub,
        "#!/usr/bin/env bash\necho 'runsc invoked despite mount-set failure' 1>&2\nexit 1\n",
    )
    .expect("write guard stub");
    let mut perms = std::fs::metadata(&runsc_stub).expect("stat").permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&runsc_stub, perms).expect("chmod");

    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("create work_root");

    // A realistic mix mirroring a kubelet/CRI gVisor Pod: pseudo-fs, the
    // `/dev` furniture tmpfs, the rw bind dotfile furniture, and a
    // read-only image bind — none of which may trip the gate — plus the
    // one genuine escape (`/host-state`, a writable host-backed bind).
    let containerd = Arc::new(
        FakeContainerd::new(
            PodResolution {
                container_id: "cid".into(),
                pod_ip: "10.0.0.1".into(),
                sandbox_id: "sbx".into(),
            },
            Vec::new(),
        )
        .with_spec_mounts(vec![
            SpecMount {
                destination: "/proc".into(),
                mount_type: "proc".into(),
                source: "proc".into(),
                options: vec![],
            },
            SpecMount {
                destination: "/dev".into(),
                mount_type: "tmpfs".into(),
                source: "tmpfs".into(),
                options: vec!["rw".into(), "nosuid".into()],
            },
            SpecMount {
                destination: "/dev/shm".into(),
                mount_type: "bind".into(),
                source: "/run/containerd/.../shm".into(),
                options: vec!["rw".into(), "rbind".into(), "rprivate".into()],
            },
            SpecMount {
                destination: "/etc/hosts".into(),
                mount_type: "bind".into(),
                source: "/var/lib/kubelet/.../etc-hosts".into(),
                options: vec!["rw".into(), "rbind".into()],
            },
            SpecMount {
                destination: "/dev/termination-log".into(),
                mount_type: "bind".into(),
                source: "/var/lib/kubelet/.../termination-log".into(),
                options: vec!["rw".into(), "rbind".into()],
            },
            SpecMount {
                destination: "/image-config".into(),
                mount_type: "bind".into(),
                source: "/var/lib/host/image-config".into(),
                options: vec!["ro".into(), "rbind".into()],
            },
            SpecMount {
                destination: "/host-state".into(),
                mount_type: "bind".into(),
                source: "/var/lib/host/state".into(),
                options: vec!["rw".into(), "rbind".into()],
            },
        ]),
    );
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    let adapter = GvisorAdapter::new(containerd.clone(), envd.clone(), &work_root)
        .with_runsc_binary(runsc_stub)
        .with_post_probe_drain(std::time::Duration::ZERO);

    let store = FakeSnapshotStore::new();
    let err = adapter
        .pause("any-uid", &store)
        .await
        .expect_err("pause must fail closed on an out-of-overlay writable mount");
    match err {
        sandbawks_types::runtime::RuntimeError::CheckpointFailed(msg) => {
            assert!(
                msg.contains("/host-state") && msg.contains("mount-set"),
                "error must name the offending mount and the assertion, got: {msg}",
            );
            assert!(
                !msg.contains("/image-config")
                    && !msg.contains("/proc")
                    && !msg.contains("/dev")
                    && !msg.contains("/etc/hosts"),
                "the furniture, pseudo-fs, tmpfs, and read-only bind must not be flagged, \
                 got: {msg}",
            );
        }
        other => panic!("expected CheckpointFailed, got {other:?}"),
    }
    assert!(
        store.is_empty(),
        "no parts may be uploaded when the mount-set assertion fails"
    );
    // The failure is fail-fast: it precedes the envd state probe.
    let fetches = envd.fetch_calls.lock().expect("not poisoned").clone();
    assert!(
        fetches.is_empty(),
        "envd must not be probed once the mount-set assertion has failed"
    );
}

// ---------------------------------------------------------------------------
// Resume tests (Plan 05 T8 / issue #144).
//
// Mirror the pause test scaffolding: pre-populate a `FakeSnapshotStore`
// with a manifest + the three parts pause would have produced, stub
// `runsc restore` with a shell script that exits 0, and bind a local
// `std::net::TcpListener` on the envd port so the resume-side wait
// completes deterministically. After running `resume`, assert:
//
// 1. The work-dir contains the expected files (memory.zst un-tar'd back
//    into the dir, envd-state.json written verbatim).
// 2. `apply_rootfs_delta` was called once with the right container ID +
//    bytes (byte-identity round-trip through the store).
// 3. `runsc restore` was invoked exactly once and exited 0.
// 4. The fakes saw the expected pod_uid / container_id arguments.
// ---------------------------------------------------------------------------

/// Build a shell script that emulates `runsc restore --image-path <dir> <cid>`.
///
/// The stub only needs to exit 0 — the resume side reads from the
/// work-dir before invoking `runsc`, so the script doesn't have to
/// inspect or modify anything. A separate variant exercises the
/// non-zero exit path, mirroring the pause test's failure branch.
fn write_runsc_restore_stub(tmp: &std::path::Path) -> std::path::PathBuf {
    write_runsc_restore_stub_with_marker(tmp, None)
}

/// Build a `runsc restore` stub that exits 0, and — when `restore_marker` is
/// `Some` — `touch`es that marker on every invocation. The marker lets a test
/// prove temporal ordering: the fake's `rearm` asserts the marker is present
/// when it fires, so a regression moving rearm before `runsc restore` (or the
/// bind wait) fails the test (#403). `write_runsc_restore_stub` is the
/// no-marker shorthand.
fn write_runsc_restore_stub_with_marker(
    tmp: &std::path::Path,
    restore_marker: Option<&std::path::Path>,
) -> std::path::PathBuf {
    let script_path = tmp.join("runsc-restore-stub.sh");
    let marker_line = marker_touch_line(restore_marker);
    let script_body = format!("#!/usr/bin/env bash\nset -euo pipefail\n{marker_line}exit 0\n");
    std::fs::write(&script_path, &script_body).expect("write runsc restore stub");
    let mut perms = std::fs::metadata(&script_path)
        .expect("stat runsc restore stub")
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script_path, perms).expect("chmod runsc restore stub");
    script_path
}

/// Build a recording stub: every invocation appends its argv to
/// `log_path` (one arg per line, preceded by `---` separators). Always
/// exits 0. Used to pin which `runsc` subcommands the resume path
/// actually invokes (issue #171 — verify `pause` precedes `restore`).
fn write_runsc_recording_stub(
    tmp: &std::path::Path,
    log_path: &std::path::Path,
) -> std::path::PathBuf {
    // `tempfile::tempdir` paths under `$TMPDIR` never contain single
    // quotes on supported platforms, so a single-quoted bash literal
    // is a safe shell escape for the tempdir-derived `log_path`.
    let log_str = log_path
        .to_str()
        .expect("recording stub log path must be utf8");
    assert!(
        !log_str.contains('\''),
        "recording stub does not support single-quote paths"
    );
    let script_path = tmp.join("runsc-recording-stub.sh");
    let script_body = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
{{
    echo '---'
    for a in "$@"; do
        echo "$a"
    done
}} >> '{log_str}'
exit 0
"#,
    );
    std::fs::write(&script_path, script_body).expect("write runsc recording stub");
    let mut perms = std::fs::metadata(&script_path)
        .expect("stat runsc recording stub")
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script_path, perms).expect("chmod runsc recording stub");
    script_path
}

/// Build a `memory.zst` payload that, when un-tar'd, produces the same
/// `checkpoint.img` / `pages.img` files the pause test writes.
///
/// Done by reusing `tar` + `zstd` directly so the test is independent
/// of the production `tar_zstd_memory_files_blocking` (which is a
/// private helper). Symmetry with what pause produces means the resume
/// path stays exercised end-to-end on a single set of fixtures.
fn build_memory_zst(files: &[(&str, &[u8])]) -> Vec<u8> {
    let buf: Vec<u8> = Vec::new();
    let zstd_writer = zstd::stream::write::Encoder::new(buf, 0).expect("zstd encoder");
    let mut tar_builder = tar::Builder::new(zstd_writer);
    for (name, bytes) in files {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar_builder
            .append_data(&mut header, name, *bytes)
            .expect("tar append_data");
    }
    let zstd_writer = tar_builder.into_inner().expect("tar finish");
    zstd_writer.finish().expect("zstd finish")
}

#[tokio::test]
async fn resume_unpacks_parts_applies_delta_and_waits_for_envd() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let runsc_stub = write_runsc_restore_stub(tmp.path());

    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("create work_root");
    let runsc_root = seed_runsc_root(tmp.path(), "abc123def");

    // Pre-populate the fake store with the three parts a successful
    // pause would have produced, plus the manifest that pins them.
    let store = FakeSnapshotStore::new();
    let memory_zst = build_memory_zst(&[CHECKPOINT_FILE_1, CHECKPOINT_FILE_2]);
    let memory_part = store
        .put_part("memory.zst", memory_zst.clone())
        .await
        .expect("put memory.zst");
    let rootfs_part = store
        .put_part("rootfs-delta.tar.zst", ROOTFS_DELTA_BYTES.to_vec())
        .await
        .expect("put rootfs-delta");
    let envd_part = store
        .put_part("envd-state.json", ENVD_STATE_BYTES.to_vec())
        .await
        .expect("put envd-state");
    let mut manifest = SnapshotManifest {
        envd_sha256: String::new(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: "sbx-test-8".to_owned(),
        runtime_class: "gvisor".to_owned(),
        sha256: String::new(),
        created_at: FIXED_NOW.to_owned(),
        parts: vec![memory_part, rootfs_part, envd_part],
        snapshot_ref: "s3://test-bucket/sbx-test-8/manifest.json".to_owned(),
        template_ref: String::new(),
        template_image: String::new(),
    };
    manifest.recompute_aggregate();
    store
        .put_manifest(&manifest)
        .await
        .expect("put_manifest seed");

    // Bind a local TCP listener on an ephemeral port; the resume-side
    // wait probes `127.0.0.1:<port>`, so `connect` succeeds on the
    // first poll iteration. Using port 0 + querying the bound address
    // avoids fighting other test runs for a fixed port.
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind envd-port listener");
    let envd_port = listener.local_addr().expect("local_addr").port();

    let resolution = PodResolution {
        container_id: "abc123def".to_owned(),
        pod_ip: "127.0.0.1".to_owned(),
        sandbox_id: "sbx-test-8".to_owned(),
    };
    let containerd = Arc::new(FakeContainerd::new(
        resolution.clone(),
        // rootfs_delta_bytes is unused on the resume path (we never
        // call `rootfs_delta`); an empty Vec keeps the fake happy.
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    let clock = Arc::new(FixedClock::new(FIXED_NOW));

    let adapter = GvisorAdapter::new(containerd.clone(), envd.clone(), &work_root)
        .with_runsc_binary(runsc_stub)
        .with_runsc_root(runsc_root)
        .with_clock(clock)
        .with_envd_port(envd_port);

    adapter
        .resume(&manifest, "pod-uid-aaa-bbb-ccc", &store)
        .await
        .expect("resume should succeed against fakes");

    // 1. apply_rootfs_delta was called exactly once with the right
    //    container_id + the verbatim bytes the sink stored.
    let applies = containerd.apply_calls.lock().expect("not poisoned").clone();
    assert_eq!(applies.len(), 1, "exactly one apply_rootfs_delta call");
    assert_eq!(applies[0].0, "abc123def");
    assert_eq!(applies[0].1, ROOTFS_DELTA_BYTES);

    // 2. The work-dir contains envd-state.json + the two checkpoint files.
    //    Locate the resume work-dir by listing the per-sandbox parent:
    //    `<work_root>/sbx-test-8/resume-<sanitized-ts>/`.
    let per_sandbox = work_root.join("sbx-test-8");
    let mut entries: Vec<std::path::PathBuf> = std::fs::read_dir(&per_sandbox)
        .expect("read per-sandbox dir")
        .map(|e| e.expect("dir entry").path())
        .collect();
    assert_eq!(
        entries.len(),
        1,
        "expected exactly one resume work-dir under {}",
        per_sandbox.display()
    );
    let work_dir = entries.pop().expect("work_dir");
    let dir_name = work_dir
        .file_name()
        .and_then(|s| s.to_str())
        .expect("work_dir name utf8");
    assert!(
        dir_name.starts_with("resume-"),
        "expected resume-prefixed work-dir, got {dir_name}"
    );

    let envd_path = work_dir.join("envd-state.json");
    let envd_bytes = std::fs::read(&envd_path).expect("read envd-state.json");
    assert_eq!(envd_bytes, ENVD_STATE_BYTES);

    let mem1_bytes =
        std::fs::read(work_dir.join(CHECKPOINT_FILE_1.0)).expect("read checkpoint.img");
    assert_eq!(mem1_bytes, CHECKPOINT_FILE_1.1);
    let mem2_bytes = std::fs::read(work_dir.join(CHECKPOINT_FILE_2.0)).expect("read pages.img");
    assert_eq!(mem2_bytes, CHECKPOINT_FILE_2.1);

    // 3. resolve_pod was called once with the supplied pod_uid.
    let resolves = containerd
        .resolve_calls
        .lock()
        .expect("not poisoned")
        .clone();
    assert_eq!(resolves.as_slice(), &["pod-uid-aaa-bbb-ccc".to_owned()]);

    // 4. The manifest in storage still matches what we seeded (resume
    //    is read-only on the store).
    let fetched = store.get_manifest().await.expect("get_manifest");
    assert_eq!(fetched, manifest);

    // 5. aggregate_sha256 over the parts the test seeded is what the
    //    cross-check inside resume compared against — a regression in
    //    the aggregate would have surfaced as a `SnapshotDownload`
    //    error here.
    assert_eq!(manifest.sha256, aggregate_sha256(&manifest.parts));

    // Keep the listener alive until after assertions so the connect
    // probe inside resume hit a bound socket.
    drop(listener);
}

/// Issue #171 hardening: when the controller *does* commit a sha256
/// via `SnapshotJobSpec.expected_sha256` and the node reconciler
/// threads it into the placeholder, a mismatching storage manifest
/// must fail the resume before any restore work — defends against a
/// tampered or stale manifest in S3.
#[tokio::test]
async fn resume_rejects_storage_sha256_mismatch_when_committed() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let runsc_stub = write_runsc_restore_stub(tmp.path());

    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("create work_root");

    let store = FakeSnapshotStore::new();
    let memory_zst = build_memory_zst(&[CHECKPOINT_FILE_1]);
    let memory_part = store
        .put_part("memory.zst", memory_zst)
        .await
        .expect("put memory");
    let rootfs_part = store
        .put_part("rootfs-delta.tar.zst", ROOTFS_DELTA_BYTES.to_vec())
        .await
        .expect("put rootfs");
    let envd_part = store
        .put_part("envd-state.json", ENVD_STATE_BYTES.to_vec())
        .await
        .expect("put envd-state");
    let mut storage_manifest = SnapshotManifest {
        envd_sha256: String::new(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: "sbx-mismatch".into(),
        runtime_class: "gvisor".into(),
        sha256: String::new(),
        created_at: FIXED_NOW.into(),
        parts: vec![memory_part, rootfs_part, envd_part],
        snapshot_ref: "s3://b/k".into(),
        template_ref: String::new(),
        template_image: String::new(),
    };
    storage_manifest.recompute_aggregate();
    store
        .put_manifest(&storage_manifest)
        .await
        .expect("seed manifest");

    // Controller has committed to a *different* sha256 — simulates
    // either a stale snapshot_sha256 in Sandbox.status or an attacker-
    // tampered manifest in storage (where the storage aggregate no
    // longer matches what pause originally produced).
    let committed = SnapshotManifest {
        envd_sha256: String::new(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: storage_manifest.sandbox_id.clone(),
        runtime_class: "gvisor".into(),
        sha256: "deadbeef".repeat(8),
        created_at: String::new(),
        parts: Vec::new(),
        snapshot_ref: storage_manifest.snapshot_ref.clone(),
        template_ref: String::new(),
        template_image: String::new(),
    };
    assert_ne!(
        committed.sha256, storage_manifest.sha256,
        "test setup: committed sha must differ from storage to exercise the check"
    );

    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "cid".into(),
            pod_ip: "127.0.0.1".into(),
            sandbox_id: storage_manifest.sandbox_id.clone(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    let adapter = GvisorAdapter::new(containerd.clone(), envd, &work_root)
        .with_runsc_binary(runsc_stub)
        .with_envd_port(
            std::net::TcpListener::bind(("127.0.0.1", 0))
                .expect("bind")
                .local_addr()
                .expect("addr")
                .port(),
        );

    let err = adapter
        .resume(&committed, "fresh-pod-uid", &store)
        .await
        .expect_err("resume must reject a storage manifest whose sha256 disagrees with the controller's commitment");
    match err {
        sandbawks_types::runtime::RuntimeError::SnapshotDownload(msg) => {
            assert!(
                msg.contains("manifest sha256 mismatch"),
                "expected mismatch error, got: {msg}",
            );
            assert!(
                msg.contains(&storage_manifest.sha256),
                "error should include the storage sha for triage, got: {msg}",
            );
        }
        other => panic!("expected SnapshotDownload, got {other:?}"),
    }

    // No restore work ran — `apply_rootfs_delta` must not have been
    // called.
    let applies = containerd.apply_calls.lock().expect("not poisoned").clone();
    assert!(
        applies.is_empty(),
        "resume must bail before applying rootfs delta when sha256 disagrees"
    );
}

/// Issue #278 hardening: when the controller commits an envd digest via
/// `SnapshotJobSpec.expected_envd_sha256` (threaded into the placeholder
/// manifest's `envd_sha256`), a storage manifest whose `envd_sha256`
/// disagrees must fail the resume *before* any restore work — defends
/// against a manifest tampered in storage to flip both `sha256` and
/// `envd_sha256` to an attacker-controlled binary, which would otherwise
/// redefine "the expected envd" on the restore node and pass the
/// local-binary `verify_envd_digest` check. The aggregate `sha256` is
/// kept matching here so the envd cross-check is what fires.
#[tokio::test]
async fn resume_rejects_storage_envd_sha256_mismatch_when_committed() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let runsc_stub = write_runsc_restore_stub(tmp.path());

    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("create work_root");

    let store = FakeSnapshotStore::new();
    let memory_zst = build_memory_zst(&[CHECKPOINT_FILE_1]);
    let memory_part = store
        .put_part("memory.zst", memory_zst)
        .await
        .expect("put memory");
    let rootfs_part = store
        .put_part("rootfs-delta.tar.zst", ROOTFS_DELTA_BYTES.to_vec())
        .await
        .expect("put rootfs");
    let envd_part = store
        .put_part("envd-state.json", ENVD_STATE_BYTES.to_vec())
        .await
        .expect("put envd-state");
    // Storage manifest carries the attacker-substituted envd digest.
    let storage_envd_sha = "a".repeat(64);
    let mut storage_manifest = SnapshotManifest {
        envd_sha256: storage_envd_sha.clone(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: "sbx-envd-mismatch".into(),
        runtime_class: "gvisor".into(),
        sha256: String::new(),
        created_at: FIXED_NOW.into(),
        parts: vec![memory_part, rootfs_part, envd_part],
        snapshot_ref: "s3://b/k".into(),
        template_ref: String::new(),
        template_image: String::new(),
    };
    storage_manifest.recompute_aggregate();
    store
        .put_manifest(&storage_manifest)
        .await
        .expect("seed manifest");

    // Controller committed the *original* (pause-time) envd digest, and
    // the matching aggregate sha256 — so the #171 aggregate cross-check
    // passes and the #278 envd cross-check is the gate that fires.
    let committed_envd_sha = "b".repeat(64);
    assert_ne!(
        committed_envd_sha, storage_envd_sha,
        "test setup: committed envd sha must differ from storage to exercise the check"
    );
    let committed = SnapshotManifest {
        envd_sha256: committed_envd_sha.clone(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: storage_manifest.sandbox_id.clone(),
        runtime_class: "gvisor".into(),
        sha256: storage_manifest.sha256.clone(),
        created_at: String::new(),
        parts: Vec::new(),
        snapshot_ref: storage_manifest.snapshot_ref.clone(),
        template_ref: String::new(),
        template_image: String::new(),
    };

    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "cid".into(),
            pod_ip: "127.0.0.1".into(),
            sandbox_id: storage_manifest.sandbox_id.clone(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    let adapter = GvisorAdapter::new(containerd.clone(), envd, &work_root)
        .with_runsc_binary(runsc_stub)
        .with_envd_port(
            std::net::TcpListener::bind(("127.0.0.1", 0))
                .expect("bind")
                .local_addr()
                .expect("addr")
                .port(),
        );

    let err = adapter
        .resume(&committed, "fresh-pod-uid", &store)
        .await
        .expect_err("resume must reject a storage manifest whose envd_sha256 disagrees with the controller's commitment");
    match err {
        sandbawks_types::runtime::RuntimeError::SnapshotDownload(msg) => {
            assert!(
                msg.contains("manifest envd_sha256 mismatch"),
                "expected envd mismatch error, got: {msg}",
            );
            assert!(
                msg.contains(&storage_envd_sha),
                "error should include the storage envd sha for triage, got: {msg}",
            );
        }
        other => panic!("expected SnapshotDownload, got {other:?}"),
    }

    // No restore work ran — the envd cross-check fails before any bytes
    // are materialized, so `apply_rootfs_delta` must not have been called.
    let applies = containerd.apply_calls.lock().expect("not poisoned").clone();
    assert!(
        applies.is_empty(),
        "resume must bail before applying rootfs delta when envd_sha256 disagrees"
    );
}

/// Issue #171 fallback: when the controller doesn't commit a sha256
/// (legacy callers, e.g. direct adapter use in unit tests), the
/// placeholder manifest's `sha256` is empty and the adapter falls
/// through to per-part verification only — otherwise every legacy
/// resume fails against the storage's real aggregate.
#[tokio::test]
async fn resume_tolerates_empty_controller_sha256() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let runsc_stub = write_runsc_restore_stub(tmp.path());

    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("create work_root");
    let runsc_root = seed_runsc_root(tmp.path(), "cid-empty-sha");

    // Seed a manifest with a *real* aggregate sha256, mirroring what
    // production GvisorAdapter::pause writes to S3.
    let store = FakeSnapshotStore::new();
    let memory_zst = build_memory_zst(&[CHECKPOINT_FILE_1]);
    let memory_part = store
        .put_part("memory.zst", memory_zst)
        .await
        .expect("put memory");
    let rootfs_part = store
        .put_part("rootfs-delta.tar.zst", ROOTFS_DELTA_BYTES.to_vec())
        .await
        .expect("put rootfs");
    let envd_part = store
        .put_part("envd-state.json", ENVD_STATE_BYTES.to_vec())
        .await
        .expect("put envd-state");
    let mut storage_manifest = SnapshotManifest {
        envd_sha256: String::new(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: "sbx-empty-controller-sha".into(),
        runtime_class: "gvisor".into(),
        sha256: String::new(),
        created_at: FIXED_NOW.into(),
        parts: vec![memory_part, rootfs_part, envd_part],
        snapshot_ref: "s3://b/k".into(),
        template_ref: String::new(),
        template_image: String::new(),
    };
    storage_manifest.recompute_aggregate();
    assert!(
        !storage_manifest.sha256.is_empty(),
        "storage manifest must have a real aggregate sha256 to exercise the bug"
    );
    store
        .put_manifest(&storage_manifest)
        .await
        .expect("seed manifest");

    // Controller-side placeholder mirrors what
    // `sandbawks_node::reconcile` actually dispatches today: empty
    // sha256, empty parts, only snapshot_ref + sandbox_id filled in.
    let placeholder = SnapshotManifest {
        envd_sha256: String::new(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: storage_manifest.sandbox_id.clone(),
        runtime_class: "gvisor".into(),
        sha256: String::new(),
        created_at: String::new(),
        parts: Vec::new(),
        snapshot_ref: storage_manifest.snapshot_ref.clone(),
        template_ref: String::new(),
        template_image: String::new(),
    };

    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind envd-port");
    let envd_port = listener.local_addr().expect("addr").port();

    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "cid-empty-sha".into(),
            pod_ip: "127.0.0.1".into(),
            sandbox_id: storage_manifest.sandbox_id.clone(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    let adapter = GvisorAdapter::new(containerd, envd, &work_root)
        .with_runsc_binary(runsc_stub)
        .with_runsc_root(runsc_root)
        .with_envd_port(envd_port);

    adapter
        .resume(&placeholder, "fresh-pod-uid", &store)
        .await
        .expect("resume must succeed when controller sha256 is empty");

    drop(listener);
}

/// Issue #171: the resume path invokes `runsc restore --image-path
/// <dir> <cid>` exactly once with the fresh container UID. An earlier
/// iteration of this branch attempted a defensive `runsc pause` first,
/// but gVisor rejected restore against a `paused` container — the
/// `pause` step is gone and this test pins the no-pause shape so a
/// regression that re-adds it surfaces here. The recording stub
/// captures every argv so the test can assert there's only one runsc
/// call and that it's the restore.
#[tokio::test]
async fn resume_invokes_runsc_restore_only_once() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let log_path = tmp.path().join("runsc-calls.log");
    let runsc_stub = write_runsc_recording_stub(tmp.path(), &log_path);

    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("create work_root");
    let runsc_root = seed_runsc_root(tmp.path(), "fresh-cid-001");

    // Seed a minimal but valid manifest + parts so the resume path
    // reaches the runsc invocations.
    let store = FakeSnapshotStore::new();
    let memory_zst = build_memory_zst(&[CHECKPOINT_FILE_1]);
    let memory_part = store
        .put_part("memory.zst", memory_zst)
        .await
        .expect("put memory");
    let rootfs_part = store
        .put_part("rootfs-delta.tar.zst", ROOTFS_DELTA_BYTES.to_vec())
        .await
        .expect("put rootfs");
    let envd_part = store
        .put_part("envd-state.json", ENVD_STATE_BYTES.to_vec())
        .await
        .expect("put envd-state");
    let mut manifest = SnapshotManifest {
        envd_sha256: String::new(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: "sbx-issue-171".into(),
        runtime_class: "gvisor".into(),
        sha256: String::new(),
        created_at: FIXED_NOW.into(),
        parts: vec![memory_part, rootfs_part, envd_part],
        snapshot_ref: "s3://b/k".into(),
        template_ref: String::new(),
        template_image: String::new(),
    };
    manifest.recompute_aggregate();
    store.put_manifest(&manifest).await.expect("seed manifest");

    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind envd-port");
    let envd_port = listener.local_addr().expect("addr").port();

    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "fresh-cid-001".into(),
            pod_ip: "127.0.0.1".into(),
            sandbox_id: "sbx-issue-171".into(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    let adapter = GvisorAdapter::new(containerd, envd, &work_root)
        .with_runsc_binary(runsc_stub)
        .with_runsc_root(runsc_root)
        .with_envd_port(envd_port);

    adapter
        .resume(&manifest, "fresh-pod-uid", &store)
        .await
        .expect("resume succeeds against the recording stub");

    let calls = std::fs::read_to_string(&log_path).expect("read recording stub log");
    let invocations: Vec<Vec<String>> = calls
        .split("---\n")
        .filter(|chunk| !chunk.trim().is_empty())
        .map(|chunk| {
            chunk
                .lines()
                .filter(|l| !l.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .collect();

    // Exactly one runsc invocation: restore. No defensive `pause`
    // (gVisor rejects restore against a paused container), no
    // pre-restore kill (regressing to that would break kubelet's view
    // of the Pod).
    assert_eq!(
        invocations.len(),
        1,
        "expected exactly one runsc invocation (restore), got {invocations:?}",
    );

    let restore_args = &invocations[0];
    assert_eq!(
        restore_args.first().map(String::as_str),
        Some("--root"),
        "restore invocation must start with --root, got {restore_args:?}",
    );
    assert!(
        restore_args.iter().any(|a| a == "restore"),
        "invocation must include 'restore', got {restore_args:?}",
    );
    assert!(
        restore_args.iter().any(|a| a == "--image-path"),
        "restore invocation must include --image-path, got {restore_args:?}",
    );
    assert!(
        !restore_args.iter().any(|a| a == "pause"),
        "restore path must not invoke `runsc pause` — gVisor rejects restore against a paused container, got {restore_args:?}",
    );
    assert_eq!(
        restore_args.last().map(String::as_str),
        Some("fresh-cid-001"),
        "restore subcommand must target the fresh container UID, got {restore_args:?}",
    );

    drop(listener);
}

#[tokio::test]
async fn resume_surfaces_runsc_nonzero_exit_as_restore_failed() {
    let tmp = tempfile::tempdir().expect("tempdir");
    // Stub that exits non-zero with a stderr message.
    let script_path = tmp.path().join("runsc-restore-fail.sh");
    std::fs::write(
        &script_path,
        "#!/usr/bin/env bash\necho 'simulated runsc restore failure' 1>&2\nexit 3\n",
    )
    .expect("write fail stub");
    let mut perms = std::fs::metadata(&script_path).expect("stat").permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script_path, perms).expect("chmod");

    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("create work_root");
    let runsc_root = seed_runsc_root(tmp.path(), "cid");

    // Seed a minimal manifest + parts the restore path can fetch
    // before tripping over the failing runsc.
    let store = FakeSnapshotStore::new();
    let memory_zst = build_memory_zst(&[CHECKPOINT_FILE_1]);
    let memory_part = store.put_part("memory.zst", memory_zst).await.expect("p1");
    let rootfs_part = store
        .put_part("rootfs-delta.tar.zst", ROOTFS_DELTA_BYTES.to_vec())
        .await
        .expect("p2");
    let envd_part = store
        .put_part("envd-state.json", ENVD_STATE_BYTES.to_vec())
        .await
        .expect("p3");
    let mut manifest = SnapshotManifest {
        envd_sha256: String::new(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: "sbx".into(),
        runtime_class: "gvisor".into(),
        sha256: String::new(),
        created_at: FIXED_NOW.into(),
        parts: vec![memory_part, rootfs_part, envd_part],
        snapshot_ref: "s3://b/k".into(),
        template_ref: String::new(),
        template_image: String::new(),
    };
    manifest.recompute_aggregate();
    store.put_manifest(&manifest).await.expect("seed manifest");

    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "cid".into(),
            pod_ip: "127.0.0.1".into(),
            sandbox_id: "sbx".into(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));

    let adapter = GvisorAdapter::new(containerd, envd, &work_root)
        .with_runsc_binary(script_path)
        .with_runsc_root(runsc_root)
        // Bind a listener so the wait wouldn't be the source of any
        // failure if we got that far (we shouldn't; runsc fails first).
        .with_envd_port(
            std::net::TcpListener::bind(("127.0.0.1", 0))
                .expect("bind")
                .local_addr()
                .expect("addr")
                .port(),
        );

    let err = adapter
        .resume(&manifest, "any-uid", &store)
        .await
        .expect_err("resume should fail when runsc exits non-zero");

    match err {
        sandbawks_types::runtime::RuntimeError::RestoreFailed(msg) => {
            assert!(
                msg.contains("simulated runsc restore failure"),
                "expected stderr to surface in error, got: {msg}",
            );
        }
        other => panic!("expected RestoreFailed, got {other:?}"),
    }
}

#[tokio::test]
async fn resume_bails_on_part_sha256_mismatch() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let runsc_stub = write_runsc_restore_stub(tmp.path());

    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("create work_root");

    // Seed a manifest with a part whose sha256 doesn't match the
    // bytes actually stored under that name. The verifier inside
    // `resume` must catch this before invoking the snapshotter or
    // `runsc restore`.
    let store = FakeSnapshotStore::new();
    let memory_zst = build_memory_zst(&[CHECKPOINT_FILE_1]);
    // Store the real bytes ...
    let _good_part = store
        .put_part("memory.zst", memory_zst.clone())
        .await
        .expect("store memory");
    let _rootfs_part = store
        .put_part("rootfs-delta.tar.zst", ROOTFS_DELTA_BYTES.to_vec())
        .await
        .expect("store rootfs");
    let _envd_part = store
        .put_part("envd-state.json", ENVD_STATE_BYTES.to_vec())
        .await
        .expect("store envd");
    // ... but record a tampered SnapshotPart claiming a different sha
    // for `memory.zst`. The aggregate is computed over the tampered
    // parts, so the controller-side check inside `resume` passes and
    // we get all the way to the per-part verification.
    let tampered_memory = SnapshotPart {
        name: "memory.zst".into(),
        sha256: "0".repeat(64),
        size_bytes: memory_zst.len() as u64,
        uncompressed_bytes: None,
    };
    let real_rootfs = SnapshotPart::from_bytes("rootfs-delta.tar.zst", ROOTFS_DELTA_BYTES);
    let real_envd = SnapshotPart::from_bytes("envd-state.json", ENVD_STATE_BYTES);
    let mut manifest = SnapshotManifest {
        envd_sha256: String::new(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: "sbx".into(),
        runtime_class: "gvisor".into(),
        sha256: String::new(),
        created_at: FIXED_NOW.into(),
        parts: vec![tampered_memory, real_rootfs, real_envd],
        snapshot_ref: "s3://b/k".into(),
        template_ref: String::new(),
        template_image: String::new(),
    };
    manifest.recompute_aggregate();
    store.put_manifest(&manifest).await.expect("seed manifest");

    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "cid".into(),
            pod_ip: "127.0.0.1".into(),
            sandbox_id: "sbx".into(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    let adapter =
        GvisorAdapter::new(containerd.clone(), envd, &work_root).with_runsc_binary(runsc_stub);

    let err = adapter
        .resume(&manifest, "any-uid", &store)
        .await
        .expect_err("resume should fail on sha256 mismatch");
    match err {
        sandbawks_types::runtime::RuntimeError::SnapshotDownload(msg) => {
            assert!(
                msg.contains("sha256 mismatch for part memory.zst"),
                "expected sha256 mismatch message, got: {msg}",
            );
        }
        other => panic!("expected SnapshotDownload, got {other:?}"),
    }
    // The snapshotter Apply API must not have been called — the
    // verifier short-circuited before reaching the rootfs part.
    let applies = containerd.apply_calls.lock().expect("not poisoned").clone();
    assert!(
        applies.is_empty(),
        "apply_rootfs_delta must not run on sha mismatch",
    );
}

// ---------------------------------------------------------------------------
// Stage tests (Plan 07 T8 / issue #212).
//
// `stage_snapshot` materializes the bundle onto local disk *without*
// restoring a container: it shares `resume`'s unpack helper, so the
// `*.img` files (issue #195) land in the staging dir alongside
// `envd-state.json`, but the `rootfs-delta.tar.zst` part is staged as a
// file (no live container to apply it to). The returned path is the
// `--image-path` the shim hands to the memory `runsc restore` (#232).
// ---------------------------------------------------------------------------

/// Third checkpoint `.img` file. Real `runsc checkpoint --image-path`
/// for gVisor 20240603+ writes `pages_meta.img` (MemoryFile metadata)
/// alongside `checkpoint.img` / `pages.img`; staging must unpack all
/// three (issue #195), and the pause stub writes all three so the pause
/// clears #232's checkpoint postcondition ([`write_runsc_stub`]).
const CHECKPOINT_FILE_3: (&str, &[u8]) = ("pages_meta.img", b"memoryfile-meta-bytes");

#[tokio::test]
async fn stage_snapshot_materializes_bundle_and_returns_path() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let staging_root = tmp.path().join("staging");
    // `work_root` is unused by stage_snapshot, but the constructor needs one.
    let work_root = tmp.path().join("snapshots");

    // Seed the three parts a successful pause would have produced, plus
    // the manifest that pins their sha256s.
    let store = FakeSnapshotStore::new();
    let memory_zst = build_memory_zst(&[CHECKPOINT_FILE_1, CHECKPOINT_FILE_2, CHECKPOINT_FILE_3]);
    let memory_part = store
        .put_part("memory.zst", memory_zst)
        .await
        .expect("put memory.zst");
    let rootfs_part = store
        .put_part("rootfs-delta.tar.zst", ROOTFS_DELTA_BYTES.to_vec())
        .await
        .expect("put rootfs-delta");
    let envd_part = store
        .put_part("envd-state.json", ENVD_STATE_BYTES.to_vec())
        .await
        .expect("put envd-state");
    let mut manifest = SnapshotManifest {
        envd_sha256: String::new(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: "sbx-stage-8".to_owned(),
        runtime_class: "gvisor".to_owned(),
        sha256: String::new(),
        created_at: FIXED_NOW.to_owned(),
        parts: vec![memory_part, rootfs_part, envd_part],
        snapshot_ref: "s3://test-bucket/sbx-stage-8/manifest.json".to_owned(),
        template_ref: String::new(),
        template_image: String::new(),
    };
    manifest.recompute_aggregate();
    store.put_manifest(&manifest).await.expect("seed manifest");

    // No pod resolution / runsc / envd probe on the stage path — the
    // fakes only need to exist to satisfy the constructor.
    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "unused-on-stage".to_owned(),
            pod_ip: "0.0.0.0".to_owned(),
            sandbox_id: "sbx-stage-8".to_owned(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    let clock = Arc::new(FixedClock::new(FIXED_NOW));
    let adapter = GvisorAdapter::new(containerd.clone(), envd, &work_root)
        .with_clock(clock)
        .with_staging_root(&staging_root);

    let path = adapter
        .stage_snapshot("sbx-stage-8", &manifest, &store, None)
        .await
        .expect("stage_snapshot should succeed against fakes");

    // 1. The returned path is `<staging_root>/sbx-stage-8/<sanitized ts>`
    //    and exists as a directory.
    let staged = std::path::PathBuf::from(&path);
    assert!(staged.is_dir(), "returned staging path must exist: {path}");
    assert!(
        staged.starts_with(&staging_root),
        "staging path must live under staging_root, got {path}"
    );
    assert_eq!(
        staged
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|s| s.to_str()),
        Some("sbx-stage-8"),
        "staging path's parent segment must be the sandbox id, got {path}"
    );

    // 2. All three checkpoint `.img` files unpacked verbatim (issue #195).
    for (name, bytes) in [CHECKPOINT_FILE_1, CHECKPOINT_FILE_2, CHECKPOINT_FILE_3] {
        let got = std::fs::read(staged.join(name)).unwrap_or_else(|_| panic!("missing {name}"));
        assert_eq!(got, bytes, "unexpected bytes for {name}");
    }

    // 3. envd-state.json written verbatim.
    let envd_bytes = std::fs::read(staged.join("envd-state.json")).expect("read envd-state.json");
    assert_eq!(envd_bytes, ENVD_STATE_BYTES);

    // 4. rootfs-delta staged as a file (NOT applied to a container —
    //    there is none at stage time).
    let rootfs_bytes =
        std::fs::read(staged.join("rootfs-delta.tar.zst")).expect("read rootfs-delta");
    assert_eq!(rootfs_bytes, ROOTFS_DELTA_BYTES);
    let applies = containerd.apply_calls.lock().expect("not poisoned").clone();
    assert!(
        applies.is_empty(),
        "stage must not apply the rootfs delta to a containerd snapshot"
    );

    // 5. The stage path never resolves a pod (no container exists yet).
    let resolves = containerd
        .resolve_calls
        .lock()
        .expect("not poisoned")
        .clone();
    assert!(
        resolves.is_empty(),
        "stage_snapshot must not resolve a pod, got {resolves:?}"
    );
}

// ---------------------------------------------------------------------------
// Scheduler-placed restore (epic #484 / issue #487): a `Stage` carrying a
// `target_path` materializes the bundle at that exact deterministic path
// (clearing any prior attempt) and drops the `.staged` completion marker
// last, so the credential-less stage-gate init container can poll for it.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stage_snapshot_honors_target_path_and_drops_staged_marker() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let staging_root = tmp.path().join("staging");
    let work_root = tmp.path().join("snapshots");

    // The controller-pinned restore path is the deterministic
    // `<staging_root>/<sandbox-id>/restore` the controller derives — a fixed
    // segment, not a clock-stamped one. Staging under the root also satisfies
    // the containment guard (see `stage_snapshot_rejects_target_outside_root`).
    let target_dir = staging_root.join("sbx-pin").join("restore");
    // Seed a stale file from a hypothetical earlier attempt: the adapter
    // must clear the target dir before laying out, so this must not survive.
    std::fs::create_dir_all(&target_dir).expect("seed target dir");
    let stale = target_dir.join("stale-from-prior-attempt.img");
    std::fs::write(&stale, b"stale").expect("seed stale file");

    let store = FakeSnapshotStore::new();
    let memory_zst = build_memory_zst(&[CHECKPOINT_FILE_1, CHECKPOINT_FILE_2, CHECKPOINT_FILE_3]);
    let memory_part = store
        .put_part("memory.zst", memory_zst)
        .await
        .expect("put memory.zst");
    let rootfs_part = store
        .put_part("rootfs-delta.tar.zst", ROOTFS_DELTA_BYTES.to_vec())
        .await
        .expect("put rootfs-delta");
    let envd_part = store
        .put_part("envd-state.json", ENVD_STATE_BYTES.to_vec())
        .await
        .expect("put envd-state");
    let mut manifest = SnapshotManifest {
        envd_sha256: String::new(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: "sbx-pin".to_owned(),
        runtime_class: "gvisor".to_owned(),
        sha256: String::new(),
        created_at: FIXED_NOW.to_owned(),
        parts: vec![memory_part, rootfs_part, envd_part],
        snapshot_ref: "s3://test-bucket/sbx-pin/manifest.json".to_owned(),
        template_ref: String::new(),
        template_image: String::new(),
    };
    manifest.recompute_aggregate();
    store.put_manifest(&manifest).await.expect("seed manifest");

    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "unused-on-stage".to_owned(),
            pod_ip: "0.0.0.0".to_owned(),
            sandbox_id: "sbx-pin".to_owned(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    let clock = Arc::new(FixedClock::new(FIXED_NOW));
    let adapter = GvisorAdapter::new(containerd, envd, &work_root)
        .with_clock(clock)
        .with_staging_root(&staging_root);

    let target_str = target_dir.to_str().expect("utf-8 target path");
    let path = adapter
        .stage_snapshot("sbx-pin", &manifest, &store, Some(target_str))
        .await
        .expect("stage_snapshot should succeed against fakes");

    // 1. The returned path is the pinned target verbatim — not a
    //    clock-segmented dir under `staging_root`.
    assert_eq!(path, target_str, "stage must return the pinned target path");

    // 2. Every part landed at the pinned path.
    for (name, bytes) in [CHECKPOINT_FILE_1, CHECKPOINT_FILE_2, CHECKPOINT_FILE_3] {
        let got = std::fs::read(target_dir.join(name)).unwrap_or_else(|_| panic!("missing {name}"));
        assert_eq!(got, bytes, "unexpected bytes for {name}");
    }
    assert_eq!(
        std::fs::read(target_dir.join("envd-state.json")).expect("read envd-state.json"),
        ENVD_STATE_BYTES
    );
    assert_eq!(
        std::fs::read(target_dir.join("rootfs-delta.tar.zst")).expect("read rootfs-delta"),
        ROOTFS_DELTA_BYTES
    );

    // 3. The stale file from the prior attempt was cleared.
    assert!(
        !stale.exists(),
        "target dir must be cleared before layout; stale file survived"
    );

    // 4. The `.staged` completion marker is present and non-empty (its
    //    body is the staged dir path), and no `.staged.tmp` leaked.
    let marker = target_dir.join(STAGED_MARKER_FILENAME);
    let marker_body = std::fs::read(&marker).expect("`.staged` marker must exist");
    assert!(
        !marker_body.is_empty(),
        "`.staged` marker should carry a breadcrumb body"
    );
    assert!(
        !target_dir
            .join(format!("{STAGED_MARKER_FILENAME}.tmp"))
            .exists(),
        "the atomic-rename temp file must not survive"
    );
}

/// Defense-in-depth (issue #487 review): a `target_path` that resolves
/// *outside* `staging_root` is refused before anything is touched, so a
/// malformed or hostile `SnapshotJob.spec.target_path` can't drive the node
/// into `remove_dir_all`-ing an unrelated node-mounted tree.
#[tokio::test]
async fn stage_snapshot_rejects_target_outside_root() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let staging_root = tmp.path().join("staging");
    let work_root = tmp.path().join("snapshots");
    // The root must exist for the containment canonicalization.
    std::fs::create_dir_all(&staging_root).expect("create staging root");

    // A tree that has nothing to do with the staging cache, with a sentinel
    // file that must survive — the guard fires before any deletion.
    let outside = tmp.path().join("unrelated").join("restore");
    std::fs::create_dir_all(&outside).expect("create unrelated dir");
    let sentinel = outside.join("precious.dat");
    std::fs::write(&sentinel, b"do-not-delete").expect("seed sentinel");

    // The manifest is irrelevant: the guard rejects before any fetch.
    let manifest = SnapshotManifest {
        envd_sha256: String::new(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: "sbx-evil".to_owned(),
        runtime_class: "gvisor".to_owned(),
        sha256: String::new(),
        created_at: FIXED_NOW.to_owned(),
        parts: Vec::new(),
        snapshot_ref: "s3://test-bucket/sbx-evil/manifest.json".to_owned(),
        template_ref: String::new(),
        template_image: String::new(),
    };
    let store = FakeSnapshotStore::new();
    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "unused-on-stage".to_owned(),
            pod_ip: "0.0.0.0".to_owned(),
            sandbox_id: "sbx-evil".to_owned(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    let clock = Arc::new(FixedClock::new(FIXED_NOW));
    let adapter = GvisorAdapter::new(containerd, envd, &work_root)
        .with_clock(clock)
        .with_staging_root(&staging_root);

    let outside_str = outside.to_str().expect("utf-8 target path");
    let err = adapter
        .stage_snapshot("sbx-evil", &manifest, &store, Some(outside_str))
        .await
        .expect_err("a target outside staging_root must be refused");
    match err {
        RuntimeError::SnapshotDownload(msg) => assert!(
            msg.contains("escapes staging root"),
            "error should name the containment failure: got {msg}"
        ),
        other => panic!("expected SnapshotDownload, got {other:?}"),
    }

    // The unrelated tree is untouched — the guard fired before `remove_dir_all`.
    assert!(
        sentinel.exists(),
        "guard must reject before deleting anything outside staging_root"
    );
}

/// The legacy (no `target_path`) Stage path must NOT drop a `.staged`
/// marker — the marker is exclusively the scheduler-placed restore's
/// gate signal, and a stray one under a timestamped dir would be noise.
#[tokio::test]
async fn stage_snapshot_legacy_path_writes_no_staged_marker() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let staging_root = tmp.path().join("staging");
    let work_root = tmp.path().join("snapshots");

    let store = FakeSnapshotStore::new();
    let memory_zst = build_memory_zst(&[CHECKPOINT_FILE_1, CHECKPOINT_FILE_2, CHECKPOINT_FILE_3]);
    let memory_part = store
        .put_part("memory.zst", memory_zst)
        .await
        .expect("put memory.zst");
    let rootfs_part = store
        .put_part("rootfs-delta.tar.zst", ROOTFS_DELTA_BYTES.to_vec())
        .await
        .expect("put rootfs-delta");
    let envd_part = store
        .put_part("envd-state.json", ENVD_STATE_BYTES.to_vec())
        .await
        .expect("put envd-state");
    let mut manifest = SnapshotManifest {
        envd_sha256: String::new(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: "sbx-legacy".to_owned(),
        runtime_class: "gvisor".to_owned(),
        sha256: String::new(),
        created_at: FIXED_NOW.to_owned(),
        parts: vec![memory_part, rootfs_part, envd_part],
        snapshot_ref: "s3://test-bucket/sbx-legacy/manifest.json".to_owned(),
        template_ref: String::new(),
        template_image: String::new(),
    };
    manifest.recompute_aggregate();
    store.put_manifest(&manifest).await.expect("seed manifest");

    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "unused-on-stage".to_owned(),
            pod_ip: "0.0.0.0".to_owned(),
            sandbox_id: "sbx-legacy".to_owned(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    let clock = Arc::new(FixedClock::new(FIXED_NOW));
    let adapter = GvisorAdapter::new(containerd, envd, &work_root)
        .with_clock(clock)
        .with_staging_root(&staging_root);

    let path = adapter
        .stage_snapshot("sbx-legacy", &manifest, &store, None)
        .await
        .expect("stage_snapshot should succeed against fakes");

    let staged = std::path::PathBuf::from(&path);
    assert!(
        !staged.join(STAGED_MARKER_FILENAME).exists(),
        "legacy Stage path must not drop a `.staged` marker"
    );
}

#[tokio::test]
async fn stage_snapshot_bails_on_part_sha256_mismatch() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let staging_root = tmp.path().join("staging");
    let work_root = tmp.path().join("snapshots");

    // Store real bytes but record a tampered sha for `memory.zst`, so the
    // shared verifier inside stage_snapshot must reject before laying out
    // any files. (The aggregate is computed over the tampered parts so the
    // controller-side cross-check passes and we reach per-part verify.)
    let store = FakeSnapshotStore::new();
    let memory_zst = build_memory_zst(&[CHECKPOINT_FILE_1]);
    let _m = store
        .put_part("memory.zst", memory_zst.clone())
        .await
        .expect("store memory");
    let _e = store
        .put_part("envd-state.json", ENVD_STATE_BYTES.to_vec())
        .await
        .expect("store envd");
    let tampered_memory = SnapshotPart {
        name: "memory.zst".into(),
        sha256: "0".repeat(64),
        size_bytes: memory_zst.len() as u64,
        uncompressed_bytes: None,
    };
    let real_envd = SnapshotPart::from_bytes("envd-state.json", ENVD_STATE_BYTES);
    let mut manifest = SnapshotManifest {
        envd_sha256: String::new(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: "sbx-stage-bad".into(),
        runtime_class: "gvisor".into(),
        sha256: String::new(),
        created_at: FIXED_NOW.into(),
        parts: vec![tampered_memory, real_envd],
        snapshot_ref: "s3://b/k".into(),
        template_ref: String::new(),
        template_image: String::new(),
    };
    manifest.recompute_aggregate();
    store.put_manifest(&manifest).await.expect("seed manifest");

    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "cid".into(),
            pod_ip: "0.0.0.0".into(),
            sandbox_id: "sbx-stage-bad".into(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    let adapter = GvisorAdapter::new(containerd, envd, &work_root)
        .with_clock(Arc::new(FixedClock::new(FIXED_NOW)))
        .with_staging_root(&staging_root);

    let err = adapter
        .stage_snapshot("sbx-stage-bad", &manifest, &store, None)
        .await
        .expect_err("stage must reject a part whose sha256 disagrees with the manifest");
    match err {
        sandbawks_types::runtime::RuntimeError::SnapshotDownload(msg) => {
            assert!(
                msg.contains("sha256 mismatch for part memory.zst"),
                "expected per-part sha mismatch message, got: {msg}"
            );
        }
        other => panic!("expected SnapshotDownload, got {other:?}"),
    }

    // The partial staging dir is cleaned up on the error path so retries
    // (each in a fresh clock-segment subdir) don't accumulate orphans.
    // The per-sandbox parent may remain; the leaf subdir the failed
    // stage allocated must be gone.
    let per_sandbox = staging_root.join("sbx-stage-bad");
    let leftover = std::fs::read_dir(&per_sandbox)
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(
        leftover,
        0,
        "partial staging dir under {} must be removed on the error path",
        per_sandbox.display()
    );
}

// ---------------------------------------------------------------------------
// validate_restorable tests (issue #235 — fail-closed teardown).
//
// `validate_restorable` is the node-side restorability gate the Pause
// worker runs between checkpoint+upload and reporting `Complete`. It is a
// *storage round-trip proof*: re-fetch the uploaded snapshot from the
// source into a throwaway scratch dir, verify every part's sha256, and
// assert the round-tripped `*.img` set is complete (the same checkpoint
// postconditions a live pause enforces). No bundle, no second sandbox boot
// (an earlier live `runsc restore` was dropped — CI proved it racy against
// containerd's post-checkpoint bundle reap), so these tests need no runsc
// stub at all.
// ---------------------------------------------------------------------------

/// Seed a `FakeSnapshotStore` with the manifest + three parts a successful
/// pause would have uploaded, returning the manifest the controller would
/// commit. `imgs` selects which checkpoint images go into `memory.zst` so a
/// test can seed an intentionally-incomplete image set.
async fn seed_pause_bundle_with(
    store: &FakeSnapshotStore,
    sandbox_id: &str,
    imgs: &[(&str, &[u8])],
) -> SnapshotManifest {
    let memory_zst = build_memory_zst(imgs);
    let memory_part = store
        .put_part("memory.zst", memory_zst)
        .await
        .expect("memory");
    let rootfs_part = store
        .put_part("rootfs-delta.tar.zst", ROOTFS_DELTA_BYTES.to_vec())
        .await
        .expect("rootfs");
    let envd_part = store
        .put_part("envd-state.json", ENVD_STATE_BYTES.to_vec())
        .await
        .expect("envd");
    let mut manifest = SnapshotManifest {
        envd_sha256: String::new(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: sandbox_id.to_owned(),
        runtime_class: "gvisor".to_owned(),
        sha256: String::new(),
        created_at: FIXED_NOW.to_owned(),
        parts: vec![memory_part, rootfs_part, envd_part],
        snapshot_ref: format!("s3://test-bucket/{sandbox_id}/manifest.json"),
        template_ref: String::new(),
        template_image: String::new(),
    };
    manifest.recompute_aggregate();
    store.put_manifest(&manifest).await.expect("seed manifest");
    manifest
}

/// Assert that no `validate-*` scratch dir leaked under the work-dir for
/// `sandbox_id` — cleanup must run on every exit path.
fn assert_no_validation_scratch(work_root: &std::path::Path, sandbox_id: &str) {
    let per_sandbox = work_root.join(sandbox_id);
    let leaked: Vec<String> = std::fs::read_dir(&per_sandbox)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.starts_with("validate-"))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        leaked.is_empty(),
        "validation scratch dirs must be cleaned up, found: {leaked:?}"
    );
}

/// Build a `GvisorAdapter` for the validate tests. The fakes only need to
/// exist to satisfy the constructor — `validate_restorable` resolves no pod
/// and invokes no runsc.
fn validate_adapter(work_root: &std::path::Path) -> GvisorAdapter {
    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "unused".to_owned(),
            pod_ip: "0.0.0.0".to_owned(),
            sandbox_id: "unused".to_owned(),
        },
        Vec::new(),
    ));
    // These tests exercise the strongest (re-download + decompress +
    // postconditions) behaviour, which is no longer the default (now `head`),
    // so pin it explicitly.
    GvisorAdapter::new(
        containerd,
        Arc::new(FakeEnvdState::new(Vec::new())),
        work_root,
    )
    .with_clock(Arc::new(FixedClock::new(FIXED_NOW)))
    .with_validate_level("full")
}

/// Build a validate adapter pinned to a specific level (e.g. `"head"`).
fn validate_adapter_at(work_root: &std::path::Path, level: &str) -> GvisorAdapter {
    validate_adapter(work_root).with_validate_level(level)
}

#[tokio::test]
async fn validate_restorable_succeeds_for_intact_uploaded_bundle() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("work_root");

    let store = FakeSnapshotStore::new();
    let manifest = seed_pause_bundle_with(
        &store,
        "sbx-val-ok",
        &[CHECKPOINT_FILE_1, CHECKPOINT_FILE_2, CHECKPOINT_FILE_3],
    )
    .await;

    validate_adapter(&work_root)
        .validate_restorable(&manifest, &store)
        .await
        .expect("an intact uploaded bundle must validate");

    // The scratch dir was torn down.
    assert_no_validation_scratch(&work_root, "sbx-val-ok");
}

#[tokio::test]
async fn validate_restorable_head_level_accepts_intact_bundle() {
    // The default `head` level confirms presence without re-materializing the
    // bundle, so it accepts an intact upload and creates no scratch dir.
    let tmp = tempfile::tempdir().expect("tempdir");
    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("work_root");

    let store = FakeSnapshotStore::new();
    let manifest = seed_pause_bundle_with(
        &store,
        "sbx-val-head",
        &[CHECKPOINT_FILE_1, CHECKPOINT_FILE_2, CHECKPOINT_FILE_3],
    )
    .await;

    validate_adapter_at(&work_root, "head")
        .validate_restorable(&manifest, &store)
        .await
        .expect("head+size must accept an intact bundle");

    assert_no_validation_scratch(&work_root, "sbx-val-head");
}

#[tokio::test]
async fn validate_restorable_rejects_corrupt_uploaded_bundle() {
    // The uploaded snapshot is corrupt: a part's bytes don't match its
    // manifest sha256. validate_restorable must reject it during re-fetch,
    // so the source stays recoverable.
    let tmp = tempfile::tempdir().expect("tempdir");
    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("work_root");

    // Seed real bytes but record a tampered sha for memory.zst (the
    // aggregate is recomputed over the tampered part, so the controller-side
    // cross-check passes and we reach per-part verification).
    let store = FakeSnapshotStore::new();
    let memory_zst = build_memory_zst(&[CHECKPOINT_FILE_1, CHECKPOINT_FILE_2, CHECKPOINT_FILE_3]);
    let _m = store
        .put_part("memory.zst", memory_zst.clone())
        .await
        .expect("memory");
    let _e = store
        .put_part("envd-state.json", ENVD_STATE_BYTES.to_vec())
        .await
        .expect("envd");
    let tampered_memory = SnapshotPart {
        name: "memory.zst".into(),
        sha256: "0".repeat(64),
        size_bytes: memory_zst.len() as u64,
        uncompressed_bytes: None,
    };
    let real_envd = SnapshotPart::from_bytes("envd-state.json", ENVD_STATE_BYTES);
    let mut manifest = SnapshotManifest {
        envd_sha256: String::new(),
        runsc_version: String::new(),
        envd_captured: false,
        sandbox_id: "sbx-val-corrupt".into(),
        runtime_class: "gvisor".into(),
        sha256: String::new(),
        created_at: FIXED_NOW.into(),
        parts: vec![tampered_memory, real_envd],
        snapshot_ref: "s3://b/k".into(),
        template_ref: String::new(),
        template_image: String::new(),
    };
    manifest.recompute_aggregate();
    store.put_manifest(&manifest).await.expect("seed manifest");

    let err = validate_adapter(&work_root)
        .validate_restorable(&manifest, &store)
        .await
        .expect_err("a corrupt uploaded bundle must fail validation");
    match err {
        sandbawks_types::runtime::RuntimeError::SnapshotDownload(msg) => assert!(
            msg.contains("sha256 mismatch for part memory.zst"),
            "expected a per-part sha mismatch, got: {msg}"
        ),
        other => panic!("expected SnapshotDownload, got {other:?}"),
    }
    assert_no_validation_scratch(&work_root, "sbx-val-corrupt");
}

#[tokio::test]
async fn validate_restorable_rejects_incomplete_image_set() {
    // Every part verifies, but the memory image is structurally incomplete
    // (missing `pages_meta.img`). The round-tripped checkpoint postconditions
    // must reject it — catching a snapshot that would fail an actual restore
    // with an opaque EOF at resume time.
    let tmp = tempfile::tempdir().expect("tempdir");
    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("work_root");

    let store = FakeSnapshotStore::new();
    // Only two of the three required images.
    let manifest = seed_pause_bundle_with(
        &store,
        "sbx-val-incomplete",
        &[CHECKPOINT_FILE_1, CHECKPOINT_FILE_2],
    )
    .await;

    let err = validate_adapter(&work_root)
        .validate_restorable(&manifest, &store)
        .await
        .expect_err("an incomplete image set must fail validation");
    match err {
        sandbawks_types::runtime::RuntimeError::CheckpointFailed(msg) => assert!(
            msg.contains("pages_meta.img") && msg.contains("postcondition"),
            "error should name the missing image + the postcondition, got: {msg}"
        ),
        other => panic!("expected CheckpointFailed, got {other:?}"),
    }
    assert_no_validation_scratch(&work_root, "sbx-val-incomplete");
}

// ---------------------------------------------------------------------------
// envd-binary determinism tests (issue #270).
//
// Under node-staging (issue #265) the workload container execs the
// cluster-wide envd binary mounted read-only from the node, so its code
// pages are mmap-backed by that file. `pause` records the binary's sha256
// in `SnapshotManifest::envd_sha256`; the restore-side `stage_snapshot`
// fails closed if the staging node's binary has drifted, since a memory
// `runsc restore` would otherwise reconnect the mmap to a different file
// and corrupt the restored process.
// ---------------------------------------------------------------------------

/// Bytes standing in for the node-staged envd binary. Opaque to the
/// adapter — only their sha256 matters for the determinism check.
const ENVD_BIN_BYTES: &[u8] = b"\x7fELF-pretend-this-is-the-envd-binary";

/// Seed a `FakeSnapshotStore` with a minimal valid bundle (the three
/// checkpoint images + envd-state, no rootfs delta needed for stage) and
/// return a manifest carrying `envd_sha256`. Mirrors the inline seeding
/// in `stage_snapshot_materializes_bundle_and_returns_path`.
async fn seed_stageable_bundle(
    store: &FakeSnapshotStore,
    sandbox_id: &str,
    envd_sha256: String,
) -> SnapshotManifest {
    let memory_zst = build_memory_zst(&[CHECKPOINT_FILE_1, CHECKPOINT_FILE_2, CHECKPOINT_FILE_3]);
    let memory_part = store
        .put_part("memory.zst", memory_zst)
        .await
        .expect("put memory.zst");
    let rootfs_part = store
        .put_part("rootfs-delta.tar.zst", ROOTFS_DELTA_BYTES.to_vec())
        .await
        .expect("put rootfs-delta");
    let envd_part = store
        .put_part("envd-state.json", ENVD_STATE_BYTES.to_vec())
        .await
        .expect("put envd-state");
    let mut manifest = SnapshotManifest {
        sandbox_id: sandbox_id.to_owned(),
        runtime_class: "gvisor".to_owned(),
        sha256: String::new(),
        created_at: FIXED_NOW.to_owned(),
        parts: vec![memory_part, rootfs_part, envd_part],
        snapshot_ref: format!("s3://test-bucket/{sandbox_id}/manifest.json"),
        envd_sha256,
        runsc_version: String::new(),
        envd_captured: false,
        template_ref: String::new(),
        template_image: String::new(),
    };
    manifest.recompute_aggregate();
    store.put_manifest(&manifest).await.expect("seed manifest");
    manifest
}

/// Build the fakes + adapter for a stage test, with the node-staged envd
/// binary pointed at `envd_bin` so the determinism check has a real file
/// to hash.
fn stage_adapter(
    work_root: &std::path::Path,
    staging_root: &std::path::Path,
    envd_bin: &std::path::Path,
) -> GvisorAdapter {
    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "unused-on-stage".to_owned(),
            pod_ip: "0.0.0.0".to_owned(),
            sandbox_id: "unused".to_owned(),
        },
        Vec::new(),
    ));
    let envd = Arc::new(FakeEnvdState::new(Vec::new()));
    GvisorAdapter::new(containerd, envd, work_root)
        .with_clock(Arc::new(FixedClock::new(FIXED_NOW)))
        .with_staging_root(staging_root)
        .with_envd_bin_path(envd_bin)
}

#[tokio::test]
async fn pause_records_node_staged_envd_digest() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let runsc_stub = write_runsc_stub(tmp.path());
    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("create work_root");
    let runsc_root = seed_runsc_root(tmp.path(), "cid-envd");

    // The node-staged envd binary the worker hashes at pause time.
    let envd_bin = tmp.path().join("envd");
    std::fs::write(&envd_bin, ENVD_BIN_BYTES).expect("write envd binary");

    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "cid-envd".to_owned(),
            pod_ip: "10.42.0.9".to_owned(),
            sandbox_id: "sbx-envd-digest".to_owned(),
        },
        ROOTFS_DELTA_BYTES.to_vec(),
    ));
    let envd = Arc::new(FakeEnvdState::new(ENVD_STATE_BYTES.to_vec()));
    let adapter = GvisorAdapter::new(containerd, envd, &work_root)
        .with_runsc_binary(runsc_stub)
        .with_runsc_root(runsc_root)
        .with_clock(Arc::new(FixedClock::new(FIXED_NOW)))
        .with_post_probe_drain(std::time::Duration::ZERO)
        .with_envd_bin_path(&envd_bin);

    let store = FakeSnapshotStore::new();
    let manifest = adapter
        .pause("pod-uid-envd", &store)
        .await
        .expect("pause should succeed against fakes");

    // The recorded digest is the sha256 of the staged binary's bytes, and
    // the persisted manifest carries it too (self-describing resume).
    assert_eq!(manifest.envd_sha256, sha256_hex(ENVD_BIN_BYTES));
    let fetched = store.get_manifest().await.expect("get_manifest");
    assert_eq!(fetched.envd_sha256, sha256_hex(ENVD_BIN_BYTES));
}

#[tokio::test]
async fn pause_tolerates_missing_envd_binary() {
    // A worker without the envd mount (older deploy) can't read the
    // binary; pause must still succeed, recording an empty digest so the
    // restore path falls back to per-part verification.
    let tmp = tempfile::tempdir().expect("tempdir");
    let runsc_stub = write_runsc_stub(tmp.path());
    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("create work_root");
    let runsc_root = seed_runsc_root(tmp.path(), "cid");

    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "cid".to_owned(),
            pod_ip: "10.42.0.9".to_owned(),
            sandbox_id: "sbx-no-envd".to_owned(),
        },
        ROOTFS_DELTA_BYTES.to_vec(),
    ));
    let envd = Arc::new(FakeEnvdState::new(ENVD_STATE_BYTES.to_vec()));
    let adapter = GvisorAdapter::new(containerd, envd, &work_root)
        .with_runsc_binary(runsc_stub)
        .with_runsc_root(runsc_root)
        .with_clock(Arc::new(FixedClock::new(FIXED_NOW)))
        .with_post_probe_drain(std::time::Duration::ZERO)
        .with_envd_bin_path(tmp.path().join("does-not-exist"));

    let store = FakeSnapshotStore::new();
    let manifest = adapter
        .pause("pod-uid-no-envd", &store)
        .await
        .expect("pause must succeed even when the envd binary is unreadable");
    assert_eq!(
        manifest.envd_sha256, "",
        "an unreadable envd binary records an empty digest, not a failure"
    );
}

#[tokio::test]
async fn stage_fails_closed_on_drifted_envd_binary() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let work_root = tmp.path().join("snapshots");
    let staging_root = tmp.path().join("staging");

    // The snapshot was checkpointed against ENVD_BIN_BYTES...
    let store = FakeSnapshotStore::new();
    let manifest = seed_stageable_bundle(&store, "sbx-drift", sha256_hex(ENVD_BIN_BYTES)).await;

    // ...but THIS node has staged a different envd binary.
    let envd_bin = tmp.path().join("envd");
    std::fs::write(&envd_bin, b"a-different-envd-binary").expect("write drifted envd");

    let adapter = stage_adapter(&work_root, &staging_root, &envd_bin);
    let err = adapter
        .stage_snapshot("sbx-drift", &manifest, &store, None)
        .await
        .expect_err("a drifted envd binary must fail the restore closed");
    match err {
        sandbawks_types::runtime::RuntimeError::RestoreFailed(msg) => {
            assert!(
                msg.contains("envd") && msg.contains("270"),
                "error must explain the envd drift and cite #270, got: {msg}"
            );
        }
        other => panic!("expected RestoreFailed, got {other:?}"),
    }

    // Fail-closed means fail *early*: no bundle was materialized.
    let per_sandbox = staging_root.join("sbx-drift");
    let leftover = std::fs::read_dir(&per_sandbox)
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(
        leftover, 0,
        "no staging dir should be left behind when the envd check fails closed"
    );
}

#[tokio::test]
async fn stage_accepts_matching_envd_binary() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let work_root = tmp.path().join("snapshots");
    let staging_root = tmp.path().join("staging");

    let store = FakeSnapshotStore::new();
    let manifest = seed_stageable_bundle(&store, "sbx-match", sha256_hex(ENVD_BIN_BYTES)).await;

    // This node's staged binary is byte-identical to checkpoint time.
    let envd_bin = tmp.path().join("envd");
    std::fs::write(&envd_bin, ENVD_BIN_BYTES).expect("write matching envd");

    let adapter = stage_adapter(&work_root, &staging_root, &envd_bin);
    let path = adapter
        .stage_snapshot("sbx-match", &manifest, &store, None)
        .await
        .expect("a byte-identical envd binary must pass the determinism check");
    assert!(
        std::path::Path::new(&path).is_dir(),
        "stage should materialize the bundle when the envd digest matches"
    );
}

// ---------------------------------------------------------------------------
// Capture-in-checkpoint envd (epic #466 / issue #469): a relocate-mode
// sandbox runs envd from the captured tmpfs, so its code pages ride in the
// checkpoint's `pages.img`. The pause path stamps `envd_captured` from the
// container's mount set, and the restore path skips the #270 host-binary
// comparison when it sees that flag (the host binary is expected to drift
// after an envd upgrade — that is the whole point).
// ---------------------------------------------------------------------------

/// A capture-mode snapshot restores even when this node's staged envd binary
/// has drifted: the host-binary determinism comparison is *skipped*, because
/// the checkpointed envd's pages came from `pages.img`, not the host file.
/// Compare with `stage_fails_closed_on_drifted_envd_binary`, which is the same
/// drift on a *legacy* snapshot — that one must fail closed.
#[tokio::test]
async fn stage_skips_host_check_for_capture_mode_snapshot() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let work_root = tmp.path().join("snapshots");
    let staging_root = tmp.path().join("staging");

    // The snapshot was checkpointed against ENVD_BIN_BYTES and is capture-mode.
    let store = FakeSnapshotStore::new();
    let mut manifest =
        seed_stageable_bundle(&store, "sbx-capture", sha256_hex(ENVD_BIN_BYTES)).await;
    manifest.envd_captured = true;
    store
        .put_manifest(&manifest)
        .await
        .expect("re-seed capture-mode manifest");

    // This node has staged a *different* envd binary — which would fail a
    // legacy snapshot closed, but must be ignored for a capture-mode one.
    let envd_bin = tmp.path().join("envd");
    std::fs::write(&envd_bin, b"a-different-envd-binary").expect("write drifted envd");

    let adapter = stage_adapter(&work_root, &staging_root, &envd_bin);
    let path = adapter
        .stage_snapshot("sbx-capture", &manifest, &store, None)
        .await
        .expect("a capture-mode snapshot must restore despite a drifted host envd binary");
    assert!(
        std::path::Path::new(&path).is_dir(),
        "stage should materialize the bundle when the host check is skipped"
    );
}

/// Pause stamps `envd_captured = true` when the container carries the relocate
/// env (`SANDBAWKS_ENVD_RELOCATE_TARGET`), the marker the controller sets on
/// every relocate-mode Pod. envd relocated into the captured root overlay, so
/// the snapshot is self-describing as capture-mode.
#[tokio::test]
async fn pause_marks_capture_mode_when_relocate_env_present() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let runsc_stub = write_runsc_stub(tmp.path());
    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("create work_root");
    let runsc_root = seed_runsc_root(tmp.path(), "cid-cap");

    let envd_bin = tmp.path().join("envd");
    std::fs::write(&envd_bin, ENVD_BIN_BYTES).expect("write envd binary");

    let containerd = Arc::new(
        FakeContainerd::new(
            PodResolution {
                container_id: "cid-cap".to_owned(),
                pod_ip: "10.42.0.9".to_owned(),
                sandbox_id: "sbx-cap".to_owned(),
            },
            ROOTFS_DELTA_BYTES.to_vec(),
        )
        // The relocate target lives in the rootfs (no mount); the env is what
        // marks the container capture-mode.
        .with_relocate_target("/var/tmp/envd/run/envd"),
    );
    let envd = Arc::new(FakeEnvdState::new(ENVD_STATE_BYTES.to_vec()));
    let adapter = GvisorAdapter::new(containerd, envd, &work_root)
        .with_runsc_binary(runsc_stub)
        .with_runsc_root(runsc_root)
        .with_clock(Arc::new(FixedClock::new(FIXED_NOW)))
        .with_post_probe_drain(std::time::Duration::ZERO)
        .with_envd_bin_path(&envd_bin);

    let store = FakeSnapshotStore::new();
    let manifest = adapter
        .pause("pod-uid-cap", &store)
        .await
        .expect("pause should succeed against fakes");

    assert!(
        manifest.envd_captured,
        "pause must mark capture-mode when the relocate env is set"
    );
    let fetched = store.get_manifest().await.expect("get_manifest");
    assert!(
        fetched.envd_captured,
        "the persisted manifest must carry the capture-mode flag"
    );
}

/// Pause stamps `envd_captured = false` for a legacy Pod that carries no
/// relocate env (an older deploy, pre-#467/#468), preserving the fail-closed
/// host-binary guard on its snapshots.
#[tokio::test]
async fn pause_marks_legacy_when_no_relocate_env() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let runsc_stub = write_runsc_stub(tmp.path());
    let work_root = tmp.path().join("snapshots");
    std::fs::create_dir_all(&work_root).expect("create work_root");
    let runsc_root = seed_runsc_root(tmp.path(), "cid-legacy");

    let envd_bin = tmp.path().join("envd");
    std::fs::write(&envd_bin, ENVD_BIN_BYTES).expect("write envd binary");

    // No relocate target set on the fake → legacy.
    let containerd = Arc::new(FakeContainerd::new(
        PodResolution {
            container_id: "cid-legacy".to_owned(),
            pod_ip: "10.42.0.9".to_owned(),
            sandbox_id: "sbx-legacy-cap".to_owned(),
        },
        ROOTFS_DELTA_BYTES.to_vec(),
    ));
    let envd = Arc::new(FakeEnvdState::new(ENVD_STATE_BYTES.to_vec()));
    let adapter = GvisorAdapter::new(containerd, envd, &work_root)
        .with_runsc_binary(runsc_stub)
        .with_runsc_root(runsc_root)
        .with_clock(Arc::new(FixedClock::new(FIXED_NOW)))
        .with_post_probe_drain(std::time::Duration::ZERO)
        .with_envd_bin_path(&envd_bin);

    let store = FakeSnapshotStore::new();
    let manifest = adapter
        .pause("pod-uid-legacy", &store)
        .await
        .expect("pause should succeed against fakes");

    assert!(
        !manifest.envd_captured,
        "pause must mark legacy (not capture-mode) when no relocate env is present"
    );
}

// ---------------------------------------------------------------------------
// OTel lifecycle orchestration tests (issue #403).
//
// Verify that the node adapter calls `/__quiesce` before `runsc checkpoint`
// and `/__rearm` after the bind-wait, and that failures on either endpoint
// are logged but never propagate to fail the pause or resume.
// ---------------------------------------------------------------------------

/// Pause calls `quiesce` after the envd state fetch and before the runsc
/// checkpoint; a quiesce failure must not fail the pause (#403).
#[tokio::test]
async fn pause_calls_quiesce_before_checkpoint_and_tolerates_failure() {
    let tmp = tempfile::tempdir().expect("tempdir");

    // Exercise both the success and failure paths in one function to
    // avoid duplicating all the setup boilerplate.
    for quiesce_should_fail in [false, true] {
        // The checkpoint stub touches this marker; the fake's quiesce asserts
        // it is still absent when quiesce fires, pinning quiesce-before-
        // checkpoint ordering (per-iteration name so the two runs don't share
        // a marker).
        let checkpoint_marker = tmp
            .path()
            .join(format!("checkpoint-ran-{quiesce_should_fail}"));
        let runsc_stub = write_runsc_stub_with_marker(tmp.path(), Some(&checkpoint_marker));
        let work_root = tmp.path().join(format!("snapshots-{quiesce_should_fail}"));
        std::fs::create_dir_all(&work_root).expect("create work_root");
        let runsc_root = seed_runsc_root(tmp.path(), "cid403");

        let containerd = Arc::new(FakeContainerd::new(
            PodResolution {
                container_id: "cid403".to_owned(),
                pod_ip: "10.0.0.1".to_owned(),
                sandbox_id: "sbx403".to_owned(),
            },
            ROOTFS_DELTA_BYTES.to_vec(),
        ));
        let mut envd_fake = FakeEnvdState::new(ENVD_STATE_BYTES.to_vec())
            .with_quiesce_before_marker(&checkpoint_marker);
        if quiesce_should_fail {
            envd_fake = envd_fake.with_quiesce_fail();
        }
        let envd = Arc::new(envd_fake);
        let clock = Arc::new(FixedClock::new(FIXED_NOW));

        let adapter = GvisorAdapter::new(containerd, envd.clone(), &work_root)
            .with_runsc_binary(&runsc_stub)
            .with_runsc_root(&runsc_root)
            .with_clock(clock)
            .with_post_probe_drain(std::time::Duration::ZERO);

        let store = FakeSnapshotStore::new();
        // Pause must succeed regardless of the quiesce outcome.
        adapter.pause("uid-403", &store).await.unwrap_or_else(|e| {
            panic!("pause must succeed even when quiesce fails={quiesce_should_fail}: {e}")
        });

        // Quiesce must have been called exactly once with the pod IP.
        let quiesce_calls = envd.quiesce_calls.lock().expect("not poisoned").clone();
        assert_eq!(
            quiesce_calls.as_slice(),
            &["10.0.0.1".to_owned()],
            "quiesce must be called once with the pod IP (fail={quiesce_should_fail})"
        );

        // The checkpoint stub ran (marker now present), so the in-quiesce
        // absence assertion above was a real before/after check, not vacuous.
        assert!(
            checkpoint_marker.exists(),
            "checkpoint stub must have run (marker absent after pause)"
        );
    }
}

/// Resume calls `rearm` after `wait_for_tcp_bind`; a rearm failure must not
/// fail the resume (#403).
#[tokio::test]
async fn resume_calls_rearm_after_bind_wait_and_tolerates_failure() {
    let tmp = tempfile::tempdir().expect("tempdir");

    for rearm_should_fail in [false, true] {
        // The restore stub touches this marker; the fake's rearm asserts it is
        // present when rearm fires, pinning rearm-after-restore ordering.
        let restore_marker = tmp.path().join(format!("restore-ran-{rearm_should_fail}"));
        let runsc_stub = write_runsc_restore_stub_with_marker(tmp.path(), Some(&restore_marker));
        let work_root = tmp
            .path()
            .join(format!("snapshots-rearm-{rearm_should_fail}"));
        std::fs::create_dir_all(&work_root).expect("create work_root");
        let runsc_root = seed_runsc_root(tmp.path(), "cid403r");

        let store = FakeSnapshotStore::new();
        let memory_zst =
            build_memory_zst(&[CHECKPOINT_FILE_1, CHECKPOINT_FILE_2, CHECKPOINT_FILE_3]);
        let memory_part = store
            .put_part("memory.zst", memory_zst)
            .await
            .expect("put memory");
        let rootfs_part = store
            .put_part("rootfs-delta.tar.zst", ROOTFS_DELTA_BYTES.to_vec())
            .await
            .expect("put rootfs");
        let envd_part = store
            .put_part("envd-state.json", ENVD_STATE_BYTES.to_vec())
            .await
            .expect("put envd-state");
        let mut manifest = SnapshotManifest {
            envd_sha256: String::new(),
            runsc_version: String::new(),
            envd_captured: false,
            sandbox_id: "sbx403r".to_owned(),
            runtime_class: "gvisor".to_owned(),
            sha256: String::new(),
            created_at: FIXED_NOW.to_owned(),
            parts: vec![memory_part, rootfs_part, envd_part],
            snapshot_ref: "s3://test-bucket/sbx403r/manifest.json".to_owned(),
            template_ref: String::new(),
            template_image: String::new(),
        };
        manifest.recompute_aggregate();
        store.put_manifest(&manifest).await.expect("seed manifest");

        // Bind a local listener so the TCP-bind wait in resume completes.
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind");
        let envd_port = listener.local_addr().expect("addr").port();

        let containerd = Arc::new(FakeContainerd::new(
            PodResolution {
                container_id: "cid403r".to_owned(),
                pod_ip: "127.0.0.1".to_owned(),
                sandbox_id: "sbx403r".to_owned(),
            },
            Vec::new(),
        ));
        let mut envd_fake = FakeEnvdState::new(Vec::new()).with_rearm_after_marker(&restore_marker);
        if rearm_should_fail {
            envd_fake = envd_fake.with_rearm_fail();
        }
        let envd = Arc::new(envd_fake);
        let clock = Arc::new(FixedClock::new(FIXED_NOW));

        let adapter = GvisorAdapter::new(containerd, envd.clone(), &work_root)
            .with_runsc_binary(&runsc_stub)
            .with_runsc_root(&runsc_root)
            .with_clock(clock)
            .with_envd_port(envd_port);

        // Resume must succeed regardless of the rearm outcome.
        adapter
            .resume(&manifest, "uid-403r", &store)
            .await
            .unwrap_or_else(|e| {
                panic!("resume must succeed even when rearm fails={rearm_should_fail}: {e}")
            });

        // Rearm must have been called exactly once with the pod IP.
        let rearm_calls = envd.rearm_calls.lock().expect("not poisoned").clone();
        assert_eq!(
            rearm_calls.as_slice(),
            &["127.0.0.1".to_owned()],
            "rearm must be called once with the pod IP (fail={rearm_should_fail})"
        );

        drop(listener);
    }
}
