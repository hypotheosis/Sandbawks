//! Integration tests for the envd-state.json snapshot module.
//!
//! Mirrors the test list in Plan 03 T6 (#59): empty / processes / ptys
//! / watches round-trips, the forward-compatibility invariant that
//! unknown JSON fields are accepted, and the version-mismatch
//! invariant that foreign schemas are rejected.

use std::collections::BTreeMap;
use std::path::PathBuf;

use envd::fingerprint::RootfsFingerprint;
use envd::state::{
    EnvdState, ProcessRegistry, ProcessSnapshot, PtyRegistry, PtySnapshot, RestoreError,
    WatchRegistry, WatchSnapshot, SCHEMA_VERSION,
};

/// Round-trip an [`EnvdState`] through JSON and assert byte-identical
/// equality of the decoded structure. Used by every sub-test below.
fn roundtrip(state: &EnvdState) {
    let json = state.to_json().expect("serialize");
    let decoded = EnvdState::from_json(json.as_bytes()).expect("deserialize");
    assert_eq!(state, &decoded, "round-tripped state differs from original");
}

#[test]
fn empty_state_roundtrip() {
    let processes = ProcessRegistry::new();
    let ptys = PtyRegistry::new();
    let watches = WatchRegistry::new();
    let state = EnvdState::capture(&processes, &ptys, &watches);
    assert_eq!(state.version, SCHEMA_VERSION);
    assert!(state.processes.is_empty());
    assert!(state.ptys.is_empty());
    assert!(state.watches.is_empty());
    roundtrip(&state);
}

#[test]
fn state_with_processes_roundtrip() {
    let mut processes = ProcessRegistry::new();
    processes.push(ProcessSnapshot {
        pid: 1234,
        cmd: "/usr/bin/env".to_string(),
        args: vec!["python3".to_string(), "-V".to_string()],
        env: {
            let mut m = BTreeMap::new();
            m.insert("PATH".to_string(), "/usr/bin:/bin".to_string());
            m.insert("HOME".to_string(), "/home/user".to_string());
            m
        },
        cwd: PathBuf::from("/home/user"),
        started_at: 1_700_000_000,
    });
    processes.push(ProcessSnapshot {
        pid: 4242,
        cmd: "/bin/sleep".to_string(),
        args: vec!["60".to_string()],
        env: BTreeMap::new(),
        cwd: PathBuf::from("/"),
        started_at: 1_700_000_042,
    });
    let state = EnvdState::capture(&processes, &PtyRegistry::new(), &WatchRegistry::new());
    assert_eq!(state.processes.len(), 2);
    roundtrip(&state);

    // restore preserves the order callers pushed snapshots in
    let restored = state.restore().expect("restore");
    assert_eq!(restored.processes.snapshots(), processes.snapshots());
}

#[test]
fn state_with_ptys_roundtrip() {
    let mut ptys = PtyRegistry::new();
    ptys.push(PtySnapshot {
        id: 7,
        pid: 2001,
        rows: 24,
        cols: 80,
        cmd: "/bin/bash".to_string(),
        args: vec!["--login".to_string()],
    });
    ptys.push(PtySnapshot {
        id: 8,
        pid: 2002,
        rows: 50,
        cols: 200,
        cmd: "/usr/bin/vim".to_string(),
        args: Vec::new(),
    });
    let state = EnvdState::capture(&ProcessRegistry::new(), &ptys, &WatchRegistry::new());
    assert_eq!(state.ptys.len(), 2);
    roundtrip(&state);

    let restored = state.restore().expect("restore");
    assert_eq!(restored.ptys.snapshots(), ptys.snapshots());
}

#[test]
fn state_with_watches_roundtrip() {
    let mut watches = WatchRegistry::new();
    watches.push(WatchSnapshot {
        path: PathBuf::from("/home/user/project"),
        subscriber_id: 1,
    });
    watches.push(WatchSnapshot {
        path: PathBuf::from("/tmp"),
        subscriber_id: 42,
    });
    let state = EnvdState::capture(&ProcessRegistry::new(), &PtyRegistry::new(), &watches);
    assert_eq!(state.watches.len(), 2);
    roundtrip(&state);

    let restored = state.restore().expect("restore");
    assert_eq!(restored.watches.snapshots(), watches.snapshots());
}

#[test]
fn forwards_compat_unknown_field_ignored() {
    // Construct a JSON payload that matches the current shape but
    // carries an extra `future_thing` field at the top level and an
    // extra `priority` field on the embedded process. A future schema
    // version could add either; an older envd loading the file must
    // silently ignore them rather than rejecting the snapshot.
    let json = format!(
        r#"{{
            "version": {SCHEMA_VERSION},
            "processes": [
                {{
                    "pid": 7,
                    "cmd": "/bin/true",
                    "args": [],
                    "env": {{}},
                    "cwd": "/",
                    "started_at": 0,
                    "priority": "low"
                }}
            ],
            "ptys": [],
            "watches": [],
            "future_thing": ["anything", 42, null]
        }}"#
    );
    let state = EnvdState::from_json(json.as_bytes()).expect("unknown fields must be ignored");
    assert_eq!(state.processes.len(), 1);
    assert_eq!(state.processes[0].pid, 7);
    // Round-tripping the resulting state must still succeed even
    // though the original carried fields we no longer emit.
    roundtrip(&state);
}

#[test]
fn version_mismatch_rejected() {
    // Encode a state whose version is one higher than the current
    // SCHEMA_VERSION and verify restore() refuses it. A patched-up
    // envd that ignored the version field would silently load a
    // payload it does not understand.
    let state = EnvdState {
        version: SCHEMA_VERSION + 1,
        processes: Vec::new(),
        ptys: Vec::new(),
        watches: Vec::new(),
        rootfs_fingerprint: None,
    };
    let err = state
        .restore()
        .expect_err("future-version state must be rejected");
    match err {
        RestoreError::VersionMismatch { found, expected } => {
            assert_eq!(found, SCHEMA_VERSION + 1);
            assert_eq!(expected, SCHEMA_VERSION);
        }
        other => panic!("expected VersionMismatch, got {other:?}"),
    }
}

#[test]
fn write_and_read_roundtrip_through_disk() {
    // Covers the file-I/O helpers used by the actual pause/resume
    // path: atomic temp-file rename, byte-identical readback.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("envd-state.json");

    let mut processes = ProcessRegistry::new();
    processes.push(ProcessSnapshot {
        pid: 9000,
        cmd: "/usr/bin/python3".to_string(),
        args: vec!["-c".to_string(), "print(1)".to_string()],
        env: BTreeMap::new(),
        cwd: PathBuf::from("/tmp"),
        started_at: 1_700_000_999,
    });
    let state = EnvdState::capture(&processes, &PtyRegistry::new(), &WatchRegistry::new());

    state.write_to(&path).expect("write_to");
    let read_back = EnvdState::read_from(&path).expect("read_from");
    assert_eq!(state, read_back);
}

#[test]
fn capture_has_no_rootfs_fingerprint_by_default() {
    // The pure constructor knows no rootfs path, so the oracle is absent
    // until the binary's capture path folds one in.
    let state = EnvdState::capture(
        &ProcessRegistry::new(),
        &PtyRegistry::new(),
        &WatchRegistry::new(),
    );
    assert!(state.rootfs_fingerprint.is_none());
}

#[test]
fn rootfs_fingerprint_survives_roundtrip() {
    // The fingerprint field must serialize and decode byte-stably so the
    // resume-side comparison sees exactly what pause recorded (#236).
    let fingerprint = RootfsFingerprint {
        digest: "a".repeat(64),
        entries: 1240,
    };
    let state = EnvdState::capture(
        &ProcessRegistry::new(),
        &PtyRegistry::new(),
        &WatchRegistry::new(),
    )
    .with_rootfs_fingerprint(fingerprint.clone());
    assert_eq!(state.rootfs_fingerprint.as_ref(), Some(&fingerprint));
    roundtrip(&state);
}

#[test]
fn missing_rootfs_fingerprint_decodes_to_none() {
    // A snapshot written by an envd predating #236 has no
    // `rootfs_fingerprint` key. `#[serde(default)]` must decode it to
    // `None` so the new envd still loads the older snapshot — and the
    // SCHEMA_VERSION must remain unbumped for that to be legal.
    let json = format!(
        r#"{{
            "version": {SCHEMA_VERSION},
            "processes": [],
            "ptys": [],
            "watches": []
        }}"#
    );
    let state =
        EnvdState::from_json(json.as_bytes()).expect("absent fingerprint must default to None");
    assert!(state.rootfs_fingerprint.is_none());
    // restore() must still succeed — the absent oracle is not an error.
    state.restore().expect("restore older snapshot");
}
