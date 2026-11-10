//! Property-based tests for the envd-state.json snapshot module.
//!
//! Plan 03 T6 (#59) calls out two `hegel`-driven invariants:
//!
//! 1. **Round-trip** — an arbitrary [`EnvdState`] survives serialize +
//!    deserialize with byte-identical structural equality.
//! 2. **Determinism** — the same state serialized twice produces a
//!    byte-identical JSON string. This is the property that makes the
//!    pause/resume snapshot bundle reproducible across hosts, which in
//!    turn makes the snapshot's content-hash a stable cache key.
//!
//! The generators below are kept minimal but cover the full structural
//! surface of [`EnvdState`]: every field of every snapshot variant is
//! filled, collections are allowed to be empty, and strings include
//! the full Unicode range that `hegel::generators::text()` ships.

use std::collections::BTreeMap;
use std::path::PathBuf;

use hegel::generators;
use hegel::TestCase;

use envd::fingerprint::RootfsFingerprint;
use envd::state::{EnvdState, ProcessSnapshot, PtySnapshot, WatchSnapshot, SCHEMA_VERSION};

/// Cap on the size of generated collections.
///
/// The snapshot file is sized in the KB range in production (spec §6.1
/// out-of-scope item: "compression — file is small"), so the tests
/// don't need to stress 10k-entry registries. Keeping the cap modest
/// also keeps the serde_json work per case bounded so the 256-case
/// default doesn't make `cargo test` slow.
const MAX_ENTRIES: usize = 6;

/// Cap on the size of generated strings. Same reasoning as
/// [`MAX_ENTRIES`] — bounded work per case, not a constraint on the
/// production type.
const MAX_STRING_LEN: usize = 32;

/// Generate a BTreeMap<String, String> with up to `MAX_ENTRIES` keys.
///
/// `hegel::generators` ships `hashmaps` but no `btreemaps`, so we draw
/// a vec of (key, value) pairs and collect. The `.into_iter().collect()`
/// transparently dedupes — for any given key the last-written value
/// wins, which matches `BTreeMap::insert`'s contract.
#[hegel::composite]
fn env_map(tc: TestCase) -> BTreeMap<String, String> {
    let pairs: Vec<(String, String)> = tc.draw(
        generators::vecs(generators::tuples!(
            generators::text().max_size(MAX_STRING_LEN),
            generators::text().max_size(MAX_STRING_LEN),
        ))
        .max_size(MAX_ENTRIES),
    );
    pairs.into_iter().collect()
}

/// Generate a `Vec<String>` suitable for argv-like fields.
#[hegel::composite]
fn string_vec(tc: TestCase) -> Vec<String> {
    tc.draw(generators::vecs(generators::text().max_size(MAX_STRING_LEN)).max_size(MAX_ENTRIES))
}

/// Generate a path. `text()` produces valid UTF-8 strings, so the
/// resulting `PathBuf` is JSON-encodable (serde_json refuses
/// non-UTF-8 paths). We deliberately allow the empty path — round-trip
/// must work there too.
#[hegel::composite]
fn path(tc: TestCase) -> PathBuf {
    PathBuf::from(tc.draw(generators::text().max_size(MAX_STRING_LEN)))
}

#[hegel::composite]
fn process_snapshot(tc: TestCase) -> ProcessSnapshot {
    ProcessSnapshot {
        pid: tc.draw(generators::integers::<u32>()),
        cmd: tc.draw(generators::text().max_size(MAX_STRING_LEN)),
        args: tc.draw(string_vec()),
        env: tc.draw(env_map()),
        cwd: tc.draw(path()),
        started_at: tc.draw(generators::integers::<i64>()),
    }
}

#[hegel::composite]
fn pty_snapshot(tc: TestCase) -> PtySnapshot {
    PtySnapshot {
        id: tc.draw(generators::integers::<u64>()),
        pid: tc.draw(generators::integers::<u32>()),
        rows: tc.draw(generators::integers::<u16>()),
        cols: tc.draw(generators::integers::<u16>()),
        cmd: tc.draw(generators::text().max_size(MAX_STRING_LEN)),
        args: tc.draw(string_vec()),
    }
}

#[hegel::composite]
fn watch_snapshot(tc: TestCase) -> WatchSnapshot {
    WatchSnapshot {
        path: tc.draw(path()),
        subscriber_id: tc.draw(generators::integers::<u64>()),
    }
}

/// Generate an optional rootfs fingerprint (#236). The structural
/// surface that matters for round-trip / determinism is the `Option`
/// wrapper plus two scalar fields; the `digest` need not be a *real*
/// hash for the serde invariants, so any text + integer pair exercises
/// the encoding.
#[hegel::composite]
fn rootfs_fingerprint(tc: TestCase) -> Option<RootfsFingerprint> {
    if tc.draw(generators::booleans()) {
        Some(RootfsFingerprint {
            digest: tc.draw(generators::text().max_size(MAX_STRING_LEN)),
            entries: tc.draw(generators::integers::<u64>()),
        })
    } else {
        None
    }
}

#[hegel::composite]
fn envd_state(tc: TestCase) -> EnvdState {
    EnvdState {
        // Always the current schema version — round-trip is about
        // wire shape, not the version-mismatch path (that's a
        // dedicated unit test in tests/state.rs).
        version: SCHEMA_VERSION,
        processes: tc.draw(generators::vecs(process_snapshot()).max_size(MAX_ENTRIES)),
        ptys: tc.draw(generators::vecs(pty_snapshot()).max_size(MAX_ENTRIES)),
        watches: tc.draw(generators::vecs(watch_snapshot()).max_size(MAX_ENTRIES)),
        rootfs_fingerprint: tc.draw(rootfs_fingerprint()),
    }
}

#[hegel::test]
fn arbitrary_state_roundtrips_through_json(tc: TestCase) {
    let state = tc.draw(envd_state());
    let json = state.to_json().expect("serialize");
    let decoded = EnvdState::from_json(json.as_bytes()).expect("deserialize");
    assert_eq!(state, decoded);
}

#[hegel::test]
fn serialization_is_deterministic(tc: TestCase) {
    let state = tc.draw(envd_state());
    let first = state.to_json().expect("serialize 1");
    let second = state.to_json().expect("serialize 2");
    assert_eq!(
        first, second,
        "expected byte-identical JSON across two serializations of the same state"
    );
}
