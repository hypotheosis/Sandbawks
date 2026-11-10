//! Integration test for the `crdgen` binary.
//!
//! Invokes the binary against a hermetic per-process temp directory under
//! `CARGO_TARGET_TMPDIR` (cleaned by `cargo clean`) and asserts that all
//! five CRD YAML files are written and look like CustomResourceDefinitions.

use std::path::PathBuf;
use std::process::Command;

const EXPECTED_FILES: [&str; 5] = [
    "sandboxes.sandbawks.dev.yaml",
    "sandboxtemplates.sandbawks.dev.yaml",
    "sandboxwarmpools.sandbawks.dev.yaml",
    "teamapikeys.sandbawks.dev.yaml",
    "snapshotjobs.sandbawks.dev.yaml",
];

#[test]
fn crdgen_emits_all_files() {
    let tmp = hermetic_tempdir("emits_all_files");
    let status = Command::new(env!("CARGO_BIN_EXE_crdgen"))
        .arg(&tmp)
        .status()
        .expect("crdgen binary should be runnable");
    assert!(status.success(), "crdgen exited non-zero");

    for name in EXPECTED_FILES {
        let path = tmp.join(name);
        assert!(path.exists(), "missing: {}", path.display());
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(
            body.starts_with("apiVersion: apiextensions.k8s.io/v1"),
            "{} did not start with the expected apiVersion line; got: {}",
            path.display(),
            body.lines().next().unwrap_or_default(),
        );
        assert!(body.contains("kind: CustomResourceDefinition"));
    }
}

#[test]
fn crdgen_is_idempotent() {
    let tmp = hermetic_tempdir("is_idempotent");
    for _ in 0..2 {
        let status = Command::new(env!("CARGO_BIN_EXE_crdgen"))
            .arg(&tmp)
            .status()
            .expect("crdgen binary should be runnable");
        assert!(status.success(), "crdgen exited non-zero");
    }
    // Re-running must produce byte-identical files, which is the property
    // the CI drift-check job (T12 / #13) depends on.
    let snapshots: Vec<(String, String)> = EXPECTED_FILES
        .iter()
        .map(|name| {
            let body = std::fs::read_to_string(tmp.join(name)).unwrap();
            ((*name).to_owned(), body)
        })
        .collect();

    let status = Command::new(env!("CARGO_BIN_EXE_crdgen"))
        .arg(&tmp)
        .status()
        .expect("crdgen binary should be runnable");
    assert!(status.success());
    for (name, before) in snapshots {
        let after = std::fs::read_to_string(tmp.join(&name)).unwrap();
        assert_eq!(before, after, "{name} changed between runs");
    }
}

/// Per-test temp directory under `CARGO_TARGET_TMPDIR` — hermetic and
/// self-cleaning on `cargo clean` without pulling in `tempfile`. The `label`
/// disambiguates concurrent tests in the same binary, which share a PID.
fn hermetic_tempdir(label: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    p.push(format!("crdgen-{}-{label}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}
