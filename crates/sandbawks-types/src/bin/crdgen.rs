//! Emit each CRD as YAML into a directory, one file per kind.
//!
//! Usage: `crdgen <out-dir>`
//!
//! Invoked from `cargo run -p sandbawks-types --bin crdgen -- deploy/crds`
//! during development, and from the CI drift-check job (Plan 01 / Task 12).

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use kube::CustomResourceExt;
use sandbawks_types::{Sandbox, SandboxTemplate, SandboxWarmPool, SnapshotJob, TeamAPIKey};

fn main() -> ExitCode {
    let Some(out_dir) = std::env::args().nth(1).map(PathBuf::from) else {
        eprintln!("usage: crdgen <out-dir>");
        return ExitCode::from(2);
    };

    if let Err(e) = run(&out_dir) {
        eprintln!("crdgen: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn run(out_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(out_dir)?;
    write_crd(out_dir, "sandboxes.sandbawks.dev.yaml", &Sandbox::crd())?;
    write_crd(
        out_dir,
        "sandboxtemplates.sandbawks.dev.yaml",
        &SandboxTemplate::crd(),
    )?;
    write_crd(
        out_dir,
        "sandboxwarmpools.sandbawks.dev.yaml",
        &SandboxWarmPool::crd(),
    )?;
    write_crd(
        out_dir,
        "teamapikeys.sandbawks.dev.yaml",
        &TeamAPIKey::crd(),
    )?;
    write_crd(
        out_dir,
        "snapshotjobs.sandbawks.dev.yaml",
        &SnapshotJob::crd(),
    )?;
    Ok(())
}

fn write_crd<T: serde::Serialize>(
    dir: &Path,
    file: &str,
    crd: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = serde_yaml::to_string(crd)?;
    let path = dir.join(file);
    std::fs::write(&path, yaml)?;
    println!("wrote {}", path.display());
    Ok(())
}
