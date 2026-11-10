//! Build script that resolves the git revision the binary is built from and
//! exposes it to the crate source as the `SANDBAWKS_GIT_SHA` compile-time env
//! var (read by `init::build_resource` and stamped onto every OTel resource).
//!
//! Resolution order (first non-empty wins):
//!
//! 1. The `SANDBAWKS_GIT_SHA` build-time env var — the value CI / the Docker
//!    `--build-arg GIT_SHA=...` injects. The image build context excludes
//!    `.git` (see `.dockerignore`), so this is the *only* source that resolves
//!    inside a container image.
//! 2. `git rev-parse HEAD` (with a `-dirty` suffix when the working tree has
//!    uncommitted changes) — the local `cargo build` path.
//! 3. The sentinel `"unknown"` — never fail the build over a missing SHA.
//!
//! Note for Python/JS developers: a `build.rs` at a crate root is Rust's
//! standard "run code at compile time" hook. Cargo compiles and runs it before
//! the crate itself; anything it prints as `cargo:rustc-env=KEY=VAL` becomes a
//! compile-time env var that `env!`/`option_env!` can read from the crate.

use std::path::Path;
use std::process::Command;

fn main() {
    // Re-run this script (and therefore re-stamp the SHA) whenever the injected
    // value changes, even if no tracked file did. Without at least one
    // `rerun-if-*` directive Cargo falls back to "re-run on any file change",
    // which is both noisier and wrong here (the SHA depends on git/env state,
    // not on this crate's sources).
    println!("cargo:rerun-if-env-changed=SANDBAWKS_GIT_SHA");

    let sha = injected_sha()
        .or_else(git_sha)
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=SANDBAWKS_GIT_SHA={sha}");
}

/// The explicitly-injected SHA (CI / Docker build-arg), trimmed; `None` when
/// the var is unset or blank so the caller can fall through to git.
fn injected_sha() -> Option<String> {
    std::env::var("SANDBAWKS_GIT_SHA")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Resolve the SHA from the local git checkout, suffixed `-dirty` when the
/// working tree has uncommitted changes. Returns `None` when git is absent or
/// this is not a repository (e.g. an unpacked source tarball).
fn git_sha() -> Option<String> {
    let sha = run_git(&["rev-parse", "HEAD"])?;

    // Re-stamp when HEAD moves on local rebuilds. The git dir lives at the
    // workspace root, not this crate's dir, so resolve its absolute path rather
    // than assuming a relative `.git/HEAD`.
    if let Some(git_dir) = run_git(&["rev-parse", "--absolute-git-dir"]) {
        let git_dir = Path::new(&git_dir);
        // `HEAD` is a *symbolic* ref (`ref: refs/heads/<branch>`): its contents
        // change on a branch switch but NOT on a `git commit` to the current
        // branch. Watch it for the switch case...
        watch_if_exists(&git_dir.join("HEAD"));
        // ...and watch the ref it points at, which is the file a commit on the
        // current branch actually rewrites — without this, a local rebuild
        // after a commit keeps re-stamping the *previous* SHA.
        if let Some(symbolic_ref) = run_git(&["symbolic-ref", "-q", "HEAD"]) {
            watch_if_exists(&git_dir.join(symbolic_ref.trim()));
        }
        // Loose refs get folded into `packed-refs` by `git gc` / `git
        // pack-refs` (after which the loose file above vanishes), so watch this
        // too to cover the packed state.
        watch_if_exists(&git_dir.join("packed-refs"));
    }

    let dirty = run_git(&["status", "--porcelain"])
        .map(|status| !status.is_empty())
        .unwrap_or(false);

    Some(if dirty { format!("{sha}-dirty") } else { sha })
}

/// Emit a `rerun-if-changed` directive for `path`, but only when it exists —
/// pointing Cargo at a nonexistent path forces a rebuild on every invocation.
fn watch_if_exists(path: &Path) {
    if path.exists() {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

/// Run `git <args>` and return trimmed stdout, or `None` on any failure
/// (git missing, non-zero exit, empty output).
fn run_git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}
