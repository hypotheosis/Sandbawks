# sandbawks Plan 02 — Controller skeleton + state machine

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the `sandbawks-controller` crate with a complete `Sandbox` state machine, a `RuntimeAdapter` trait (defined in `sandbawks-types`), a `FakeRuntimeAdapter` for tests, a kube-rs reconcile loop, and a binary that runs the loop. No real runtime adapter implementations and no Pod materialization land here — that's plans 03 and 05.

**Architecture:** The state machine is a *pure function* `transition(phase, trigger) -> Option<(phase, action)>` with no dependencies on kube or any runtime. The reconcile loop layers status patching and adapter dispatch on top of it. A `SandboxStore` trait abstracts the kube `Api<Sandbox>` calls so reconcile is unit-testable against an in-memory fake. The controller binary wires kube-rs `Controller` to the real store + the fake adapter; later plans swap the fake out.

**Tech Stack:**
- Rust 2021, Cargo workspace
- `kube` (runtime `Controller`, already in workspace deps)
- `k8s-openapi`
- `async-trait` (new workspace dep)
- `tokio` (already in workspace deps)
- `proptest` (new dev-dependency for property tests)
- `tracing` + `tracing-subscriber` (new workspace dep for the subscriber)

**Spec reference:** `docs/superpowers/specs/2026-05-21-sandbawks-mvp-design.md` §3 (architecture), §5 (state machine diagram), §6 (`RuntimeAdapter` trait shape), §14 (sub-plan 2 scope).

---

## File map

**Created:**
- `crates/sandbawks-controller/Cargo.toml`
- `crates/sandbawks-controller/src/lib.rs`
- `crates/sandbawks-controller/src/state_machine.rs`
- `crates/sandbawks-controller/src/fake_adapter.rs`
- `crates/sandbawks-controller/src/store.rs`
- `crates/sandbawks-controller/src/reconcile.rs`
- `crates/sandbawks-controller/src/bin/sandbawks-controller.rs`
- `crates/sandbawks-controller/tests/smoke.rs`
- `crates/sandbawks-controller/tests/state_machine_unit.rs`
- `crates/sandbawks-controller/tests/state_machine_properties.rs`
- `crates/sandbawks-controller/tests/fake_adapter.rs`
- `crates/sandbawks-controller/tests/reconcile.rs`
- `crates/sandbawks-controller/tests/lifecycle.rs`
- `crates/sandbawks-types/src/runtime.rs`
- `changelog/2026-05-22-plan-02-controller-skeleton.md`

**Modified:**
- `Cargo.toml` (add workspace member + `async-trait` + `tracing-subscriber` workspace deps)
- `crates/sandbawks-types/Cargo.toml` (add `async-trait` dep)
- `crates/sandbawks-types/src/lib.rs` (re-export `runtime` module)
- `deploy/helm/sandbawks/templates/controller-deployment.yaml` (drop `--placeholder`, add readiness probe)

---

## Task 1: Workspace + crate scaffold

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/sandbawks-controller/Cargo.toml`
- Create: `crates/sandbawks-controller/src/lib.rs`
- Create: `crates/sandbawks-controller/tests/smoke.rs`

- [ ] **Step 1: Add the new workspace member and three new workspace dependencies**

Edit `Cargo.toml`:

```toml
[workspace]
resolver = "2"
members = [
    "crates/sandbawks-types",
    "crates/sandbawks-controller",
]

[workspace.package]
version = "0.1.0"
edition = "2021"
authors = ["Peter Grace <pete.grace@gmail.com>"]
license = "Apache-2.0"
repository = "https://github.com/pete-grace/sandbawks"
rust-version = "1.80"

[workspace.dependencies]
# CRD / k8s
kube = { version = "0.97", default-features = false, features = ["client", "derive", "runtime", "rustls-tls"] }
k8s-openapi = { version = "0.23", features = ["v1_31"] }
schemars = "0.8"

# serde stack
serde = { version = "1.0.216", features = ["derive"] }
serde_json = "1.0"
serde_yaml = "0.9"

# errors / logging
thiserror = "2.0.6"
tracing = "0.1.41"
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt"] }

# async / runtime
tokio = { version = "1.42.0", features = ["rt-multi-thread", "macros", "time", "signal"] }
async-trait = "0.1.83"
futures = "0.3"
anyhow = "1.0"

[profile.release]
lto = "thin"
codegen-units = 1
strip = "symbols"
```

- [ ] **Step 2: Write the failing smoke test**

Create `crates/sandbawks-controller/tests/smoke.rs`:

```rust
#[test]
fn crate_links() {
    let _ = sandbawks_controller::CRATE_NAME;
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p sandbawks-controller 2>&1 | tail -10`
Expected: failure — workspace member missing / no library target.

- [ ] **Step 4: Create `crates/sandbawks-controller/Cargo.toml`**

```toml
[package]
name = "sandbawks-controller"
version.workspace = true
edition.workspace = true
authors.workspace = true
license.workspace = true
repository.workspace = true
description = "Reconcile loop and Sandbox state machine for the sandbawks Kubernetes sandbox controller."

[dependencies]
sandbawks-types = { path = "../sandbawks-types" }
kube = { workspace = true }
k8s-openapi = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }
tokio = { workspace = true }
async-trait = { workspace = true }
futures = { workspace = true }
anyhow = { workspace = true }

[dev-dependencies]
proptest = "1.5"

[[bin]]
name = "sandbawks-controller"
path = "src/bin/sandbawks-controller.rs"
```

- [ ] **Step 5: Create `crates/sandbawks-controller/src/lib.rs`**

```rust
//! Reconcile loop, state machine, and runtime-adapter wiring for the
//! sandbawks Kubernetes sandbox controller.
//!
//! The crate is intentionally layered so the state machine module is
//! testable without any kube or tokio dependency:
//!
//! - [`state_machine`] — pure transitions + actions (no I/O).
//! - [`store`] — trait abstraction over `kube::Api<Sandbox>` so the
//!   reconcile loop is unit-testable against an in-memory fake.
//! - [`fake_adapter`] — a no-op `RuntimeAdapter` impl that records
//!   calls. Used until Plan 05 lands `GvisorAdapter`.
//! - [`reconcile`] — glue that reads spec/status, derives a
//!   [`state_machine::Trigger`], runs the transition, patches status
//!   *before* any side effect, and dispatches the resulting action.

/// Crate name string — smoke test that downstream tests link.
pub const CRATE_NAME: &str = "sandbawks-controller";

pub mod fake_adapter;
pub mod reconcile;
pub mod state_machine;
pub mod store;

pub use fake_adapter::{FakeAdapterCall, FakeRuntimeAdapter};
pub use reconcile::{reconcile, ControllerError, ReconcileContext};
pub use state_machine::{transition, Action, Trigger};
pub use store::{FakeSandboxStore, KubeSandboxStore, SandboxStore, StoreError};
```

The `pub use` lines reference modules and items that don't exist yet — that's deliberate. The next tasks land each piece in turn. The crate will not compile after Task 1; Task 2 onward fixes it incrementally, mirroring the Plan 01 approach.

- [ ] **Step 6: Commit (compilation deliberately broken — recorded as such)**

```bash
git add Cargo.toml crates/sandbawks-controller/Cargo.toml crates/sandbawks-controller/src/lib.rs crates/sandbawks-controller/tests/smoke.rs
git commit -m "$(cat <<'EOF'
feat(controller): skeleton sandbawks-controller crate

Adds the workspace member with module declarations and three new
workspace deps (async-trait, tracing-subscriber, futures). Module
bodies land in subsequent tasks; the crate intentionally does not
compile yet.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: `RuntimeAdapter` trait in `sandbawks-types::runtime`

**Files:**
- Create: `crates/sandbawks-types/src/runtime.rs`
- Modify: `crates/sandbawks-types/src/lib.rs`
- Modify: `crates/sandbawks-types/Cargo.toml`
- Create: `crates/sandbawks-types/tests/runtime_trait.rs`

We put the trait in `sandbawks-types` (not in the controller crate) because both `sandbawks-controller` and the future `sandbawks-node` are *callers* of the trait. Putting it in `types` keeps the dependency graph one-directional: types ← controller, types ← node, with no controller ↔ node edge.

- [ ] **Step 1: Add `async-trait` to the types crate**

Edit `crates/sandbawks-types/Cargo.toml` to add `async-trait`:

```toml
[dependencies]
kube = { workspace = true }
k8s-openapi = { workspace = true }
schemars = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
serde_yaml = { workspace = true }
thiserror = { workspace = true }
async-trait = { workspace = true }
```

- [ ] **Step 2: Write the failing test**

Create `crates/sandbawks-types/tests/runtime_trait.rs`:

```rust
use std::sync::Arc;

use async_trait::async_trait;
use sandbawks_types::runtime::{RuntimeAdapter, RuntimeError, SnapshotManifest};

struct NullAdapter;

#[async_trait]
impl RuntimeAdapter for NullAdapter {
    async fn pause(&self, _pod_uid: &str) -> Result<SnapshotManifest, RuntimeError> {
        Ok(SnapshotManifest {
            sandbox_id: "test".into(),
            runtime_class: "null".into(),
            sha256: "0".repeat(64),
            created_at: "1970-01-01T00:00:00Z".into(),
        })
    }

    async fn resume(
        &self,
        _manifest: &SnapshotManifest,
        _pod_uid: &str,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn runtime_class(&self) -> &str {
        "null"
    }
}

#[tokio::test(flavor = "current_thread")]
async fn trait_is_dyn_object_safe() {
    let adapter: Arc<dyn RuntimeAdapter> = Arc::new(NullAdapter);
    assert_eq!(adapter.runtime_class(), "null");
    let manifest = adapter.pause("pod-1").await.unwrap();
    assert_eq!(manifest.sandbox_id, "test");
    adapter.resume(&manifest, "pod-1").await.unwrap();
}

#[test]
fn snapshot_manifest_serde_roundtrip() {
    let m = SnapshotManifest {
        sandbox_id: "sbx-abc123".into(),
        runtime_class: "gvisor".into(),
        sha256: format!("{:0>64}", "deadbeef"),
        created_at: "2026-05-22T18:00:00Z".into(),
    };
    let json = serde_json::to_string(&m).unwrap();
    let back: SnapshotManifest = serde_json::from_str(&json).unwrap();
    assert_eq!(m, back);
}
```

This test pulls `tokio` into the types crate dev-deps. Add it:

Append to `crates/sandbawks-types/Cargo.toml`:

```toml
[dev-dependencies]
tokio = { workspace = true }
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p sandbawks-types --test runtime_trait 2>&1 | tail -10`
Expected: compilation error — `sandbawks_types::runtime` module not found.

- [ ] **Step 4: Create `crates/sandbawks-types/src/runtime.rs`**

```rust
//! The `RuntimeAdapter` trait — the contract between
//! `sandbawks-controller` (caller) and `sandbawks-node` (caller, via
//! per-runtime impls landed in plans 05/08/09).
//!
//! The trait is intentionally minimal in this plan. Plan 05 adds the
//! `SnapshotSink` parameter and richer error variants once the gVisor
//! adapter exercises them; here we ship the smallest surface the
//! controller's state machine needs to call into.

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Metadata describing one snapshot of a sandbox's filesystem + memory.
///
/// The fields here are the *minimum* set the controller needs to record
/// in `Sandbox.status.snapshotRef`. Plan 05 extends this with chunk
/// indexing, compression, and per-file manifests; for now we keep it
/// JSON-serializable so it can round-trip through `serde_json::Value`
/// in the kube status field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotManifest {
    /// The Sandbox the snapshot belongs to (the CR `metadata.name`).
    pub sandbox_id: String,
    /// Runtime that produced the snapshot (`gvisor`, `kata-fc`, `runc-criu`).
    pub runtime_class: String,
    /// SHA-256 of the snapshot blob (or aggregate of chunks).
    pub sha256: String,
    /// RFC 3339 timestamp.
    pub created_at: String,
}

/// Errors a `RuntimeAdapter` impl can raise.
///
/// Variants are intentionally coarse for Plan 02. Plan 05 splits these
/// further (per-chunk upload failures, CRI socket errors, etc.) once
/// we have a real impl exercising them.
#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("pause failed: {0}")]
    Pause(String),
    #[error("resume failed: {0}")]
    Resume(String),
    /// Transient failure — caller should retry with backoff.
    #[error("transient: {0}")]
    Transient(String),
}

/// The contract a runtime backend must satisfy for the controller to
/// pause and resume sandboxes.
///
/// Both methods are required to be **idempotent**: the controller may
/// re-invoke them after a crash or requeue. Implementations must detect
/// existing snapshots by manifest content-hash and short-circuit.
#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    /// Quiesce the sandbox running in `pod_uid`, capture its state, and
    /// return a manifest the caller can record in `Sandbox.status`.
    async fn pause(&self, pod_uid: &str) -> Result<SnapshotManifest, RuntimeError>;

    /// Restore a sandbox from `manifest` into `pod_uid`. The pod is
    /// expected to already exist in a `Paused`-ready slot; provisioning
    /// the pod itself is the controller's responsibility.
    async fn resume(
        &self,
        manifest: &SnapshotManifest,
        pod_uid: &str,
    ) -> Result<(), RuntimeError>;

    /// The RuntimeClass name this adapter binds to (`gvisor`, `kata-fc`,
    /// `runc-criu`). Used by the controller to match Sandboxes to the
    /// right adapter at dispatch time.
    fn runtime_class(&self) -> &str;
}
```

- [ ] **Step 5: Re-export the runtime module from `lib.rs`**

Edit `crates/sandbawks-types/src/lib.rs` to add the module declaration and re-exports:

```rust
//! Shared types for the sandbawks Kubernetes sandbox controller.
//!
//! This crate is intentionally schema-only: it defines the CRDs and DTOs
//! that every other sandbawks crate consumes, but contains no runtime
//! behavior. Keeping it leaf-of-the-dependency-graph means everything
//! else can be tested against it cheaply.

/// Crate name string — used as a smoke test that downstream crates linked
/// against this one successfully.
pub const CRATE_NAME: &str = "sandbawks-types";

pub mod error;
pub mod resources;
pub mod runtime;
pub mod sandbox;
pub mod sandbox_template;
pub mod team_api_key;
pub mod warm_pool;

pub use error::Error;
pub use resources::{ResourceQuantity, ResourceRequests};
pub use runtime::{RuntimeAdapter, RuntimeError, SnapshotManifest};
pub use sandbox::{
    NetworkPolicy, Sandbox, SandboxCondition, SandboxPhase, SandboxSpec, SandboxStatus,
};
pub use sandbox_template::{SandboxTemplate, SandboxTemplateSpec};
pub use team_api_key::{TeamAPIKey, TeamApiKeySpec};
pub use warm_pool::{SandboxWarmPool, SandboxWarmPoolSpec, SandboxWarmPoolStatus};
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cargo test -p sandbawks-types --test runtime_trait 2>&1 | tail -20`
Expected: 2 tests pass.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml crates/sandbawks-types/Cargo.toml crates/sandbawks-types/src/lib.rs crates/sandbawks-types/src/runtime.rs crates/sandbawks-types/tests/runtime_trait.rs
git commit -m "$(cat <<'EOF'
feat(types): RuntimeAdapter trait + SnapshotManifest

Defines the controller <-> node contract in the types crate (the
shared leaf of the dependency graph). Minimal surface: pause / resume
/ runtime_class. SnapshotManifest is the kube status payload. Variants
will be extended in plan 05 when the first real adapter lands.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: `FakeRuntimeAdapter`

**Files:**
- Create: `crates/sandbawks-controller/src/fake_adapter.rs`
- Create: `crates/sandbawks-controller/tests/fake_adapter.rs`

- [ ] **Step 1: Write the failing tests**

Create `crates/sandbawks-controller/tests/fake_adapter.rs`:

```rust
use std::sync::Arc;

use sandbawks_controller::{FakeAdapterCall, FakeRuntimeAdapter};
use sandbawks_types::RuntimeAdapter;

#[tokio::test(flavor = "current_thread")]
async fn pause_records_call_and_returns_manifest() {
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let manifest = adapter.pause("pod-abc").await.unwrap();
    assert_eq!(manifest.runtime_class, "fake");
    assert_eq!(manifest.sandbox_id, "pod-abc");
    let calls = adapter.calls();
    assert_eq!(calls.len(), 1);
    assert!(matches!(&calls[0], FakeAdapterCall::Pause { pod_uid } if pod_uid == "pod-abc"));
}

#[tokio::test(flavor = "current_thread")]
async fn resume_records_call() {
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let manifest = adapter.pause("pod-abc").await.unwrap();
    adapter.resume(&manifest, "pod-abc").await.unwrap();
    let calls = adapter.calls();
    assert_eq!(calls.len(), 2);
    assert!(matches!(&calls[1], FakeAdapterCall::Resume { pod_uid, .. } if pod_uid == "pod-abc"));
}

#[tokio::test(flavor = "current_thread")]
async fn pause_can_be_configured_to_fail() {
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake").with_pause_failure("disk full"));
    let err = adapter.pause("pod-abc").await.unwrap_err();
    assert!(err.to_string().contains("disk full"));
}

#[test]
fn runtime_class_matches_constructor() {
    let a = FakeRuntimeAdapter::new("kata-fc");
    assert_eq!(a.runtime_class(), "kata-fc");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p sandbawks-controller --test fake_adapter 2>&1 | tail -10`
Expected: compilation error — `FakeRuntimeAdapter` unresolved.

- [ ] **Step 3: Create `crates/sandbawks-controller/src/fake_adapter.rs`**

```rust
//! A no-op `RuntimeAdapter` impl that records every call.
//!
//! Used until Plan 05 lands `GvisorAdapter`. The recording behavior
//! lets tests assert *that* the controller dispatched the right action
//! (pause vs resume vs nothing) without needing a real runtime.
//!
//! Failure injection (`with_pause_failure`, `with_resume_failure`)
//! lets us exercise the controller's error-handling paths without a
//! flaky real runtime.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sandbawks_types::{RuntimeAdapter, RuntimeError, SnapshotManifest};

/// One recorded call into the adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FakeAdapterCall {
    Pause { pod_uid: String },
    Resume { pod_uid: String, manifest: SnapshotManifest },
}

/// Records calls in an `Arc<Mutex<Vec<...>>>` so tests can assert on
/// them after the fact.
pub struct FakeRuntimeAdapter {
    runtime_class: String,
    calls: Arc<Mutex<Vec<FakeAdapterCall>>>,
    pause_failure: Option<String>,
    resume_failure: Option<String>,
}

impl FakeRuntimeAdapter {
    /// Construct a new adapter bound to a synthetic runtime class.
    pub fn new(runtime_class: impl Into<String>) -> Self {
        Self {
            runtime_class: runtime_class.into(),
            calls: Arc::new(Mutex::new(Vec::new())),
            pause_failure: None,
            resume_failure: None,
        }
    }

    /// Configure pause() to return a `RuntimeError::Pause(msg)`.
    pub fn with_pause_failure(mut self, msg: impl Into<String>) -> Self {
        self.pause_failure = Some(msg.into());
        self
    }

    /// Configure resume() to return a `RuntimeError::Resume(msg)`.
    pub fn with_resume_failure(mut self, msg: impl Into<String>) -> Self {
        self.resume_failure = Some(msg.into());
        self
    }

    /// Snapshot the recorded calls.
    pub fn calls(&self) -> Vec<FakeAdapterCall> {
        self.calls.lock().expect("poisoned").clone()
    }
}

#[async_trait]
impl RuntimeAdapter for FakeRuntimeAdapter {
    async fn pause(&self, pod_uid: &str) -> Result<SnapshotManifest, RuntimeError> {
        self.calls
            .lock()
            .expect("poisoned")
            .push(FakeAdapterCall::Pause {
                pod_uid: pod_uid.to_owned(),
            });
        if let Some(msg) = &self.pause_failure {
            return Err(RuntimeError::Pause(msg.clone()));
        }
        Ok(SnapshotManifest {
            sandbox_id: pod_uid.to_owned(),
            runtime_class: self.runtime_class.clone(),
            sha256: format!("{:0>64}", "fa1e"),
            created_at: "1970-01-01T00:00:00Z".into(),
        })
    }

    async fn resume(
        &self,
        manifest: &SnapshotManifest,
        pod_uid: &str,
    ) -> Result<(), RuntimeError> {
        self.calls
            .lock()
            .expect("poisoned")
            .push(FakeAdapterCall::Resume {
                pod_uid: pod_uid.to_owned(),
                manifest: manifest.clone(),
            });
        if let Some(msg) = &self.resume_failure {
            return Err(RuntimeError::Resume(msg.clone()));
        }
        Ok(())
    }

    fn runtime_class(&self) -> &str {
        &self.runtime_class
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p sandbawks-controller --test fake_adapter 2>&1 | tail -10`
Expected: 4 tests pass. (The smoke test from Task 1 still fails until later tasks land the missing modules — that's expected.)

- [ ] **Step 5: Commit**

```bash
git add crates/sandbawks-controller/src/fake_adapter.rs crates/sandbawks-controller/tests/fake_adapter.rs
git commit -m "$(cat <<'EOF'
feat(controller): FakeRuntimeAdapter for tests

Records every pause/resume call into an Arc<Mutex<Vec<_>>> so tests
can assert on dispatch. with_pause_failure / with_resume_failure let
us exercise error paths deterministically.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: State machine — Trigger, Action, transition()

**Files:**
- Create: `crates/sandbawks-controller/src/state_machine.rs`
- Create: `crates/sandbawks-controller/tests/state_machine_unit.rs`

The state machine is a pure function with no kube or async dependency. It maps `(current_phase, trigger) -> Option<(next_phase, action)>`. Illegal transitions return `None`; the reconcile loop treats `None` as "no change, requeue."

- [ ] **Step 1: Write the failing unit tests**

Create `crates/sandbawks-controller/tests/state_machine_unit.rs`:

```rust
use sandbawks_controller::{transition, Action, Trigger};
use sandbawks_types::SandboxPhase;

#[test]
fn creating_pod_ready_to_running() {
    let (phase, action) = transition(SandboxPhase::Creating, Trigger::PodReady).unwrap();
    assert_eq!(phase, SandboxPhase::Running);
    assert_eq!(action, Action::Noop);
}

#[test]
fn running_pause_requested_to_pausing_pause_action() {
    let (phase, action) =
        transition(SandboxPhase::Running, Trigger::PauseRequested).unwrap();
    assert_eq!(phase, SandboxPhase::Pausing);
    assert_eq!(action, Action::Pause);
}

#[test]
fn running_timeout_autopause_true_pauses() {
    let (phase, action) =
        transition(SandboxPhase::Running, Trigger::Timeout { auto_pause: true }).unwrap();
    assert_eq!(phase, SandboxPhase::Pausing);
    assert_eq!(action, Action::Pause);
}

#[test]
fn running_timeout_autopause_false_terminates() {
    let (phase, action) =
        transition(SandboxPhase::Running, Trigger::Timeout { auto_pause: false }).unwrap();
    assert_eq!(phase, SandboxPhase::Terminating);
    assert_eq!(action, Action::Terminate);
}

#[test]
fn running_kill_requested_terminates() {
    let (phase, action) =
        transition(SandboxPhase::Running, Trigger::KillRequested).unwrap();
    assert_eq!(phase, SandboxPhase::Terminating);
    assert_eq!(action, Action::Terminate);
}

#[test]
fn pausing_snapshot_uploaded_to_paused() {
    let (phase, action) =
        transition(SandboxPhase::Pausing, Trigger::SnapshotUploaded).unwrap();
    assert_eq!(phase, SandboxPhase::Paused);
    assert_eq!(action, Action::Noop);
}

#[test]
fn pausing_failure_to_failed() {
    let (phase, action) =
        transition(SandboxPhase::Pausing, Trigger::Failure("oom".into())).unwrap();
    assert_eq!(phase, SandboxPhase::Failed);
    assert_eq!(action, Action::RecordFailure("oom".into()));
}

#[test]
fn paused_resume_requested_to_resuming() {
    let (phase, action) =
        transition(SandboxPhase::Paused, Trigger::ResumeRequested).unwrap();
    assert_eq!(phase, SandboxPhase::Resuming);
    assert_eq!(action, Action::Resume);
}

#[test]
fn paused_kill_requested_terminates() {
    let (phase, action) =
        transition(SandboxPhase::Paused, Trigger::KillRequested).unwrap();
    assert_eq!(phase, SandboxPhase::Terminating);
    assert_eq!(action, Action::Terminate);
}

#[test]
fn resuming_ready_to_running() {
    let (phase, action) =
        transition(SandboxPhase::Resuming, Trigger::ResumeReady).unwrap();
    assert_eq!(phase, SandboxPhase::Running);
    assert_eq!(action, Action::Noop);
}

#[test]
fn resuming_failure_to_failed() {
    let (phase, action) =
        transition(SandboxPhase::Resuming, Trigger::Failure("io".into())).unwrap();
    assert_eq!(phase, SandboxPhase::Failed);
    assert_eq!(action, Action::RecordFailure("io".into()));
}

#[test]
fn creating_failure_to_failed() {
    let (phase, action) =
        transition(SandboxPhase::Creating, Trigger::Failure("imgpull".into())).unwrap();
    assert_eq!(phase, SandboxPhase::Failed);
    assert_eq!(action, Action::RecordFailure("imgpull".into()));
}

#[test]
fn failed_is_absorbing() {
    assert!(transition(SandboxPhase::Failed, Trigger::PodReady).is_none());
    assert!(transition(SandboxPhase::Failed, Trigger::PauseRequested).is_none());
    assert!(transition(SandboxPhase::Failed, Trigger::ResumeRequested).is_none());
    assert!(transition(SandboxPhase::Failed, Trigger::KillRequested).is_none());
    assert!(transition(SandboxPhase::Failed, Trigger::Failure("x".into())).is_none());
}

#[test]
fn terminating_is_absorbing() {
    assert!(transition(SandboxPhase::Terminating, Trigger::PauseRequested).is_none());
    assert!(transition(SandboxPhase::Terminating, Trigger::ResumeRequested).is_none());
    assert!(transition(SandboxPhase::Terminating, Trigger::PodReady).is_none());
}

#[test]
fn pause_requested_in_pausing_is_idempotent_noop() {
    // Requesting pause while already pausing returns None — the caller
    // (reconcile) interprets None as "no-op, the in-flight transition
    // will complete on its own."
    assert!(transition(SandboxPhase::Pausing, Trigger::PauseRequested).is_none());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p sandbawks-controller --test state_machine_unit 2>&1 | tail -10`
Expected: compilation error — `transition`, `Trigger`, `Action` unresolved.

- [ ] **Step 3: Create `crates/sandbawks-controller/src/state_machine.rs`**

```rust
//! Pure state machine for the `Sandbox` lifecycle.
//!
//! This module has no `kube`, `tokio`, or I/O dependency. It exists so
//! the state machine can be exhaustively unit- and property-tested
//! cheaply; the reconcile loop on top of it does the kube work.
//!
//! Transition diagram in spec §5. Every legal `(phase, trigger)` pair
//! maps to exactly one `(next_phase, action)`. Illegal pairs return
//! `None`, which the reconcile loop interprets as "no change."

use sandbawks_types::SandboxPhase;

/// Inputs to the state machine.
///
/// Triggers are derived by the reconcile loop from the observed
/// Sandbox spec/status/annotations + adapter call results. They are
/// the *atomic* events the state machine reacts to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Trigger {
    /// The backing pod reached `Ready=true`. (Plan 03 wires pod
    /// readiness via a watcher; Plan 02 simulates it in tests by
    /// setting an annotation the reconcile loop reads.)
    PodReady,
    /// User wants the sandbox paused (`spec.desiredPhase=Paused`
    /// annotation, or `POST /sandboxes/{id}/pause`).
    PauseRequested,
    /// User wants the sandbox resumed.
    ResumeRequested,
    /// User wants the sandbox killed (deletion timestamp set, or
    /// `POST /sandboxes/{id}/kill`).
    KillRequested,
    /// The sandbox's `spec.timeoutSeconds` elapsed.
    Timeout {
        /// Mirrors `spec.autoPause`. True → pause; false → terminate.
        auto_pause: bool,
    },
    /// The `RuntimeAdapter::pause` call returned Ok.
    SnapshotUploaded,
    /// The `RuntimeAdapter::resume` call returned Ok.
    ResumeReady,
    /// Any unrecoverable error during pod creation, pause, or resume.
    Failure(String),
}

/// What the reconcile loop should do as a result of entering the new
/// phase. The `Action` is associated with the *destination* phase, not
/// the trigger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// No side effect — the destination phase is observable on its own
    /// (e.g. `Running`, `Paused`).
    Noop,
    /// Call `RuntimeAdapter::pause` for this sandbox.
    Pause,
    /// Call `RuntimeAdapter::resume` for this sandbox.
    Resume,
    /// Tear down the pod and (optionally) clean up the snapshot.
    Terminate,
    /// Record an unrecoverable failure in `status.conditions`.
    RecordFailure(String),
}

/// Transition the state machine.
///
/// Returns `Some((next_phase, action))` if `(current, trigger)` is a
/// legal pair from spec §5; `None` otherwise. The caller (reconcile)
/// treats `None` as "this trigger does not apply to the current phase,
/// no change required."
pub fn transition(
    current: SandboxPhase,
    trigger: Trigger,
) -> Option<(SandboxPhase, Action)> {
    use SandboxPhase::*;
    use Trigger::*;

    match (current, trigger) {
        // Creating ────────────────────────────────────────────────
        (Creating, PodReady) => Some((Running, Action::Noop)),
        (Creating, Failure(msg)) => Some((Failed, Action::RecordFailure(msg))),

        // Running ─────────────────────────────────────────────────
        (Running, PauseRequested) => Some((Pausing, Action::Pause)),
        (Running, KillRequested) => Some((Terminating, Action::Terminate)),
        (Running, Timeout { auto_pause: true }) => Some((Pausing, Action::Pause)),
        (Running, Timeout { auto_pause: false }) => Some((Terminating, Action::Terminate)),
        (Running, Failure(msg)) => Some((Failed, Action::RecordFailure(msg))),

        // Pausing ─────────────────────────────────────────────────
        (Pausing, SnapshotUploaded) => Some((Paused, Action::Noop)),
        (Pausing, Failure(msg)) => Some((Failed, Action::RecordFailure(msg))),

        // Paused ──────────────────────────────────────────────────
        (Paused, ResumeRequested) => Some((Resuming, Action::Resume)),
        (Paused, KillRequested) => Some((Terminating, Action::Terminate)),

        // Resuming ────────────────────────────────────────────────
        (Resuming, ResumeReady) => Some((Running, Action::Noop)),
        (Resuming, Failure(msg)) => Some((Failed, Action::RecordFailure(msg))),

        // Terminal sinks ──────────────────────────────────────────
        (Failed, _) => None,
        (Terminating, _) => None,

        // Anything else — no transition.
        _ => None,
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p sandbawks-controller --test state_machine_unit 2>&1 | tail -20`
Expected: 15 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/sandbawks-controller/src/state_machine.rs crates/sandbawks-controller/tests/state_machine_unit.rs
git commit -m "$(cat <<'EOF'
feat(controller): pure Sandbox state machine

Trigger / Action enums + transition() function covering every legal
edge in spec §5. Zero kube or tokio deps so the module can be
exhaustively unit-tested. Illegal pairs return None; Failed and
Terminating are absorbing sinks.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Property tests for state machine

**Files:**
- Create: `crates/sandbawks-controller/tests/state_machine_properties.rs`

Property tests catch invariants the per-edge unit tests don't: totality, determinism, absorbing-state stickiness, and reachability constraints.

- [ ] **Step 1: Write the property tests**

Create `crates/sandbawks-controller/tests/state_machine_properties.rs`:

```rust
//! Property tests for the Sandbox state machine.
//!
//! These complement the per-edge unit tests in `state_machine_unit.rs`
//! by checking invariants over the *whole* trigger/phase space.

use proptest::prelude::*;
use sandbawks_controller::{transition, Trigger};
use sandbawks_types::SandboxPhase;

fn any_phase() -> impl Strategy<Value = SandboxPhase> {
    prop_oneof![
        Just(SandboxPhase::Creating),
        Just(SandboxPhase::Running),
        Just(SandboxPhase::Pausing),
        Just(SandboxPhase::Paused),
        Just(SandboxPhase::Resuming),
        Just(SandboxPhase::Terminating),
        Just(SandboxPhase::Failed),
    ]
}

fn any_trigger() -> impl Strategy<Value = Trigger> {
    prop_oneof![
        Just(Trigger::PodReady),
        Just(Trigger::PauseRequested),
        Just(Trigger::ResumeRequested),
        Just(Trigger::KillRequested),
        any::<bool>().prop_map(|auto_pause| Trigger::Timeout { auto_pause }),
        Just(Trigger::SnapshotUploaded),
        Just(Trigger::ResumeReady),
        "[a-z]{1,16}".prop_map(Trigger::Failure),
    ]
}

proptest! {
    /// transition() never panics for any (phase, trigger) pair.
    #[test]
    fn transition_is_total(phase in any_phase(), trigger in any_trigger()) {
        let _ = transition(phase, trigger);
    }

    /// transition() is deterministic — calling it twice with the same
    /// inputs returns the same output.
    #[test]
    fn transition_is_deterministic(phase in any_phase(), trigger in any_trigger()) {
        let a = transition(phase, trigger.clone());
        let b = transition(phase, trigger);
        prop_assert_eq!(a, b);
    }

    /// Failed has no outbound transitions.
    #[test]
    fn failed_is_absorbing(trigger in any_trigger()) {
        prop_assert!(transition(SandboxPhase::Failed, trigger).is_none());
    }

    /// Terminating has no outbound transitions.
    #[test]
    fn terminating_is_absorbing(trigger in any_trigger()) {
        prop_assert!(transition(SandboxPhase::Terminating, trigger).is_none());
    }

    /// Once a transition produces Failed or Terminating, no further
    /// trigger can move out of it.
    #[test]
    fn sink_states_stay_sunk(
        phase in any_phase(),
        t1 in any_trigger(),
        t2 in any_trigger(),
    ) {
        if let Some((next, _)) = transition(phase, t1) {
            if matches!(next, SandboxPhase::Failed | SandboxPhase::Terminating) {
                prop_assert!(transition(next, t2).is_none());
            }
        }
    }

    /// A successful transition never lands back on the starting phase
    /// (no self-loops in the spec §5 graph).
    #[test]
    fn no_self_loops(phase in any_phase(), trigger in any_trigger()) {
        if let Some((next, _)) = transition(phase, trigger) {
            prop_assert_ne!(next, phase);
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p sandbawks-controller --test state_machine_properties 2>&1 | tail -20`
Expected: 6 proptests pass with ~256 cases each.

- [ ] **Step 3: Commit**

```bash
git add crates/sandbawks-controller/tests/state_machine_properties.rs
git commit -m "$(cat <<'EOF'
test(controller): proptest invariants for state machine

Totality, determinism, absorbing-state stickiness, no-self-loops.
Complements per-edge unit tests by sweeping the whole (phase, trigger)
space.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: `SandboxStore` trait + `FakeSandboxStore`

**Files:**
- Create: `crates/sandbawks-controller/src/store.rs`

The reconcile loop needs to read Sandboxes and patch their status. Real production wires this to `kube::Api<Sandbox>`. For testing we want a fake. `SandboxStore` is the seam.

- [ ] **Step 1: Write the failing tests**

We'll put the store tests inline in `store.rs` (as `#[cfg(test)] mod tests`) since they're small and don't need cross-test imports.

- [ ] **Step 2: Create `crates/sandbawks-controller/src/store.rs`**

```rust
//! `SandboxStore` — the trait that abstracts over `kube::Api<Sandbox>`
//! so the reconcile loop is unit-testable against an in-memory fake.
//!
//! Production wires this to `KubeSandboxStore`, a thin wrapper over
//! `kube::Api<Sandbox>`. Tests use `FakeSandboxStore`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kube::api::{Patch, PatchParams};
use sandbawks_types::{Sandbox, SandboxStatus};
use thiserror::Error;

/// Errors raised by the store.
#[derive(Debug, Error)]
pub enum StoreError {
    /// Sandbox with the given name was not found in the store.
    #[error("sandbox not found: {0}")]
    NotFound(String),
    /// Underlying kube client error.
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),
    /// Underlying serialization error when constructing patches.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Read/write the Sandboxes the controller cares about.
///
/// Implementations are expected to namespace-scope by themselves — the
/// reconcile loop passes only the sandbox `name`.
#[async_trait]
pub trait SandboxStore: Send + Sync {
    /// Fetch a Sandbox by name. `NotFound` if the resource is gone.
    async fn get(&self, name: &str) -> Result<Sandbox, StoreError>;

    /// Patch only the `status` subresource of a Sandbox.
    async fn patch_status(
        &self,
        name: &str,
        new_status: SandboxStatus,
    ) -> Result<(), StoreError>;
}

/// `kube::Api<Sandbox>`-backed implementation for production.
pub struct KubeSandboxStore {
    api: kube::Api<Sandbox>,
}

impl KubeSandboxStore {
    /// Build a namespaced store.
    pub fn new(client: kube::Client, namespace: &str) -> Self {
        Self {
            api: kube::Api::namespaced(client, namespace),
        }
    }
}

#[async_trait]
impl SandboxStore for KubeSandboxStore {
    async fn get(&self, name: &str) -> Result<Sandbox, StoreError> {
        match self.api.get_opt(name).await? {
            Some(s) => Ok(s),
            None => Err(StoreError::NotFound(name.to_owned())),
        }
    }

    async fn patch_status(
        &self,
        name: &str,
        new_status: SandboxStatus,
    ) -> Result<(), StoreError> {
        let patch = serde_json::json!({ "status": new_status });
        self.api
            .patch_status(name, &PatchParams::apply("sandbawks-controller"), &Patch::Merge(&patch))
            .await?;
        Ok(())
    }
}

/// In-memory store for tests.
#[derive(Default, Clone)]
pub struct FakeSandboxStore {
    inner: Arc<Mutex<HashMap<String, Sandbox>>>,
}

impl FakeSandboxStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Pre-populate the store with a Sandbox.
    pub fn insert(&self, sandbox: Sandbox) {
        let name = sandbox
            .metadata
            .name
            .clone()
            .expect("test sandbox needs metadata.name");
        self.inner.lock().expect("poisoned").insert(name, sandbox);
    }

    /// Read the current state of a Sandbox by name. Useful in tests
    /// for asserting on the post-reconcile status.
    pub fn peek(&self, name: &str) -> Option<Sandbox> {
        self.inner.lock().expect("poisoned").get(name).cloned()
    }
}

#[async_trait]
impl SandboxStore for FakeSandboxStore {
    async fn get(&self, name: &str) -> Result<Sandbox, StoreError> {
        self.inner
            .lock()
            .expect("poisoned")
            .get(name)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(name.to_owned()))
    }

    async fn patch_status(
        &self,
        name: &str,
        new_status: SandboxStatus,
    ) -> Result<(), StoreError> {
        let mut guard = self.inner.lock().expect("poisoned");
        let sandbox = guard
            .get_mut(name)
            .ok_or_else(|| StoreError::NotFound(name.to_owned()))?;
        sandbox.status = Some(new_status);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sandbawks_types::{
        ResourceRequests, SandboxPhase, SandboxSpec, SandboxStatus,
    };

    fn sandbox(name: &str) -> Sandbox {
        Sandbox::new(
            name,
            SandboxSpec {
                template_ref: "tpl".into(),
                runtime_class_name: None,
                resources: ResourceRequests {
                    cpu: "1".parse().unwrap(),
                    memory: "1Gi".parse().unwrap(),
                    ephemeral_storage: None,
                },
                timeout_seconds: 60,
                auto_pause: false,
                env: Default::default(),
                metadata: Default::default(),
                network_policy: None,
            },
        )
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fake_get_returns_inserted_sandbox() {
        let store = FakeSandboxStore::new();
        store.insert(sandbox("sbx-1"));
        let s = store.get("sbx-1").await.unwrap();
        assert_eq!(s.metadata.name.as_deref(), Some("sbx-1"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fake_get_missing_returns_not_found() {
        let store = FakeSandboxStore::new();
        let err = store.get("nope").await.unwrap_err();
        assert!(matches!(err, StoreError::NotFound(_)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fake_patch_status_updates_in_place() {
        let store = FakeSandboxStore::new();
        store.insert(sandbox("sbx-1"));
        let mut new_status = SandboxStatus::default();
        new_status.phase = SandboxPhase::Running;
        store.patch_status("sbx-1", new_status).await.unwrap();
        let s = store.peek("sbx-1").unwrap();
        assert_eq!(s.status.unwrap().phase, SandboxPhase::Running);
    }
}
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test -p sandbawks-controller --lib store 2>&1 | tail -20`
Expected: 3 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/sandbawks-controller/src/store.rs
git commit -m "$(cat <<'EOF'
feat(controller): SandboxStore trait + Kube/Fake impls

Abstracts kube::Api<Sandbox> behind a trait so the reconcile loop is
unit-testable against FakeSandboxStore. KubeSandboxStore is the thin
production wrapper.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Reconcile function

**Files:**
- Create: `crates/sandbawks-controller/src/reconcile.rs`
- Create: `crates/sandbawks-controller/tests/reconcile.rs`

The reconcile function is where the state machine, the store, and the adapter meet. Two invariants from spec §10 it must honor:

1. **Status is durable before side effects.** Patch `status.phase = Pausing` to the kube API server *before* calling `adapter.pause()`. If the controller crashes between the patch and the adapter call, a new reconcile sees `status.phase=Pausing` and resumes.
2. **Idempotent on retry.** Re-reading the same Sandbox a second time and going through reconcile must not duplicate side effects. The state machine returning `None` for `(Pausing, PauseRequested)` is what makes this work.

The trigger is derived from the sandbox itself:

- `metadata.deletionTimestamp` set → `KillRequested`
- `metadata.annotations["sandbawks.io/desired-phase"]` (Plan 04 wires the API to set this; Plan 02 tests set it directly).
- Adapter result triggers are produced *inside* reconcile after the adapter call (synchronous in Plan 02).

- [ ] **Step 1: Write the failing tests**

Create `crates/sandbawks-controller/tests/reconcile.rs`:

```rust
use std::sync::Arc;

use sandbawks_controller::{
    reconcile, FakeRuntimeAdapter, FakeSandboxStore, ReconcileContext,
};
use sandbawks_types::{
    ResourceRequests, Sandbox, SandboxPhase, SandboxSpec, SandboxStatus,
};

fn base_sandbox(name: &str) -> Sandbox {
    Sandbox::new(
        name,
        SandboxSpec {
            template_ref: "tpl".into(),
            runtime_class_name: None,
            resources: ResourceRequests {
                cpu: "1".parse().unwrap(),
                memory: "1Gi".parse().unwrap(),
                ephemeral_storage: None,
            },
            timeout_seconds: 60,
            auto_pause: false,
            env: Default::default(),
            metadata: Default::default(),
            network_policy: None,
        },
    )
}

fn with_status(mut s: Sandbox, phase: SandboxPhase) -> Sandbox {
    let mut status = SandboxStatus::default();
    status.phase = phase;
    s.status = Some(status);
    s
}

fn with_annotation(mut s: Sandbox, k: &str, v: &str) -> Sandbox {
    let ann = s.metadata.annotations.get_or_insert_with(Default::default);
    ann.insert(k.to_owned(), v.to_owned());
    s
}

fn ctx(store: FakeSandboxStore, adapter: Arc<FakeRuntimeAdapter>) -> Arc<ReconcileContext> {
    Arc::new(ReconcileContext {
        store: Arc::new(store),
        adapter,
    })
}

#[tokio::test(flavor = "current_thread")]
async fn creating_with_pod_ready_annotation_transitions_to_running() {
    let store = FakeSandboxStore::new();
    let s = with_annotation(
        with_status(base_sandbox("sbx-1"), SandboxPhase::Creating),
        "sandbawks.io/pod-ready",
        "true",
    );
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    reconcile("sbx-1", ctx(store.clone(), adapter.clone()))
        .await
        .unwrap();
    assert_eq!(
        store.peek("sbx-1").unwrap().status.unwrap().phase,
        SandboxPhase::Running
    );
    // No adapter calls on a plain PodReady transition.
    assert!(adapter.calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn running_with_desired_paused_pauses_via_adapter() {
    let store = FakeSandboxStore::new();
    let s = with_annotation(
        with_status(base_sandbox("sbx-1"), SandboxPhase::Running),
        "sandbawks.io/desired-phase",
        "Paused",
    );
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    reconcile("sbx-1", ctx(store.clone(), adapter.clone()))
        .await
        .unwrap();
    let s = store.peek("sbx-1").unwrap();
    let status = s.status.unwrap();
    assert_eq!(status.phase, SandboxPhase::Paused);
    assert!(status.snapshot_ref.is_some(), "snapshot ref should be set");
    let calls = adapter.calls();
    assert_eq!(calls.len(), 1, "exactly one pause call");
}

#[tokio::test(flavor = "current_thread")]
async fn pause_adapter_failure_transitions_to_failed() {
    let store = FakeSandboxStore::new();
    let s = with_annotation(
        with_status(base_sandbox("sbx-1"), SandboxPhase::Running),
        "sandbawks.io/desired-phase",
        "Paused",
    );
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake").with_pause_failure("disk full"));
    reconcile("sbx-1", ctx(store.clone(), adapter.clone()))
        .await
        .unwrap();
    let status = store.peek("sbx-1").unwrap().status.unwrap();
    assert_eq!(status.phase, SandboxPhase::Failed);
    assert!(status
        .conditions
        .iter()
        .any(|c| c.message.as_deref() == Some("disk full")));
}

#[tokio::test(flavor = "current_thread")]
async fn paused_with_desired_running_resumes() {
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Paused);
    let mut status = s.status.take().unwrap();
    status.snapshot_ref = Some("s3://bucket/sbx-1/snap-1".into());
    s.status = Some(status);
    let s = with_annotation(s, "sandbawks.io/desired-phase", "Running");
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    reconcile("sbx-1", ctx(store.clone(), adapter.clone()))
        .await
        .unwrap();
    assert_eq!(
        store.peek("sbx-1").unwrap().status.unwrap().phase,
        SandboxPhase::Running
    );
    assert_eq!(adapter.calls().len(), 1, "exactly one resume call");
}

#[tokio::test(flavor = "current_thread")]
async fn deletion_timestamp_set_transitions_to_terminating() {
    let store = FakeSandboxStore::new();
    let mut s = with_status(base_sandbox("sbx-1"), SandboxPhase::Running);
    s.metadata.deletion_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
        chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
    ));
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    reconcile("sbx-1", ctx(store.clone(), adapter.clone()))
        .await
        .unwrap();
    assert_eq!(
        store.peek("sbx-1").unwrap().status.unwrap().phase,
        SandboxPhase::Terminating
    );
}

#[tokio::test(flavor = "current_thread")]
async fn reconcile_is_idempotent() {
    let store = FakeSandboxStore::new();
    let s = with_annotation(
        with_status(base_sandbox("sbx-1"), SandboxPhase::Running),
        "sandbawks.io/desired-phase",
        "Paused",
    );
    store.insert(s);
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    reconcile("sbx-1", ctx(store.clone(), adapter.clone()))
        .await
        .unwrap();
    reconcile("sbx-1", ctx(store.clone(), adapter.clone()))
        .await
        .unwrap();
    assert_eq!(adapter.calls().len(), 1, "second reconcile is a noop");
}
```

This test pulls `chrono` for a deletion timestamp. `k8s-openapi` re-exports `chrono` via `Time(DateTime<Utc>)`, so add `chrono` to the controller's dev-deps. Edit `crates/sandbawks-controller/Cargo.toml`:

```toml
[dev-dependencies]
proptest = "1.5"
chrono = "0.4"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p sandbawks-controller --test reconcile 2>&1 | tail -10`
Expected: compilation error — `reconcile`, `ReconcileContext` unresolved.

- [ ] **Step 3: Create `crates/sandbawks-controller/src/reconcile.rs`**

```rust
//! Reconcile loop entrypoint.
//!
//! Honors spec §10's two key invariants:
//!
//! 1. **Status is durable before side effects.** We patch
//!    `status.phase` first, then call into the adapter. A crash
//!    between the two leaves the Sandbox in the intermediate phase
//!    (`Pausing`, `Resuming`), and the next reconcile recovers.
//! 2. **Idempotent.** Re-reading the same Sandbox and re-reconciling
//!    must not duplicate adapter calls. The state machine returning
//!    `None` for in-flight phases (e.g. `(Pausing, PauseRequested)`)
//!    is how we get that.
//!
//! The trigger is derived from the Sandbox itself — annotations,
//! deletion timestamp, status.phase. Plan 04 will replace direct
//! annotation reads with reads of `spec.desiredPhase` once that field
//! lands.

use std::sync::Arc;

use sandbawks_types::{
    RuntimeAdapter, Sandbox, SandboxCondition, SandboxPhase, SandboxStatus,
};
use thiserror::Error;

use crate::state_machine::{transition, Action, Trigger};
use crate::store::{SandboxStore, StoreError};

/// Annotation key on the Sandbox carrying the user's desired phase.
///
/// Plan 02 reads this directly. Plan 04 will replace the annotation
/// with a typed `spec.desiredPhase` field once the API gateway lands.
pub const ANN_DESIRED_PHASE: &str = "sandbawks.io/desired-phase";

/// Annotation marking that the backing Pod has reached Ready=true.
/// Plan 03 wires this from a real pod watcher; Plan 02 tests set it.
pub const ANN_POD_READY: &str = "sandbawks.io/pod-ready";

/// Dependencies the reconcile loop needs.
pub struct ReconcileContext {
    pub store: Arc<dyn SandboxStore>,
    pub adapter: Arc<dyn RuntimeAdapter>,
}

/// Errors raised from the reconcile loop.
#[derive(Debug, Error)]
pub enum ControllerError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    /// The reconcile loop saw an inconsistent Sandbox (e.g. attempted
    /// to resume but `status.snapshotRef` was None). Bug if reached
    /// at runtime; tests assert against it explicitly.
    #[error("invariant violation: {0}")]
    Invariant(&'static str),
}

/// Reconcile a single Sandbox.
///
/// This is the entrypoint kube-rs Controller will call (wrapped in
/// the appropriate Action/Error envelope). Tests call it directly
/// against a `FakeSandboxStore` + `FakeRuntimeAdapter`.
pub async fn reconcile(
    name: &str,
    ctx: Arc<ReconcileContext>,
) -> Result<(), ControllerError> {
    let sandbox = ctx.store.get(name).await?;
    let current_phase = current_phase(&sandbox);
    let Some(trigger) = derive_trigger(&sandbox, current_phase) else {
        tracing::debug!(
            sandbox = %name,
            phase = ?current_phase,
            "no trigger; idle"
        );
        return Ok(());
    };

    let Some((next_phase, action)) = transition(current_phase, trigger.clone()) else {
        tracing::debug!(
            sandbox = %name,
            phase = ?current_phase,
            trigger = ?trigger,
            "no-op transition; idle"
        );
        return Ok(());
    };

    // INVARIANT #1: patch status FIRST, then dispatch the action.
    let mut new_status = sandbox.status.clone().unwrap_or_default();
    new_status.phase = next_phase;
    ctx.store.patch_status(name, new_status.clone()).await?;
    tracing::info!(
        sandbox = %name,
        from = ?current_phase,
        to = ?next_phase,
        "phase transition committed"
    );

    // Now run the action. Failures roll the Sandbox into Failed via
    // a second transition + patch.
    if let Err(action_err) = dispatch(&ctx, &sandbox, action.clone(), &mut new_status).await
    {
        tracing::warn!(
            sandbox = %name,
            error = %action_err,
            "action failed; transitioning to Failed"
        );
        if let Some((fail_phase, fail_action)) =
            transition(next_phase, Trigger::Failure(action_err.to_string()))
        {
            let mut final_status = new_status.clone();
            final_status.phase = fail_phase;
            if let Action::RecordFailure(msg) = fail_action {
                final_status.conditions.push(SandboxCondition {
                    kind: "Failed".into(),
                    status: "True".into(),
                    reason: Some("ActionFailure".into()),
                    message: Some(msg),
                    last_transition_time: None,
                });
            }
            ctx.store.patch_status(name, final_status).await?;
        }
    } else {
        ctx.store.patch_status(name, new_status).await?;
    }

    Ok(())
}

fn current_phase(sandbox: &Sandbox) -> SandboxPhase {
    sandbox
        .status
        .as_ref()
        .map(|s| s.phase)
        .unwrap_or(SandboxPhase::Creating)
}

fn derive_trigger(sandbox: &Sandbox, current: SandboxPhase) -> Option<Trigger> {
    // Deletion timestamp wins: the user issued a delete.
    if sandbox.metadata.deletion_timestamp.is_some() {
        return Some(Trigger::KillRequested);
    }

    let desired = sandbox
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(ANN_DESIRED_PHASE))
        .map(String::as_str);
    let pod_ready = sandbox
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(ANN_POD_READY))
        .map(String::as_str)
        == Some("true");

    match (current, desired, pod_ready) {
        (SandboxPhase::Creating, _, true) => Some(Trigger::PodReady),
        (SandboxPhase::Running, Some("Paused"), _) => Some(Trigger::PauseRequested),
        (SandboxPhase::Paused, Some("Running"), _) => Some(Trigger::ResumeRequested),
        _ => None,
    }
}

async fn dispatch(
    ctx: &ReconcileContext,
    sandbox: &Sandbox,
    action: Action,
    new_status: &mut SandboxStatus,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let name = sandbox
        .metadata
        .name
        .as_deref()
        .ok_or("sandbox missing metadata.name")?;
    match action {
        Action::Noop => Ok(()),
        Action::Pause => {
            let manifest = ctx.adapter.pause(name).await?;
            // Record the snapshot ref and finish the Pausing → Paused
            // transition.
            new_status.snapshot_ref =
                Some(format!("inline://manifest/{}", manifest.sha256));
            new_status.phase = SandboxPhase::Paused;
            Ok(())
        }
        Action::Resume => {
            let manifest_ref = new_status
                .snapshot_ref
                .clone()
                .ok_or("resume requested but status.snapshotRef is None")?;
            let manifest = sandbawks_types::SnapshotManifest {
                sandbox_id: name.to_owned(),
                runtime_class: ctx.adapter.runtime_class().to_owned(),
                sha256: manifest_ref.trim_start_matches("inline://manifest/").to_owned(),
                created_at: "1970-01-01T00:00:00Z".into(),
            };
            ctx.adapter.resume(&manifest, name).await?;
            new_status.phase = SandboxPhase::Running;
            Ok(())
        }
        Action::Terminate => {
            // Plan 02: no real pod to delete (Plan 03 lands pod
            // creation). The Sandbox CR itself is GC'd via owner-ref
            // or finalizer in a later plan; here we just leave the
            // phase at Terminating.
            Ok(())
        }
        Action::RecordFailure(msg) => {
            new_status.conditions.push(SandboxCondition {
                kind: "Failed".into(),
                status: "True".into(),
                reason: Some("ActionFailure".into()),
                message: Some(msg),
                last_transition_time: None,
            });
            Ok(())
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p sandbawks-controller --test reconcile 2>&1 | tail -20`
Expected: 6 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/sandbawks-controller/Cargo.toml crates/sandbawks-controller/src/reconcile.rs crates/sandbawks-controller/tests/reconcile.rs
git commit -m "$(cat <<'EOF'
feat(controller): reconcile loop bridging state machine + adapter

reconcile(name, ctx) reads a Sandbox, derives a Trigger from
spec/status/annotations + deletion timestamp, runs the state machine,
patches status BEFORE side effects (spec §10), then dispatches the
Action via the adapter. Failed adapter calls roll into the Failed
phase with a condition.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Full-lifecycle integration test

**Files:**
- Create: `crates/sandbawks-controller/tests/lifecycle.rs`

End-to-end: drive a Sandbox through Creating → Running → Pausing → Paused → Resuming → Running → Terminating against the fake adapter, asserting at every step.

- [ ] **Step 1: Write the test**

```rust
//! Full Sandbox lifecycle against fakes.
//!
//! Walks one Sandbox through every legal phase in spec §5 and asserts
//! both the phase observed at each step and the adapter calls dispatched.

use std::collections::BTreeMap;
use std::sync::Arc;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use sandbawks_controller::{
    reconcile, FakeAdapterCall, FakeRuntimeAdapter, FakeSandboxStore, ReconcileContext,
};
use sandbawks_types::{
    ResourceRequests, Sandbox, SandboxPhase, SandboxSpec, SandboxStatus,
};

fn fresh_sandbox() -> Sandbox {
    Sandbox::new(
        "sbx-lifecycle",
        SandboxSpec {
            template_ref: "tpl".into(),
            runtime_class_name: None,
            resources: ResourceRequests {
                cpu: "1".parse().unwrap(),
                memory: "1Gi".parse().unwrap(),
                ephemeral_storage: None,
            },
            timeout_seconds: 60,
            auto_pause: false,
            env: Default::default(),
            metadata: Default::default(),
            network_policy: None,
        },
    )
}

fn annotate(s: &mut Sandbox, k: &str, v: &str) {
    s.metadata
        .annotations
        .get_or_insert_with(BTreeMap::new)
        .insert(k.into(), v.into());
}

fn clear_annotation(s: &mut Sandbox, k: &str) {
    if let Some(ann) = s.metadata.annotations.as_mut() {
        ann.remove(k);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn full_lifecycle_creating_to_terminating() {
    let store = FakeSandboxStore::new();
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let ctx = Arc::new(ReconcileContext {
        store: Arc::new(store.clone()),
        adapter: adapter.clone(),
    });

    // Seed: Sandbox in Creating, no annotations.
    let mut s = fresh_sandbox();
    let mut status = SandboxStatus::default();
    status.phase = SandboxPhase::Creating;
    s.status = Some(status);
    store.insert(s);

    // 1. Creating + PodReady → Running
    {
        let mut s = store.peek("sbx-lifecycle").unwrap();
        annotate(&mut s, "sandbawks.io/pod-ready", "true");
        store.insert(s);
    }
    reconcile("sbx-lifecycle", ctx.clone()).await.unwrap();
    assert_eq!(
        store.peek("sbx-lifecycle").unwrap().status.unwrap().phase,
        SandboxPhase::Running
    );

    // 2. Running + desired=Paused → Pausing → Paused (one reconcile)
    {
        let mut s = store.peek("sbx-lifecycle").unwrap();
        annotate(&mut s, "sandbawks.io/desired-phase", "Paused");
        store.insert(s);
    }
    reconcile("sbx-lifecycle", ctx.clone()).await.unwrap();
    let s = store.peek("sbx-lifecycle").unwrap();
    assert_eq!(s.status.as_ref().unwrap().phase, SandboxPhase::Paused);
    assert!(s.status.as_ref().unwrap().snapshot_ref.is_some());
    assert_eq!(adapter.calls().len(), 1);
    assert!(matches!(&adapter.calls()[0], FakeAdapterCall::Pause { .. }));

    // 3. Paused + desired=Running → Resuming → Running
    {
        let mut s = store.peek("sbx-lifecycle").unwrap();
        annotate(&mut s, "sandbawks.io/desired-phase", "Running");
        store.insert(s);
    }
    reconcile("sbx-lifecycle", ctx.clone()).await.unwrap();
    let s = store.peek("sbx-lifecycle").unwrap();
    assert_eq!(s.status.as_ref().unwrap().phase, SandboxPhase::Running);
    assert_eq!(adapter.calls().len(), 2);
    assert!(matches!(&adapter.calls()[1], FakeAdapterCall::Resume { .. }));

    // 4. Running + deletionTimestamp set → Terminating
    {
        let mut s = store.peek("sbx-lifecycle").unwrap();
        clear_annotation(&mut s, "sandbawks.io/desired-phase");
        s.metadata.deletion_timestamp = Some(Time(
            chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        ));
        store.insert(s);
    }
    reconcile("sbx-lifecycle", ctx.clone()).await.unwrap();
    assert_eq!(
        store.peek("sbx-lifecycle").unwrap().status.unwrap().phase,
        SandboxPhase::Terminating
    );
}
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cargo test -p sandbawks-controller --test lifecycle 2>&1 | tail -20`
Expected: 1 test passes.

- [ ] **Step 3: Commit**

```bash
git add crates/sandbawks-controller/tests/lifecycle.rs
git commit -m "$(cat <<'EOF'
test(controller): full Sandbox lifecycle integration test

Walks Creating → Running → Paused → Running → Terminating against
fakes, asserting at each step.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: Controller binary

**Files:**
- Create: `crates/sandbawks-controller/src/bin/sandbawks-controller.rs`

The binary wires the in-cluster kube client to `KubeSandboxStore` + `FakeRuntimeAdapter` and runs the kube-rs `Controller`. Real adapter wiring is Plan 05.

- [ ] **Step 1: Create the binary**

```rust
//! sandbawks-controller binary entrypoint.
//!
//! Boots tracing, builds an in-cluster kube client, wires
//! `KubeSandboxStore` + `FakeRuntimeAdapter`, and runs the kube-rs
//! `Controller` over `Sandbox` CRs. Plan 05 swaps the fake adapter
//! out for the real `GvisorAdapter`.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use kube::{
    runtime::controller::{Action, Controller},
    Api, Client,
};
use sandbawks_controller::{
    reconcile, ControllerError, FakeRuntimeAdapter, KubeSandboxStore, ReconcileContext,
};
use sandbawks_types::Sandbox;
use tokio::signal;
use tracing_subscriber::{prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let client = Client::try_default()
        .await
        .context("building in-cluster kube client")?;
    let namespace = std::env::var("SANDBAWKS_NAMESPACE").unwrap_or_else(|_| "default".into());

    let sandboxes: Api<Sandbox> = Api::namespaced(client.clone(), &namespace);
    let store = Arc::new(KubeSandboxStore::new(client.clone(), &namespace));
    let adapter = Arc::new(FakeRuntimeAdapter::new("fake"));
    let ctx = Arc::new(ReconcileContext {
        store,
        adapter,
    });

    tracing::info!(namespace = %namespace, "starting sandbawks-controller");

    let controller = Controller::new(sandboxes, Default::default())
        .shutdown_on_signal()
        .run(reconcile_adapter, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(?obj, "reconciled"),
                Err(e) => tracing::warn!(error = %e, "reconcile error"),
            }
        });

    tokio::select! {
        _ = controller => {},
        _ = signal::ctrl_c() => tracing::info!("SIGINT received, shutting down"),
    }

    Ok(())
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();
}

/// Adapter between kube-rs `Controller`'s expected signature and our
/// `reconcile()`. kube-rs calls this with the observed Sandbox + the
/// shared context; we extract the name and delegate.
async fn reconcile_adapter(
    sandbox: Arc<Sandbox>,
    ctx: Arc<ReconcileContext>,
) -> Result<Action, ControllerError> {
    let name = sandbox
        .metadata
        .name
        .clone()
        .ok_or(ControllerError::Invariant("missing metadata.name"))?;
    reconcile(&name, ctx).await?;
    Ok(Action::requeue(Duration::from_secs(30)))
}

fn error_policy(
    _sandbox: Arc<Sandbox>,
    _err: &ControllerError,
    _ctx: Arc<ReconcileContext>,
) -> Action {
    Action::requeue(Duration::from_secs(5))
}
```

- [ ] **Step 2: Confirm the binary builds**

Run: `cargo build -p sandbawks-controller --bin sandbawks-controller 2>&1 | tail -10`
Expected: clean build.

- [ ] **Step 3: Commit**

```bash
git add crates/sandbawks-controller/src/bin/sandbawks-controller.rs
git commit -m "$(cat <<'EOF'
feat(controller): sandbawks-controller binary

Boots tracing, in-cluster kube client, KubeSandboxStore +
FakeRuntimeAdapter, runs kube-rs Controller with shutdown-on-signal.
Plan 05 swaps the fake adapter for the real GvisorAdapter.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Wire the controller into the Helm chart

**Files:**
- Modify: `deploy/helm/sandbawks/templates/controller-deployment.yaml`

The Plan 01 chart shipped the controller Deployment with `args: ["--placeholder"]`. Replace it with the real binary invocation + a readiness probe.

- [ ] **Step 1: Edit the template**

Replace the contents of `deploy/helm/sandbawks/templates/controller-deployment.yaml` with:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sandbawks-controller
  labels: {{- include "sandbawks.labels" . | nindent 4 }}
spec:
  replicas: {{ .Values.controller.replicas }}
  selector:
    matchLabels:
      {{- include "sandbawks.selectorLabels" . | nindent 6 }}
      app.kubernetes.io/component: controller
  template:
    metadata:
      labels:
        {{- include "sandbawks.selectorLabels" . | nindent 8 }}
        app.kubernetes.io/component: controller
    spec:
      serviceAccountName: sandbawks-controller
      containers:
        - name: controller
          image: "{{ .Values.images.controller.repository }}:{{ .Values.images.controller.tag }}"
          imagePullPolicy: {{ .Values.images.controller.pullPolicy }}
          env:
            - name: SANDBAWKS_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            - name: RUST_LOG
              value: {{ .Values.controller.logLevel | default "info" | quote }}
          resources:
            {{- toYaml .Values.controller.resources | nindent 12 }}
```

- [ ] **Step 2: Add `logLevel` to `values.yaml`**

Edit `deploy/helm/sandbawks/values.yaml`, in the `controller:` block, append:

```yaml
controller:
  replicas: 1
  logLevel: info
  resources:
    requests: { cpu: 100m, memory: 128Mi }
    limits:   { cpu: "1",  memory: 512Mi }
```

(Replace the whole `controller:` block — preserve other fields if any were added.)

- [ ] **Step 3: Lint + template the chart**

Run: `helm lint deploy/helm/sandbawks`
Expected: `1 chart(s) linted, 0 chart(s) failed`.

Run: `helm template test deploy/helm/sandbawks | grep -A4 'name: sandbawks-controller$' | head -20`
Expected: shows the Deployment with the `env:` block and no `args: --placeholder`.

- [ ] **Step 4: Commit**

```bash
git add deploy/helm/sandbawks/templates/controller-deployment.yaml deploy/helm/sandbawks/values.yaml
git commit -m "$(cat <<'EOF'
feat(deploy): wire real controller binary into Helm chart

Replaces the --placeholder args with SANDBAWKS_NAMESPACE + RUST_LOG
env, drops the placeholder arg. helm lint still passes.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 11: Workspace-wide green gate

**Files:** none new — just verifies everything still compiles together.

- [ ] **Step 1: Full workspace test run**

Run: `cargo test --workspace 2>&1 | tail -30`
Expected: all crates compile; controller adds ~30 new tests (smoke 1 + state_machine_unit 15 + state_machine_properties 6 + fake_adapter 4 + reconcile 6 + lifecycle 1 + store inline 3 = ~36). Adjust if numbers differ slightly; every test must pass.

- [ ] **Step 2: Clippy clean**

Run: `cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -30`
Expected: no warnings.

- [ ] **Step 3: Format check**

Run: `cargo fmt --all -- --check 2>&1 | tail -10`
Expected: no output.

If fmt fails, run `cargo fmt --all` and commit the result with `chore: rustfmt`.

- [ ] **Step 4: Doc build**

Run: `cargo doc --workspace --no-deps 2>&1 | tail -10`
Expected: docs built without warnings.

- [ ] **Step 5: CRD regen drift check (Plan 01 still holds — runtime trait additions don't touch CRDs)**

Run:
```bash
cargo run -p sandbawks-types --bin crdgen -- /tmp/crds-check
diff -ru deploy/crds /tmp/crds-check
```
Expected: empty diff.

If diff is non-empty, regenerate CRDs into `deploy/crds`, copy into `deploy/helm/sandbawks/crds/`, and commit as `chore(types): regenerate CRDs`.

---

## Task 12: Changelog entry

**Files:**
- Create: `changelog/2026-05-22-plan-02-controller-skeleton.md`

- [ ] **Step 1: Write the changelog**

```markdown
# 2026-05-22 — Plan 02: Controller skeleton + state machine

## What landed

- New crate `crates/sandbawks-controller`: state machine, reconcile loop, store abstraction, fake runtime adapter, binary entrypoint.
- `RuntimeAdapter` trait + `SnapshotManifest` + `RuntimeError` added to `sandbawks-types::runtime` (shared contract between controller and the future node crate).
- Pure state machine: `Trigger`, `Action`, `transition()` function covering every edge from spec §5; absorbing `Failed` and `Terminating` sinks.
- Property tests (`proptest`) for totality, determinism, absorbing-state stickiness, no self-loops.
- `SandboxStore` trait: `KubeSandboxStore` for production, `FakeSandboxStore` for tests.
- Reconcile loop honors spec §10's durability-before-side-effects invariant: status patched, then adapter dispatched.
- Full-lifecycle integration test against fakes: Creating → Running → Paused → Running → Terminating.
- Helm chart's controller Deployment now runs the real binary (env-driven config; no more `--placeholder`).

## What's deliberately not here

- Pod materialization — Plan 03 (envd-rs + reference template image) lands the in-sandbox agent; pod creation is part of plan 04 or 05 depending on how E2B API translation shakes out.
- Real `RuntimeAdapter` impls (gVisor, Kata-FC, runc+CRIU) — Plans 05, 08, 09.
- Pod-readiness watcher — Plan 03 wires it; Plan 02 simulates via the `sandbawks.io/pod-ready` annotation in tests.
- Finalizer logic for clean pod / snapshot tear-down — deferred to Plan 05.
- Leader election — single-replica deployment in the Helm chart is sufficient until multi-replica HA becomes a goal.

## Next plan

`docs/superpowers/plans/2026-05-22-sandbawks-03-envd-and-template-image.md` —
in-sandbox `envd-rs` agent (gRPC surface on port 49983) and a reference
template OCI image that downstream pause/resume tests will run against.
```

- [ ] **Step 2: Commit**

```bash
git add changelog/2026-05-22-plan-02-controller-skeleton.md
git commit -m "docs(changelog): plan 02 landed"
```

---

## Self-review checklist (run before declaring this plan done)

- [ ] `cargo test --workspace` is green (~52 tests: 16 from Plan 01 + ~36 from Plan 02).
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` is silent.
- [ ] `cargo fmt --all -- --check` is silent.
- [ ] `cargo doc --workspace --no-deps` builds cleanly.
- [ ] `cargo run -p sandbawks-types --bin crdgen -- deploy/crds` is idempotent (no diff after rerun).
- [ ] `helm lint deploy/helm/sandbawks` passes.
- [ ] `helm template test deploy/helm/sandbawks > /dev/null` succeeds.
- [ ] State machine has property tests, not just unit tests.
- [ ] Reconcile patches status *before* calling adapter (verified by reading `reconcile.rs:reconcile()`).
- [ ] `FakeRuntimeAdapter` is `Send + Sync` (verified by `Arc<dyn RuntimeAdapter>` in tests).
- [ ] `git log --oneline` shows ~12 small focused commits, each runnable.

## What this plan does NOT do (rejected scope)

- No HTTP / REST API handlers. The API gateway is Plan 04.
- No envd-rs gRPC agent. That's Plan 03.
- No real `RuntimeAdapter` impls. gVisor is Plan 05.
- No pod creation. The controller in Plan 02 only patches Sandbox status; pod materialization is later.
- No real snapshot storage (S3, hostPath). Plan 02 uses an `inline://manifest/<sha256>` placeholder URI in `status.snapshotRef`.
- No leader election. Single-replica controller is fine for MVP.
- No metrics endpoint. `tracing` events are sufficient until Plan 06's Prometheus wiring.
