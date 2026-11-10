//! Kata Containers runtime adapter (epic #594).
//!
//! Cold-start sandboxes run fine on Kata — the gVisor-only assumptions in the
//! scheduling, staging gate, and cold-start path were fixed in #593/#595/#596 —
//! but **pause/resume is not supported on Kata**, and this adapter fails closed.
//!
//! # Why fail closed (not a CH-direct snapshot)
//!
//! #595 shipped a `KataAdapter` that drove the sandbox's Cloud Hypervisor VMM
//! directly over its API socket (`vm.pause`/`vm.snapshot`/`vm.restore`). A live
//! viability trial on AKS (cloud-hypervisor v51 on MSHV) disproved that approach:
//!
//! - `vm.snapshot` *does* produce a valid bundle (`config.json` + `state.json` +
//!   `memory-ranges`) — the capture mechanism is real.
//! - **But driving it out-of-band — behind the `containerd-shim-kata-v2` that
//!   owns the VM lifecycle — wedged and destroyed the running VM**: the CH API
//!   went unresponsive, `vm.resume` failed, the `/run/vc/vm/<id>/` dir was torn
//!   down, and the sandbox folded to `Failed`.
//! - `vm.restore` is worse: it needs a fresh cloud-hypervisor process with no
//!   booted VM to restore into, but the kata shim always boots the VM itself —
//!   there is no restore-ready VMM to target without shim cooperation.
//!
//! So a Kata pause MUST fail closed (leaving the workload Pod running) rather
//! than attempt a snapshot that kills the sandbox. The CH-direct code from #595
//! was removed so a later "fix" of its socket resolution can't re-enable the
//! destructive path. A working implementation needs **shim-mediated** snapshot
//! (Kata's experimental `pause_with_snapshot`/`resume_from_snapshot` endpoints,
//! absent from stock AKS Kata) — tracked in #594.

use std::borrow::Cow;

use async_trait::async_trait;

use sandbawks_types::runtime::{RuntimeAdapter, RuntimeError, SnapshotManifest};
use sandbawks_types::snapshot::{SnapshotSink, SnapshotSource};

/// Default RuntimeClass name this adapter reports in
/// [`SnapshotManifest::runtime_class`] — the reserved `kata-fc` value.
pub const DEFAULT_KATA_RUNTIME_CLASS: &str = "kata-fc";

/// A fail-closed [`RuntimeAdapter`] for Kata nodes.
///
/// Cold start works (the controller's `PodAdapter` materializes the Pod), but
/// the snapshot operations the node drives — `pause` / `resume` — return
/// [`RuntimeError::Unsupported`] so a Kata sandbox is never destroyed by an
/// out-of-band snapshot attempt (see the module docs and #594). The controller
/// treats a failed pause as fail-closed and leaves the workload Pod running.
pub struct KataAdapter {
    runtime_class: String,
}

impl Default for KataAdapter {
    fn default() -> Self {
        Self {
            runtime_class: DEFAULT_KATA_RUNTIME_CLASS.to_owned(),
        }
    }
}

impl KataAdapter {
    /// Construct with the default `kata-fc` runtime class.
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the RuntimeClass name this adapter reports (defaults to
    /// [`DEFAULT_KATA_RUNTIME_CLASS`]).
    pub fn with_runtime_class(mut self, class: impl Into<String>) -> Self {
        self.runtime_class = class.into();
        self
    }
}

#[async_trait]
impl RuntimeAdapter for KataAdapter {
    fn runtime_class(&self) -> &str {
        &self.runtime_class
    }

    async fn pause(
        &self,
        pod_uid: &str,
        _sink: &dyn SnapshotSink,
    ) -> Result<SnapshotManifest, RuntimeError> {
        Err(RuntimeError::Unsupported(Cow::Owned(format!(
            "pause is unsupported on the kata runtime (sandbox {pod_uid}): an out-of-band \
             Cloud Hypervisor snapshot destroys the shim-managed VM; see #594"
        ))))
    }

    async fn resume(
        &self,
        _manifest: &SnapshotManifest,
        pod_uid: &str,
        _source: &dyn SnapshotSource,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(Cow::Owned(format!(
            "resume is unsupported on the kata runtime (sandbox {pod_uid}); see #594"
        ))))
    }

    // `stage_snapshot` and `validate_restorable` keep their trait defaults
    // (`Unsupported` / no-op): a Kata sandbox is never paused, so neither the
    // restore-staging nor the pause-validation path is ever reached.
}

#[cfg(test)]
mod tests {
    use super::*;

    use sandbawks_types::testing::FakeSnapshotStore;

    #[test]
    fn runtime_class_defaults_and_overrides() {
        assert_eq!(
            KataAdapter::new().runtime_class(),
            DEFAULT_KATA_RUNTIME_CLASS
        );
        assert_eq!(
            KataAdapter::new()
                .with_runtime_class("kata-vm-isolation")
                .runtime_class(),
            "kata-vm-isolation"
        );
    }

    #[tokio::test]
    async fn pause_fails_closed_without_touching_storage() {
        let store = FakeSnapshotStore::new();
        let err = KataAdapter::new()
            .pause("pod-1", &store)
            .await
            .expect_err("kata pause must fail closed");
        assert!(matches!(err, RuntimeError::Unsupported(_)), "got {err:?}");
        // Critically: nothing was captured or written — no destructive snapshot.
        assert!(
            store.is_empty(),
            "kata pause must not write any snapshot data"
        );
    }

    #[tokio::test]
    async fn resume_fails_closed() {
        let store = FakeSnapshotStore::new();
        let manifest = SnapshotManifest::default();
        let err = KataAdapter::new()
            .resume(&manifest, "pod-1", &store)
            .await
            .expect_err("kata resume must fail closed");
        assert!(matches!(err, RuntimeError::Unsupported(_)), "got {err:?}");
    }
}
