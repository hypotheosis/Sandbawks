//! Unit tests for the lazy-restore staging-dir garbage collection
//! (Plan 08 B2 / issue #262, extended by issue #483).
//!
//! With lazy / page-fault restore (Plan 08 B1) gVisor reads pages from the
//! staged `--image-path` bundle for the resumed container's whole lifetime,
//! so a bundle may only be reclaimed once its owning `Sandbox` leaves
//! `Running`/`Resuming` — *and*, since resume became scheduler-placed (#483),
//! only on the node that actually hosts the live Pod. A resume retry can move
//! the restore Pod to a different node and re-stage there, orphaning the
//! original node's copy while the sandbox stays `Running`/`Resuming`. These
//! tests pin:
//!
//! 1. The pure phase predicate
//!    ([`sandbawks_node::gvisor::staging_bundle_needed`]) for every
//!    `SandboxPhase` plus the CR-absent (`None`) case.
//! 2. The node-aware retention predicate
//!    ([`sandbawks_node::gvisor::staging_bundle_retained_on_node`]): the phase
//!    gate combined with the locality gate.
//! 3. The sweep ([`sandbawks_node::gvisor::sweep_staging_dirs`]) against a
//!    real on-disk staging tree and a fake owner lookup: a `Running` owner's
//!    subtree survives on its host node, an absent / `Paused` owner's subtree
//!    is reclaimed, and a `Running` owner whose Pod moved to another node has
//!    its orphaned subtree reclaimed here (#483).
//! 4. The full lifecycle the plan calls out: stage a bundle, mark the
//!    sandbox `Running` → sweep keeps it; flip to `Paused` → sweep reclaims
//!    it.
//! 5. A missing staging root is an empty sweep, not an error.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use async_trait::async_trait;
use sandbawks_node::gvisor::{
    staging_bundle_needed, staging_bundle_retained_on_node, sweep_staging_dirs, SandboxOwnerLookup,
    SandboxStagingOwner,
};
use sandbawks_types::runtime::RuntimeError;
use sandbawks_types::SandboxPhase;

/// The node these tests pretend the sweeper is running on.
const THIS_NODE: &str = "node-a";

/// In-memory [`SandboxOwnerLookup`] double. A key present in `owners` maps to
/// that sandbox's [`SandboxStagingOwner`]; a key absent models the `Sandbox`
/// CR being gone (`owner_of` returns `Ok(None)` — the common reclaim trigger).
/// Ids in `errors` instead return a transient error, modelling a kube `Get`
/// failure so a test can assert the sweep skips that subtree without aborting.
struct FakeOwnerLookup {
    owners: HashMap<String, SandboxStagingOwner>,
    errors: HashSet<String>,
}

impl FakeOwnerLookup {
    /// Build a lookup where each named sandbox has the given phase and is
    /// hosted on [`THIS_NODE`] — the common "owner's Pod is local" case.
    fn new(phases: &[(&str, SandboxPhase)]) -> Self {
        Self::with_owners(
            &phases
                .iter()
                .map(|(name, phase)| (*name, owner_on(*phase, Some(THIS_NODE))))
                .collect::<Vec<_>>(),
        )
    }

    /// Build a lookup from explicit owner state, so a test can place a
    /// sandbox's Pod on a specific node (or leave it unscheduled).
    fn with_owners(owners: &[(&str, SandboxStagingOwner)]) -> Self {
        Self {
            owners: owners
                .iter()
                .map(|(name, owner)| ((*name).to_string(), owner.clone()))
                .collect(),
            errors: HashSet::new(),
        }
    }

    /// Make `owner_of` return [`RuntimeError::Transient`] for `sandbox_id`.
    fn erroring_on(mut self, sandbox_id: &str) -> Self {
        self.errors.insert(sandbox_id.to_string());
        self
    }
}

#[async_trait]
impl SandboxOwnerLookup for FakeOwnerLookup {
    async fn owner_of(
        &self,
        sandbox_id: &str,
    ) -> Result<Option<SandboxStagingOwner>, RuntimeError> {
        if self.errors.contains(sandbox_id) {
            return Err(RuntimeError::Transient(format!(
                "simulated kube Get failure for {sandbox_id}"
            )));
        }
        Ok(self.owners.get(sandbox_id).cloned())
    }
}

/// Build a [`SandboxStagingOwner`] with the given phase and host node
/// (`None` = unscheduled / host not yet known).
fn owner_on(phase: SandboxPhase, node: Option<&str>) -> SandboxStagingOwner {
    SandboxStagingOwner {
        phase,
        assigned_node: node.map(str::to_string),
    }
}

/// Lay out a `<staging_root>/<sandbox-id>/<job-segment>/bundle.img` tree so
/// the sweep has a non-empty subtree (with a file inside) to reclaim, exactly
/// as `stage_snapshot` would materialize one.
fn stage_fake_bundle(staging_root: &Path, sandbox_id: &str) {
    let dir = staging_root.join(sandbox_id).join("2026-06-02T00-00-00Z");
    std::fs::create_dir_all(&dir).expect("create fake staging dir");
    std::fs::write(dir.join("pages.img"), b"fake-pages").expect("write fake bundle file");
}

#[test]
fn predicate_keeps_live_container_phases() {
    // A live (or about-to-be-live) container may fault pages from the bundle.
    for phase in [
        SandboxPhase::Running,
        SandboxPhase::Resuming,
        SandboxPhase::Pausing,
    ] {
        assert!(
            staging_bundle_needed(Some(phase)),
            "phase {phase:?} must retain the staged bundle"
        );
    }
}

#[test]
fn predicate_drops_idle_and_absent_phases() {
    // No container reads the bundle in these phases; `None` is the CR-absent
    // (deleted) case. The plan's contract: keep on Running/Resuming, drop on
    // Paused/Failed/Terminating/absent — plus Creating (cold boot never
    // restores from a staged bundle).
    for phase in [
        Some(SandboxPhase::Creating),
        Some(SandboxPhase::Paused),
        Some(SandboxPhase::Failed),
        Some(SandboxPhase::Terminating),
        None,
    ] {
        assert!(
            !staging_bundle_needed(phase),
            "phase {phase:?} must reclaim the staged bundle"
        );
    }
}

#[test]
fn retention_predicate_combines_phase_and_node_locality() {
    // Absent CR → reclaim regardless of node.
    assert!(
        !staging_bundle_retained_on_node(None, THIS_NODE),
        "absent owner must reclaim"
    );

    // Live phase on this node → keep.
    assert!(
        staging_bundle_retained_on_node(
            Some(&owner_on(SandboxPhase::Running, Some(THIS_NODE))),
            THIS_NODE
        ),
        "live owner hosted on this node must keep"
    );

    // Live phase but host node not yet known → keep (never reclaim on
    // incomplete information).
    assert!(
        staging_bundle_retained_on_node(Some(&owner_on(SandboxPhase::Resuming, None)), THIS_NODE),
        "live owner with unknown host node must keep"
    );

    // Live phase but the Pod moved to another node → reclaim this node's
    // orphaned copy (#483). Cover every bundle-reading phase so the locality
    // gate (not merely the phase gate) is pinned for all three.
    for phase in [
        SandboxPhase::Running,
        SandboxPhase::Resuming,
        SandboxPhase::Pausing,
    ] {
        assert!(
            !staging_bundle_retained_on_node(Some(&owner_on(phase, Some("node-b"))), THIS_NODE),
            "live owner ({phase:?}) whose Pod is on another node must reclaim the local orphan"
        );
    }

    // Idle/terminal phase reclaims regardless of which node it names.
    assert!(
        !staging_bundle_retained_on_node(
            Some(&owner_on(SandboxPhase::Paused, Some(THIS_NODE))),
            THIS_NODE
        ),
        "idle owner must reclaim even when its last host was this node"
    );
}

#[tokio::test]
async fn sweep_keeps_running_owner_and_reclaims_absent_owner() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    stage_fake_bundle(root, "sbx-running");
    stage_fake_bundle(root, "sbx-deleted");

    // `sbx-running` is Running on this node (keep); `sbx-deleted` has no CR
    // (reclaim).
    let lookup = FakeOwnerLookup::new(&[("sbx-running", SandboxPhase::Running)]);

    let report = sweep_staging_dirs(root, &lookup, THIS_NODE)
        .await
        .expect("sweep ok");

    assert!(
        root.join("sbx-running").is_dir(),
        "Running owner's staging subtree must survive the sweep"
    );
    assert!(
        !root.join("sbx-deleted").exists(),
        "deleted owner's staging subtree must be reclaimed"
    );
    assert_eq!(report.kept, vec!["sbx-running".to_string()]);
    assert_eq!(report.reclaimed, vec!["sbx-deleted".to_string()]);
}

#[tokio::test]
async fn sweep_reclaims_running_owner_whose_pod_moved_to_another_node() {
    // #483: resume is scheduler-placed. A recovery retry rescheduled the
    // restore Pod from this node onto `node-b`, which re-staged the snapshot
    // there. The sandbox is still Running (on node-b), so the phase gate alone
    // would pin this node's now-orphaned copy until the sandbox terminates.
    // The locality gate must reclaim it here.
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    stage_fake_bundle(root, "sbx-local");
    stage_fake_bundle(root, "sbx-moved");

    let lookup = FakeOwnerLookup::with_owners(&[
        // Still hosted here → keep.
        (
            "sbx-local",
            owner_on(SandboxPhase::Running, Some(THIS_NODE)),
        ),
        // Live, but its Pod now runs on node-b → orphan here → reclaim.
        ("sbx-moved", owner_on(SandboxPhase::Running, Some("node-b"))),
    ]);

    let report = sweep_staging_dirs(root, &lookup, THIS_NODE)
        .await
        .expect("sweep ok");

    assert!(
        root.join("sbx-local").is_dir(),
        "owner hosted on this node must keep its bundle"
    );
    assert!(
        !root.join("sbx-moved").exists(),
        "orphaned bundle of an owner whose Pod moved to another node must be reclaimed"
    );
    assert_eq!(report.kept, vec!["sbx-local".to_string()]);
    assert_eq!(report.reclaimed, vec!["sbx-moved".to_string()]);
}

#[tokio::test]
async fn sweep_keeps_running_owner_with_unknown_host_node() {
    // A live owner whose `status.assigned_node` is not yet populated (resume
    // restore Pod still being placed): keep, never reclaim on incomplete
    // information.
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    stage_fake_bundle(root, "sbx-pending");

    let lookup =
        FakeOwnerLookup::with_owners(&[("sbx-pending", owner_on(SandboxPhase::Resuming, None))]);

    let report = sweep_staging_dirs(root, &lookup, THIS_NODE)
        .await
        .expect("sweep ok");

    assert!(
        root.join("sbx-pending").is_dir(),
        "owner with an unknown host node must keep its bundle (incomplete information)"
    );
    assert_eq!(report.kept, vec!["sbx-pending".to_string()]);
    assert_eq!(report.reclaimed, Vec::<String>::new());
}

#[tokio::test]
async fn sweep_reclaims_only_after_owner_leaves_running() {
    // The lifecycle the plan calls out: a staged bundle survives while its
    // sandbox is Running on this node, and is reclaimed once it flips to
    // Paused.
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    stage_fake_bundle(root, "sbx-lifecycle");

    let running = FakeOwnerLookup::new(&[("sbx-lifecycle", SandboxPhase::Running)]);
    let report = sweep_staging_dirs(root, &running, THIS_NODE)
        .await
        .expect("sweep ok");
    assert!(
        root.join("sbx-lifecycle").is_dir(),
        "bundle must be retained while the sandbox is Running"
    );
    assert_eq!(report.reclaimed, Vec::<String>::new());

    let paused = FakeOwnerLookup::new(&[("sbx-lifecycle", SandboxPhase::Paused)]);
    let report = sweep_staging_dirs(root, &paused, THIS_NODE)
        .await
        .expect("sweep ok");
    assert!(
        !root.join("sbx-lifecycle").exists(),
        "bundle must be reclaimed once the sandbox re-enters Paused"
    );
    assert_eq!(report.reclaimed, vec!["sbx-lifecycle".to_string()]);
}

#[tokio::test]
async fn sweep_skips_lookup_error_but_reclaims_others() {
    // Best-effort per subtree: a transient lookup error on one sandbox is
    // logged and skipped (the subtree is kept — never reclaim on incomplete
    // information), and must NOT abort the pass, so an unrelated reclaimable
    // subtree is still reclaimed this tick.
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    stage_fake_bundle(root, "sbx-flaky");
    stage_fake_bundle(root, "sbx-gone");

    // `sbx-flaky`'s lookup errors; `sbx-gone` has no CR (reclaim).
    let lookup = FakeOwnerLookup::new(&[]).erroring_on("sbx-flaky");

    let report = sweep_staging_dirs(root, &lookup, THIS_NODE)
        .await
        .expect("transient lookup error must not fail the whole sweep");

    assert!(
        root.join("sbx-flaky").is_dir(),
        "subtree whose lookup errored must be kept, not reclaimed"
    );
    assert!(
        !root.join("sbx-gone").exists(),
        "an unrelated reclaimable subtree must still be reclaimed despite the error"
    );
    assert_eq!(report.reclaimed, vec!["sbx-gone".to_string()]);
    // The errored subtree is neither reclaimed nor counted as deliberately
    // kept — it was skipped.
    assert!(report.kept.is_empty());
}

#[tokio::test]
async fn sweep_of_missing_root_is_empty_not_error() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let missing = tmp.path().join("never-staged");

    let lookup = FakeOwnerLookup::new(&[]);
    let report = sweep_staging_dirs(&missing, &lookup, THIS_NODE)
        .await
        .expect("sweep ok");

    assert_eq!(report.reclaimed, Vec::<String>::new());
    assert_eq!(report.kept, Vec::<String>::new());
}
