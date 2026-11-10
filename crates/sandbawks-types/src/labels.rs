//! Well-known Kubernetes label keys that form cross-crate contracts
//! between sandbawks components.
//!
//! Keeping these in the schema-only `sandbawks-types` crate means the
//! controller (which stamps a `nodeAffinity` referencing the key) and
//! the `sandbawks-node` DaemonSet (which sets the label) share a single
//! source of truth. A drift between the two would silently break sandbox
//! scheduling — exactly the failure mode #272 exists to prevent.

/// Node label advertising that the `sandbawks-node` DaemonSet has staged
/// the static `envd` binary onto this node (#272).
///
/// Every sandbox Pod execs the node-staged binary as its entrypoint, so
/// the controller stamps a `requiredDuringSchedulingIgnoredDuringExecution`
/// nodeAffinity with an `Exists` match on this key
/// (`crates/sandbawks-controller/src/pod_adapter.rs`). That keeps a
/// sandbox from scheduling onto a node before the DaemonSet's
/// `stage-envd` init container has placed the binary — which would
/// otherwise mount an empty `DirectoryOrCreate` hostPath and CrashLoop on
/// `exec: /sandbawks/bin/envd: no such file or directory` (observable on
/// cluster bootstrap, node bring-up, and DaemonSet rollouts).
///
/// The `sandbawks.dev/` prefix matches the other sandbawks labels
/// (`sandbawks.dev/sandbox`, …).
pub const ENVD_STAGED_LABEL: &str = "sandbawks.dev/envd-staged";

/// Value written for [`ENVD_STAGED_LABEL`].
///
/// The scheduling gate only tests for the key's *existence*, so the value
/// is informational today. Per-digest staging (#270) will replace it with
/// the staged binary's content hash so a sandbox can additionally require
/// the exact `envd` build its checkpoint was taken against.
pub const ENVD_STAGED_LABEL_VALUE: &str = "true";

/// Node label advertising the host `runsc` (gVisor) binary release this node
/// is currently running (#546, sub-issue of the #545 fleet-upgrade epic).
///
/// The `sandbawks-node` DaemonSet shells `runsc --version` against the host
/// `/usr/local/bin/runsc`, parses the `release-<ver>` token, and stamps it here
/// so runsc version drift across the fleet is visible and alertable with a plain
/// `kubectl get nodes -L sandbawks.dev/runsc-version`. gVisor checkpoint/restore
/// requires the *exact* same runsc build on the pausing and restoring node, so a
/// node whose label diverges from the fleet's pinned version cannot safely host a
/// cross-node resume — the #545 restore-placement gate keys off this signal.
///
/// The value is the full `release-<ver>` token (e.g. `release-20260525.0`), which
/// is a valid Kubernetes label value (alphanumerics plus `-`/`_`/`.`, ≤63 chars,
/// starting and ending alphanumeric). The `sandbawks.dev/` prefix matches the
/// other sandbawks labels.
pub const RUNSC_VERSION_LABEL: &str = "sandbawks.dev/runsc-version";

/// Per-sandbox identity label key, carrying the Sandbox CR's `metadata.name`.
///
/// Stamped on every sandbox Pod by the controller
/// (`crates/sandbawks-controller/src/pod_adapter.rs`) and consumed in two
/// cross-crate contracts that must agree on the exact key:
///
/// * the controller's per-sandbox egress `NetworkPolicy` `podSelector`
///   targets the Pod by this label (#443), and
/// * the `sandbawks-node` DaemonSet reads it to map a Pod back to its
///   Sandbox (`gvisor.rs`, `memory_pressure.rs`, `sandbox_pressure.rs`).
///
/// Living here — rather than as a private literal in each crate — keeps the
/// label one source of truth so the policy selector and the Pod label can
/// never drift, the same drift-prevention rationale as [`ENVD_STAGED_LABEL`].
pub const SANDBOX_NAME_LABEL: &str = "sandbawks.dev/sandbox";

/// Sandbox annotation carrying the name of the owning `TeamAPIKey` CR — the
/// per-key billing/attribution dimension for usage metering (epic #480 Phase 2,
/// issue #631).
///
/// The Sandbox's `metadata.namespace` already identifies the owning *team* (1
/// team = 1 namespace, per the `TeamAPIKey` model), but a team may rotate or
/// issue multiple keys, so the key identity is not recoverable from the team
/// alone. The api gateway resolves the `TeamAPIKey` name at auth time
/// (`crates/sandbawks-api/src/auth.rs`) and stamps it here when it creates a
/// Sandbox (and when it claims a warm-pool member), so the controller's reconcile
/// loop can attribute resource-seconds to the exact key without re-reading the
/// key store.
///
/// An **annotation**, not a label: it is attribution metadata the controller
/// reads, never a selector — nothing schedules or filters on it, so it does not
/// belong in the (indexed, 63-char-bounded) label space. Absent on operator-hand-
/// edited or pre-#631 Sandboxes; consumers MUST treat a missing value as "unknown
/// key" rather than failing. The `sandbawks.dev/` prefix matches the other
/// sandbawks keys.
pub const API_KEY_NAME_ANNOTATION: &str = "sandbawks.dev/api-key-name";

/// Pod annotation marking a sandbox Pod as a **snapshot-mode clone** that must
/// reconcile its inherited guest identity on first boot (issue #643).
///
/// A gVisor `snapshot`-mode restore rebuilds the entire Sentry from the source's
/// checkpoint, so the clone inherits the *source's* in-kernel identity — UTS
/// hostname (`uname -n`), and any `/etc/machine-id` / D-Bus machine-id / boot-id
/// captured in the overlay (`pages.img`). The kubelet-managed furniture binds
/// (`/etc/hostname`, …) already reflect the destination Pod, so a clone otherwise
/// runs with a split hostname and a machine-id duplicated across the source and
/// every clone of it. See the design spec
/// `docs/superpowers/specs/2026-06-24-clone-guest-identity-reconcile-design.md`.
///
/// The controller stamps this annotation (value [`CLONE_IDENTITY_RESET_VALUE`])
/// on the restore Pod precisely when [`crate::sandbox::clone_needs_guest_identity_reset`]
/// holds — a `spec.restoreFrom` clone whose effective persistence mode is
/// [`crate::sandbox::PersistenceMode::Snapshot`]. `envd` reads it from the pod's
/// downward-API projection on startup and, when present, runs the one-shot
/// identity reset (set UTS hostname to the new sandbox name; regenerate
/// `/etc/machine-id` & friends) before handing control to the workload.
///
/// An **annotation**, not a label: nothing schedules or selects on it — it is a
/// one-shot instruction `envd` consumes once per clone boot. The `sandbawks.dev/`
/// prefix matches the other sandbawks keys. Absent on cold-booted sandboxes and
/// on filesystem-mode clones (whose fresh Pod cold-boots its own identity), and
/// on pre-#643 clones; consumers MUST treat a missing value as "no reset needed".
pub const CLONE_IDENTITY_RESET_ANNOTATION: &str = "sandbawks.dev/clone-identity-reset";

/// Value written for [`CLONE_IDENTITY_RESET_ANNOTATION`].
///
/// The instruction is binary today (reset, or no annotation at all), so the
/// value is informational — `envd` keys off the annotation's *presence*, not its
/// content. A future revision could carry a comma-separated facet list
/// (`hostname,machine-id`) to scope the reset, which is why the contract is a
/// constant rather than an inlined literal.
pub const CLONE_IDENTITY_RESET_VALUE: &str = "true";
