# sandbawks Plan 01 — Types crate + CRDs + Helm scaffold

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the foundational `sandbawks-types` Rust crate (CRD types, error types, shared DTOs), generate CRD YAML, and scaffold a Helm chart. No behavior — just schema and packaging. Every later plan builds on this.

**Architecture:** Convert the existing single-crate repo to a Cargo workspace. Add `crates/sandbawks-types` with `kube`-derived CustomResource types for `Sandbox`, `SandboxTemplate`, `SandboxWarmPool`, `TeamAPIKey`. Add a small `crdgen` binary that emits CRD YAML to `deploy/crds/`. Scaffold `deploy/helm/sandbawks` with placeholder Deployment/RBAC/CRD installation. Add CI to keep it honest.

**Tech Stack:**
- Rust 2021, Cargo workspace
- `kube` (CustomResource derive macro)
- `k8s-openapi` (Kubernetes API types)
- `schemars` (JSON Schema for CRDs)
- `serde` + `serde_yaml` + `serde_json`
- `thiserror`
- Helm 3 (for the chart)
- GitHub Actions CI

**Spec reference:** `docs/superpowers/specs/2026-05-21-sandbawks-mvp-design.md` §3.2 (repo layout), §4 (CRDs).

---

## File map

**Created:**
- `Cargo.toml` (rewritten as workspace root — replaces existing single-crate manifest)
- `crates/sandbawks-types/Cargo.toml`
- `crates/sandbawks-types/src/lib.rs`
- `crates/sandbawks-types/src/error.rs`
- `crates/sandbawks-types/src/resources.rs`
- `crates/sandbawks-types/src/sandbox.rs`
- `crates/sandbawks-types/src/sandbox_template.rs`
- `crates/sandbawks-types/src/warm_pool.rs`
- `crates/sandbawks-types/src/team_api_key.rs`
- `crates/sandbawks-types/src/bin/crdgen.rs`
- `crates/sandbawks-types/tests/serde_roundtrip.rs`
- `deploy/crds/` (generated YAML lives here)
- `deploy/helm/sandbawks/Chart.yaml`
- `deploy/helm/sandbawks/values.yaml`
- `deploy/helm/sandbawks/templates/_helpers.tpl`
- `deploy/helm/sandbawks/templates/serviceaccount.yaml`
- `deploy/helm/sandbawks/templates/rbac.yaml`
- `deploy/helm/sandbawks/templates/controller-deployment.yaml`
- `deploy/helm/sandbawks/templates/api-deployment.yaml`
- `deploy/helm/sandbawks/templates/api-service.yaml`
- `deploy/helm/sandbawks/templates/node-daemonset.yaml`
- `deploy/helm/sandbawks/templates/crds/` (copied from `deploy/crds`)
- `.github/workflows/ci.yml`
- `changelog/2026-05-21-plan-01-types-and-crds.md`

**Deleted:**
- `src/main.rs` (the template-commit binary — replaced by workspace, the controller binary lives in `crates/sandbawks-controller` in a later plan)

**Modified:**
- `.gitignore` (add `/deploy/helm/sandbawks/templates/crds/` won't be ignored, but add Helm packaging artifacts)

---

## Task 1: Convert root to Cargo workspace, drop the template binary

**Files:**
- Modify: `Cargo.toml`
- Delete: `src/main.rs` (the `src/` directory becomes empty and is removed)

- [ ] **Step 1: Replace `Cargo.toml` with workspace manifest**

Overwrite `Cargo.toml` with:

```toml
[workspace]
resolver = "2"
members = [
    "crates/sandbawks-types",
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
k8s-openapi = { version = "0.24", features = ["v1_31"] }
schemars = "0.8"

# serde stack
serde = { version = "1.0.216", features = ["derive"] }
serde_json = "1.0"
serde_yaml = "0.9"

# errors / logging
thiserror = "2.0.6"
tracing = "0.1.41"

# async / runtime (kept for later crates; types crate doesn't use these)
tokio = { version = "1.42.0", features = ["rt-multi-thread", "macros", "time", "signal"] }
anyhow = "1.0"

[profile.release]
lto = "thin"
codegen-units = 1
strip = "symbols"
```

- [ ] **Step 2: Remove the old template binary**

```bash
rm src/main.rs
rmdir src
```

- [ ] **Step 3: Verify workspace resolves (it'll fail until Task 2 lands the member crate, so use `--workspace --offline=false` carefully — we want it to fail cleanly with "member not found")**

Run: `cargo metadata --format-version 1 --no-deps 2>&1 | head -5`
Expected: emits JSON with a `"workspace_members"` array containing nothing yet (or an error pointing at the missing `crates/sandbawks-types/Cargo.toml`). Either is fine — the next task creates the member.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml
git rm -r src
git commit -m "$(cat <<'EOF'
chore: convert to Cargo workspace, drop template binary

Root Cargo.toml is now a workspace manifest. The template tracing
binary in src/main.rs is removed; the controller binary will live in
crates/sandbawks-controller in a later plan.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Create `sandbawks-types` crate skeleton

**Files:**
- Create: `crates/sandbawks-types/Cargo.toml`
- Create: `crates/sandbawks-types/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/sandbawks-types/tests/smoke.rs`:

```rust
#[test]
fn crate_links() {
    // Smoke test: if this file compiles and links against
    // sandbawks_types, the crate exists and is on the workspace.
    let _ = sandbawks_types::CRATE_NAME;
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sandbawks-types 2>&1 | tail -20`
Expected: failure — "no library targets found in package `sandbawks-types`" or the workspace member is missing.

- [ ] **Step 3: Create `crates/sandbawks-types/Cargo.toml`**

```toml
[package]
name = "sandbawks-types"
version.workspace = true
edition.workspace = true
authors.workspace = true
license.workspace = true
repository.workspace = true
description = "Shared CRD types, DTOs, and error types for the sandbawks Kubernetes sandbox controller."

[dependencies]
kube = { workspace = true }
k8s-openapi = { workspace = true }
schemars = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
serde_yaml = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
# (none yet — Task 9 adds serde_yaml round-trip tests using only the workspace dep)

[[bin]]
name = "crdgen"
path = "src/bin/crdgen.rs"
```

- [ ] **Step 4: Create `crates/sandbawks-types/src/lib.rs`**

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
pub mod sandbox;
pub mod sandbox_template;
pub mod team_api_key;
pub mod warm_pool;

pub use error::Error;
pub use resources::{ResourceRequests, ResourceQuantity};
pub use sandbox::{
    NetworkPolicy, Sandbox, SandboxCondition, SandboxPhase, SandboxSpec, SandboxStatus,
};
pub use sandbox_template::{SandboxTemplate, SandboxTemplateSpec};
pub use team_api_key::{TeamApiKey, TeamApiKeySpec};
pub use warm_pool::{SandboxWarmPool, SandboxWarmPoolSpec, SandboxWarmPoolStatus};
```

This `lib.rs` references modules that don't exist yet — that's intentional. Tasks 3–8 add each module file. After Task 2 it won't compile; that's expected and Task 3 onwards fixes it incrementally.

- [ ] **Step 5: Add the new crate to the workspace members**

In `Cargo.toml`, the workspace `members` array already includes `"crates/sandbawks-types"` from Task 1, so no change needed. Verify with:

Run: `grep -A2 '\[workspace\]' Cargo.toml | grep sandbawks-types`
Expected: matches the line.

- [ ] **Step 6: Commit (compilation deliberately broken — recorded as such)**

```bash
git add crates/sandbawks-types/Cargo.toml crates/sandbawks-types/src/lib.rs crates/sandbawks-types/tests/smoke.rs
git commit -m "$(cat <<'EOF'
feat(types): skeleton sandbawks-types crate

Adds the workspace member with module declarations. Modules themselves
land in subsequent tasks; the crate intentionally does not compile yet.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: `Error` type

**Files:**
- Create: `crates/sandbawks-types/src/error.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/sandbawks-types/tests/smoke.rs`:

```rust
#[test]
fn error_display_renders() {
    let e = sandbawks_types::Error::InvalidQuantity("not a number".into());
    let rendered = format!("{e}");
    assert!(rendered.contains("not a number"), "got: {rendered}");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sandbawks-types --test smoke 2>&1 | tail -20`
Expected: compilation error — `error::Error` cannot be resolved.

- [ ] **Step 3: Create `crates/sandbawks-types/src/error.rs`**

```rust
use thiserror::Error;

/// Errors raised when parsing or validating sandbawks types.
///
/// The variants here are intentionally schema-level: they describe what's
/// wrong with a piece of CRD data, not what went wrong with an API call
/// or controller reconcile. Higher-level error types live in the
/// downstream `sandbawks-api`, `sandbawks-controller`, and
/// `sandbawks-node` crates.
#[derive(Debug, Error)]
pub enum Error {
    /// A quantity string (e.g. `"2"`, `"1Gi"`, `"500m"`) could not be
    /// parsed into a structured ResourceQuantity.
    #[error("invalid resource quantity: {0}")]
    InvalidQuantity(String),

    /// JSON (de)serialization failed.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// YAML (de)serialization failed.
    #[error("yaml error: {0}")]
    Yaml(#[from] serde_yaml::Error),
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sandbawks-types --test smoke error_display_renders 2>&1 | tail -10`
Expected: 1 test passed (the `crate_links` test is still broken until later tasks; if cargo bails before reaching this test, focus the test selector as shown).

- [ ] **Step 5: Commit**

```bash
git add crates/sandbawks-types/src/error.rs crates/sandbawks-types/tests/smoke.rs
git commit -m "$(cat <<'EOF'
feat(types): Error enum for schema-level validation failures

thiserror-derived variants for quantity parsing and serde failures.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: `ResourceQuantity` + `ResourceRequests`

**Files:**
- Create: `crates/sandbawks-types/src/resources.rs`

- [ ] **Step 1: Write the failing tests**

Create `crates/sandbawks-types/tests/resources.rs`:

```rust
use sandbawks_types::{Error, ResourceQuantity, ResourceRequests};

#[test]
fn parse_plain_integer() {
    let q: ResourceQuantity = "2".parse().unwrap();
    assert_eq!(q.as_str(), "2");
}

#[test]
fn parse_memory_gibibytes() {
    let q: ResourceQuantity = "1Gi".parse().unwrap();
    assert_eq!(q.as_str(), "1Gi");
}

#[test]
fn parse_millicores() {
    let q: ResourceQuantity = "500m".parse().unwrap();
    assert_eq!(q.as_str(), "500m");
}

#[test]
fn parse_rejects_empty() {
    let err = "".parse::<ResourceQuantity>().unwrap_err();
    assert!(matches!(err, Error::InvalidQuantity(_)));
}

#[test]
fn parse_rejects_garbage() {
    let err = "not a quantity".parse::<ResourceQuantity>().unwrap_err();
    assert!(matches!(err, Error::InvalidQuantity(_)));
}

#[test]
fn resource_requests_yaml_roundtrip() {
    let r = ResourceRequests {
        cpu: "2".parse().unwrap(),
        memory: "2Gi".parse().unwrap(),
        ephemeral_storage: Some("10Gi".parse().unwrap()),
    };
    let yaml = serde_yaml::to_string(&r).unwrap();
    let back: ResourceRequests = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(r, back);
    // Spec §4.1 uses ephemeralStorage in camelCase
    assert!(yaml.contains("ephemeralStorage"), "yaml was: {yaml}");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p sandbawks-types --test resources 2>&1 | tail -20`
Expected: compilation error — `ResourceQuantity` / `ResourceRequests` unresolved.

- [ ] **Step 3: Create `crates/sandbawks-types/src/resources.rs`**

```rust
use std::fmt;
use std::str::FromStr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::error::Error;

/// A Kubernetes-style resource quantity, e.g. `"2"`, `"500m"`, `"1Gi"`.
///
/// We keep the original string verbatim instead of normalizing into a
/// numeric type. Kubernetes itself accepts the string form on the wire
/// and normalizes server-side, so storing the lexical form means we
/// don't round-trip through float and lose precision on units like
/// `"1500m"` or `"1.5Gi"`.
///
/// Parsing only validates the *shape* (non-empty, matches the regex
/// `^[0-9]+(\.[0-9]+)?[a-zA-Z]*$`). Numeric range / unit-correctness is
/// the API server's job — we deliberately don't reimplement it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct ResourceQuantity(String);

impl ResourceQuantity {
    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ResourceQuantity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for ResourceQuantity {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(Error::InvalidQuantity(s.to_owned()));
        }
        // Shape check: digits, optional fractional part, optional unit suffix.
        // Avoids pulling in regex for one validator.
        let mut chars = s.chars().peekable();
        let mut saw_digit = false;
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() {
                saw_digit = true;
                chars.next();
            } else {
                break;
            }
        }
        if !saw_digit {
            return Err(Error::InvalidQuantity(s.to_owned()));
        }
        if let Some(&'.') = chars.peek() {
            chars.next();
            let mut saw_frac = false;
            while let Some(&c) = chars.peek() {
                if c.is_ascii_digit() {
                    saw_frac = true;
                    chars.next();
                } else {
                    break;
                }
            }
            if !saw_frac {
                return Err(Error::InvalidQuantity(s.to_owned()));
            }
        }
        // Anything left must be a unit suffix made of ASCII letters.
        for c in chars {
            if !c.is_ascii_alphabetic() {
                return Err(Error::InvalidQuantity(s.to_owned()));
            }
        }
        Ok(Self(s.to_owned()))
    }
}

/// Per-sandbox resource budget.
///
/// `cpu` and `memory` map directly onto a Pod's container `resources.limits`.
/// `ephemeral_storage` maps to the Pod's ephemeral-storage limit (the
/// snapshot delta of the sandbox's writable layer).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRequests {
    pub cpu: ResourceQuantity,
    pub memory: ResourceQuantity,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub ephemeral_storage: Option<ResourceQuantity>,
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p sandbawks-types --test resources 2>&1 | tail -20`
Expected: 6 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/sandbawks-types/src/resources.rs crates/sandbawks-types/tests/resources.rs
git commit -m "$(cat <<'EOF'
feat(types): ResourceQuantity + ResourceRequests with parse validation

Quantity stays as a lexical string; we only shape-check it (regex-free)
so we don't lose precision on units like 1.5Gi.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: `Sandbox` CRD

**Files:**
- Create: `crates/sandbawks-types/src/sandbox.rs`

- [ ] **Step 1: Write the failing tests**

Create `crates/sandbawks-types/tests/sandbox.rs`:

```rust
use kube::CustomResourceExt;
use sandbawks_types::{
    ResourceQuantity, ResourceRequests, Sandbox, SandboxPhase, SandboxSpec, SandboxStatus,
};

#[test]
fn crd_metadata_matches_spec() {
    let crd = Sandbox::crd();
    assert_eq!(crd.spec.group, "sandbawks.io");
    assert_eq!(crd.spec.names.kind, "Sandbox");
    assert_eq!(crd.spec.names.plural, "sandboxes");
    assert!(crd.spec.names.short_names.as_ref().unwrap().contains(&"sbx".to_owned()));
}

#[test]
fn yaml_roundtrip_sandbox() {
    let s = Sandbox::new(
        "sbx-abc123",
        SandboxSpec {
            template_ref: "python-data-sci".into(),
            runtime_class_name: Some("gvisor".into()),
            resources: ResourceRequests {
                cpu: "2".parse().unwrap(),
                memory: "2Gi".parse().unwrap(),
                ephemeral_storage: Some("10Gi".parse().unwrap()),
            },
            timeout_seconds: 3600,
            auto_pause: true,
            env: Default::default(),
            metadata: Default::default(),
            network_policy: None,
        },
    );
    let yaml = serde_yaml::to_string(&s).unwrap();
    let back: Sandbox = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(s.spec, back.spec);
}

#[test]
fn phase_serializes_pascal_case() {
    // The state machine in spec §5 uses PascalCase phase names.
    let p = SandboxPhase::Paused;
    assert_eq!(serde_json::to_string(&p).unwrap(), "\"Paused\"");
    let parsed: SandboxPhase = serde_json::from_str("\"Resuming\"").unwrap();
    assert_eq!(parsed, SandboxPhase::Resuming);
}

#[test]
fn status_subresource_default() {
    // Make sure SandboxStatus has a sensible default (used when the
    // controller first observes a freshly-created Sandbox).
    let st = SandboxStatus::default();
    assert_eq!(st.phase, SandboxPhase::Creating);
    assert!(st.assigned_node.is_none());
    assert!(st.snapshot_ref.is_none());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p sandbawks-types --test sandbox 2>&1 | tail -20`
Expected: compilation error — `Sandbox` etc. unresolved.

- [ ] **Step 3: Create `crates/sandbawks-types/src/sandbox.rs`**

```rust
use std::collections::BTreeMap;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::resources::ResourceRequests;

/// The primary resource: one Sandbox = one E2B sandbox = one Pod (when running).
///
/// Naming the CRD plural `sandboxes` matches the english spelling
/// expected by `kubectl get sandboxes` and by the E2B `/sandboxes`
/// endpoint. Short name `sbx` matches E2B's sandbox ID prefix.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[kube(
    group = "sandbawks.io",
    version = "v1alpha1",
    kind = "Sandbox",
    plural = "sandboxes",
    shortname = "sbx",
    namespaced,
    status = "SandboxStatus",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Template","type":"string","jsonPath":".spec.templateRef"}"#,
    printcolumn = r#"{"name":"Runtime","type":"string","jsonPath":".spec.runtimeClassName"}"#,
    printcolumn = r#"{"name":"Node","type":"string","jsonPath":".status.assignedNode"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct SandboxSpec {
    /// Name of a `SandboxTemplate` in the same namespace.
    pub template_ref: String,

    /// Override the template's default RuntimeClass.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub runtime_class_name: Option<String>,

    pub resources: ResourceRequests,

    /// Seconds before the sandbox is auto-paused (if `auto_pause`) or
    /// killed (if not).
    pub timeout_seconds: u32,

    /// If true, the timeout transitions the sandbox to `Paused`. If
    /// false, the timeout transitions to `Terminating`.
    #[serde(default)]
    pub auto_pause: bool,

    /// Environment variables injected into the sandbox process.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,

    /// Opaque K/V passed through from the E2B API. Not interpreted by
    /// the controller — surfaced back via `GET /sandboxes`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,

    /// Optional egress policy. `None` means deny-all by default
    /// (the controller still attaches a default-deny NetworkPolicy).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub network_policy: Option<NetworkPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPolicy {
    /// Egress destinations (DNS names) to permit. Empty means deny-all
    /// egress (the default-deny baseline is unchanged).
    #[serde(default)]
    pub egress_allow: Vec<String>,
}

/// Lifecycle phase. See spec §5 for transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub enum SandboxPhase {
    Creating,
    Running,
    Pausing,
    Paused,
    Resuming,
    Terminating,
    Failed,
}

impl Default for SandboxPhase {
    fn default() -> Self {
        Self::Creating
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SandboxStatus {
    #[serde(default)]
    pub phase: SandboxPhase,

    /// Node currently hosting the sandbox Pod (None when Paused or
    /// Creating before scheduling).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub assigned_node: Option<String>,

    /// Reference to the backing Pod, if any.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub pod_ref: Option<String>,

    /// `host:port` of the in-sandbox envd agent when running.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub envd_address: Option<String>,

    /// S3 URL of the snapshot when Paused.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub snapshot_ref: Option<String>,

    /// RFC 3339 timestamp of the last phase transition.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub last_transition_time: Option<String>,

    /// Free-form conditions in the conventional Kubernetes style.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<SandboxCondition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SandboxCondition {
    #[serde(rename = "type")]
    pub kind: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub last_transition_time: Option<String>,
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p sandbawks-types --test sandbox 2>&1 | tail -20`
Expected: 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/sandbawks-types/src/sandbox.rs crates/sandbawks-types/tests/sandbox.rs
git commit -m "$(cat <<'EOF'
feat(types): Sandbox CRD with full spec + status + phase enum

Group sandbawks.io, version v1alpha1, kind Sandbox, plural sandboxes,
short name sbx. Phase enum covers the 7 states from spec §5.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: `SandboxTemplate` CRD

**Files:**
- Create: `crates/sandbawks-types/src/sandbox_template.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/sandbawks-types/tests/sandbox_template.rs`:

```rust
use kube::CustomResourceExt;
use sandbawks_types::{ResourceRequests, SandboxTemplate, SandboxTemplateSpec};

#[test]
fn crd_metadata() {
    let crd = SandboxTemplate::crd();
    assert_eq!(crd.spec.group, "sandbawks.io");
    assert_eq!(crd.spec.names.kind, "SandboxTemplate");
    assert_eq!(crd.spec.names.plural, "sandboxtemplates");
}

#[test]
fn yaml_roundtrip() {
    let t = SandboxTemplate::new(
        "python-data-sci",
        SandboxTemplateSpec {
            image: "ghcr.io/acme/py-data-sci:1.4".into(),
            default_runtime_class: "gvisor".into(),
            default_resources: ResourceRequests {
                cpu: "1".parse().unwrap(),
                memory: "1Gi".parse().unwrap(),
                ephemeral_storage: Some("5Gi".parse().unwrap()),
            },
            envd_image: "ghcr.io/sandbawks/envd:0.1".into(),
            default_timeout_seconds: 3600,
        },
    );
    let yaml = serde_yaml::to_string(&t).unwrap();
    let back: SandboxTemplate = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(t.spec, back.spec);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sandbawks-types --test sandbox_template 2>&1 | tail -20`
Expected: compilation error — `SandboxTemplate` unresolved.

- [ ] **Step 3: Create `crates/sandbawks-types/src/sandbox_template.rs`**

```rust
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::resources::ResourceRequests;

/// A reusable definition for a class of sandboxes.
///
/// Templates capture the OCI image + sensible defaults (resources,
/// runtime class, timeout, envd build). Per-Sandbox spec values
/// override the template defaults.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[kube(
    group = "sandbawks.io",
    version = "v1alpha1",
    kind = "SandboxTemplate",
    plural = "sandboxtemplates",
    shortname = "sbxtpl",
    namespaced,
    printcolumn = r#"{"name":"Image","type":"string","jsonPath":".spec.image"}"#,
    printcolumn = r#"{"name":"Runtime","type":"string","jsonPath":".spec.defaultRuntimeClass"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct SandboxTemplateSpec {
    /// OCI image reference. Must include envd-rs in the image.
    pub image: String,

    /// RuntimeClass name to use unless the Sandbox overrides it.
    pub default_runtime_class: String,

    /// Resource defaults applied when the Sandbox doesn't specify them.
    pub default_resources: ResourceRequests,

    /// envd-rs image (separate from the template image so envd can be
    /// updated independently — injected as a sidecar by the controller).
    pub envd_image: String,

    /// Default timeout in seconds.
    pub default_timeout_seconds: u32,
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sandbawks-types --test sandbox_template 2>&1 | tail -10`
Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/sandbawks-types/src/sandbox_template.rs crates/sandbawks-types/tests/sandbox_template.rs
git commit -m "$(cat <<'EOF'
feat(types): SandboxTemplate CRD

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: `SandboxWarmPool` CRD

**Files:**
- Create: `crates/sandbawks-types/src/warm_pool.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/sandbawks-types/tests/warm_pool.rs`:

```rust
use kube::CustomResourceExt;
use sandbawks_types::{SandboxWarmPool, SandboxWarmPoolSpec};

#[test]
fn crd_metadata() {
    let crd = SandboxWarmPool::crd();
    assert_eq!(crd.spec.names.kind, "SandboxWarmPool");
    assert_eq!(crd.spec.names.plural, "sandboxwarmpools");
}

#[test]
fn yaml_roundtrip() {
    let p = SandboxWarmPool::new(
        "python-pool",
        SandboxWarmPoolSpec {
            template_ref: "python-data-sci".into(),
            min_ready: 5,
            max_ready: 20,
        },
    );
    let yaml = serde_yaml::to_string(&p).unwrap();
    let back: SandboxWarmPool = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(p.spec, back.spec);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sandbawks-types --test warm_pool 2>&1 | tail -10`
Expected: compilation error — type unresolved.

- [ ] **Step 3: Create `crates/sandbawks-types/src/warm_pool.rs`**

```rust
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Pool of pre-paused sandboxes for sub-second cold-start handoff.
///
/// The controller keeps `status.ready` between `spec.min_ready` and
/// `spec.max_ready` by creating, booting, and pausing fresh sandboxes
/// from `spec.template_ref` and caching their snapshots.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[kube(
    group = "sandbawks.io",
    version = "v1alpha1",
    kind = "SandboxWarmPool",
    plural = "sandboxwarmpools",
    shortname = "sbxwp",
    namespaced,
    status = "SandboxWarmPoolStatus",
    printcolumn = r#"{"name":"Template","type":"string","jsonPath":".spec.templateRef"}"#,
    printcolumn = r#"{"name":"Min","type":"integer","jsonPath":".spec.minReady"}"#,
    printcolumn = r#"{"name":"Max","type":"integer","jsonPath":".spec.maxReady"}"#,
    printcolumn = r#"{"name":"Ready","type":"integer","jsonPath":".status.ready"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct SandboxWarmPoolSpec {
    pub template_ref: String,
    pub min_ready: u32,
    pub max_ready: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SandboxWarmPoolStatus {
    #[serde(default)]
    pub ready: u32,

    /// Sandbox names whose snapshots are currently warmed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warm_snapshots: Vec<String>,
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sandbawks-types --test warm_pool 2>&1 | tail -10`
Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/sandbawks-types/src/warm_pool.rs crates/sandbawks-types/tests/warm_pool.rs
git commit -m "$(cat <<'EOF'
feat(types): SandboxWarmPool CRD with status subresource

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: `TeamApiKey` CRD

**Files:**
- Create: `crates/sandbawks-types/src/team_api_key.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/sandbawks-types/tests/team_api_key.rs`:

```rust
use kube::CustomResourceExt;
use sandbawks_types::{TeamApiKey, TeamApiKeySpec};

#[test]
fn crd_metadata() {
    let crd = TeamApiKey::crd();
    assert_eq!(crd.spec.names.kind, "TeamAPIKey");
    assert_eq!(crd.spec.names.plural, "teamapikeys");
}

#[test]
fn yaml_roundtrip() {
    let k = TeamApiKey::new(
        "ci-bot",
        TeamApiKeySpec {
            key_prefix: "sbk_a1b2".into(),
            key_hash: "sha256:abcdef0123".into(),
            scopes: vec!["sandboxes:*".into()],
        },
    );
    let yaml = serde_yaml::to_string(&k).unwrap();
    let back: TeamApiKey = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(k.spec, back.spec);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sandbawks-types --test team_api_key 2>&1 | tail -10`
Expected: compilation error — type unresolved.

- [ ] **Step 3: Create `crates/sandbawks-types/src/team_api_key.rs`**

```rust
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Authenticates a team (= one Kubernetes namespace) at the
/// sandbawks-api ingress.
///
/// We store **only the hash** of the key, never the key itself. The
/// 8-char prefix is indexed so a lookup is O(1) instead of scanning
/// every key on every API call; the rest of the key is then
/// constant-time-compared against the stored hash.
///
/// The full key is shown to the user exactly once at issue time and
/// is not recoverable.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[kube(
    group = "sandbawks.io",
    version = "v1alpha1",
    kind = "TeamAPIKey",
    plural = "teamapikeys",
    shortname = "tak",
    namespaced,
    printcolumn = r#"{"name":"Prefix","type":"string","jsonPath":".spec.keyPrefix"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct TeamApiKeySpec {
    /// First 8 characters of the issued key, e.g. `"sbk_a1b2"`.
    pub key_prefix: String,

    /// SHA-256 hex digest of the full key, prefixed `"sha256:"`.
    pub key_hash: String,

    /// Scope strings. MVP only honours `sandboxes:*`; additional
    /// scopes are reserved for later plans.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<String>,
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sandbawks-types --test team_api_key 2>&1 | tail -10`
Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/sandbawks-types/src/team_api_key.rs crates/sandbawks-types/tests/team_api_key.rs
git commit -m "$(cat <<'EOF'
feat(types): TeamAPIKey CRD (hash-only, never the key itself)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: `crdgen` binary — emit CRD YAML

**Files:**
- Create: `crates/sandbawks-types/src/bin/crdgen.rs`
- Create: `deploy/crds/.gitkeep` (placeholder so the dir is tracked even before the binary runs)
- Create: `crates/sandbawks-types/tests/crdgen.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/sandbawks-types/tests/crdgen.rs`:

```rust
use std::process::Command;

#[test]
fn crdgen_emits_four_files() {
    let tmp = tempdir_from_target();
    let status = Command::new(env!("CARGO_BIN_EXE_crdgen"))
        .arg(&tmp)
        .status()
        .expect("crdgen binary should be runnable");
    assert!(status.success(), "crdgen exited non-zero");

    for name in [
        "sandboxes.sandbawks.io.yaml",
        "sandboxtemplates.sandbawks.io.yaml",
        "sandboxwarmpools.sandbawks.io.yaml",
        "teamapikeys.sandbawks.io.yaml",
    ] {
        let path = tmp.join(name);
        assert!(path.exists(), "missing: {}", path.display());
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.contains("apiVersion: apiextensions.k8s.io/v1"));
        assert!(body.contains("kind: CustomResourceDefinition"));
    }
}

fn tempdir_from_target() -> std::path::PathBuf {
    // Use cargo target dir so the test is hermetic and self-cleans on
    // `cargo clean`. tempfile crate not pulled in for one test.
    let mut p = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    p.push(format!("crdgen-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sandbawks-types --test crdgen 2>&1 | tail -10`
Expected: link error — `crdgen` binary missing.

- [ ] **Step 3: Create `crates/sandbawks-types/src/bin/crdgen.rs`**

```rust
//! Emit each CRD as YAML into a directory, one file per kind.
//!
//! Usage: `crdgen <out-dir>`
//!
//! Invoked from `cargo run -p sandbawks-types --bin crdgen -- deploy/crds`
//! during development, and from the CI check in Task 13.

use std::path::PathBuf;

use kube::CustomResourceExt;
use sandbawks_types::{Sandbox, SandboxTemplate, SandboxWarmPool, TeamApiKey};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir: PathBuf = std::env::args()
        .nth(1)
        .ok_or("usage: crdgen <out-dir>")?
        .into();
    std::fs::create_dir_all(&out_dir)?;

    write_crd(&out_dir, "sandboxes.sandbawks.io.yaml", &Sandbox::crd())?;
    write_crd(&out_dir, "sandboxtemplates.sandbawks.io.yaml", &SandboxTemplate::crd())?;
    write_crd(&out_dir, "sandboxwarmpools.sandbawks.io.yaml", &SandboxWarmPool::crd())?;
    write_crd(&out_dir, "teamapikeys.sandbawks.io.yaml", &TeamApiKey::crd())?;
    Ok(())
}

fn write_crd<T: serde::Serialize>(
    dir: &std::path::Path,
    file: &str,
    crd: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = serde_yaml::to_string(crd)?;
    let path = dir.join(file);
    std::fs::write(&path, yaml)?;
    println!("wrote {}", path.display());
    Ok(())
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sandbawks-types --test crdgen 2>&1 | tail -20`
Expected: 1 test passes.

- [ ] **Step 5: Generate CRD YAML into `deploy/crds/` and verify**

```bash
mkdir -p deploy/crds
cargo run -p sandbawks-types --bin crdgen -- deploy/crds
ls deploy/crds
```
Expected: four YAML files printed and present.

- [ ] **Step 6: Sanity-check the generated CRDs with `kubectl --dry-run` if available, otherwise skip**

```bash
if command -v kubectl >/dev/null 2>&1; then
  for f in deploy/crds/*.yaml; do kubectl --dry-run=client apply -f "$f"; done
else
  echo "kubectl not installed; skipping dry-run"
fi
```
Expected: each CRD reported as `created (dry run)` or the skip message.

- [ ] **Step 7: Commit**

```bash
git add crates/sandbawks-types/src/bin/crdgen.rs crates/sandbawks-types/tests/crdgen.rs deploy/crds/
git commit -m "$(cat <<'EOF'
feat(types): crdgen binary + generated CRD YAML

cargo run -p sandbawks-types --bin crdgen -- deploy/crds emits four
CustomResourceDefinition YAML files (sandboxes, sandboxtemplates,
sandboxwarmpools, teamapikeys), regenerated whenever the type
definitions change.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Helm chart skeleton

**Files:**
- Create: `deploy/helm/sandbawks/Chart.yaml`
- Create: `deploy/helm/sandbawks/values.yaml`
- Create: `deploy/helm/sandbawks/templates/_helpers.tpl`
- Create: `deploy/helm/sandbawks/templates/serviceaccount.yaml`
- Create: `deploy/helm/sandbawks/templates/rbac.yaml`
- Create: `deploy/helm/sandbawks/templates/controller-deployment.yaml`
- Create: `deploy/helm/sandbawks/templates/api-deployment.yaml`
- Create: `deploy/helm/sandbawks/templates/api-service.yaml`
- Create: `deploy/helm/sandbawks/templates/node-daemonset.yaml`
- Create: `deploy/helm/sandbawks/crds/` (symlink-equivalent: copy from `deploy/crds`)

- [ ] **Step 1: `Chart.yaml`**

```yaml
apiVersion: v2
name: sandbawks
description: E2B-compatible Kubernetes sandbox controller
type: application
version: 0.1.0
appVersion: "0.1.0"
home: https://github.com/pete-grace/sandbawks
sources:
  - https://github.com/pete-grace/sandbawks
keywords:
  - sandbox
  - e2b
  - ai
  - agents
```

- [ ] **Step 2: `values.yaml`**

```yaml
# Image references — overridden in production deployments.
images:
  api:
    repository: ghcr.io/pete-grace/sandbawks-api
    tag: 0.1.0
    pullPolicy: IfNotPresent
  controller:
    repository: ghcr.io/pete-grace/sandbawks-controller
    tag: 0.1.0
    pullPolicy: IfNotPresent
  node:
    repository: ghcr.io/pete-grace/sandbawks-node
    tag: 0.1.0
    pullPolicy: IfNotPresent

api:
  replicas: 2
  service:
    type: ClusterIP
    port: 8080
  resources:
    requests: { cpu: 100m, memory: 128Mi }
    limits:   { cpu: "1",  memory: 512Mi }

controller:
  replicas: 1   # leader-elected; can scale for HA
  resources:
    requests: { cpu: 100m, memory: 128Mi }
    limits:   { cpu: "1",  memory: 512Mi }

node:
  resources:
    requests: { cpu: 100m, memory: 128Mi }
    limits:   { cpu: "2",  memory: 1Gi }
  hostPathSnapshotCache: /var/lib/sandbawks/snapshots

# S3-compatible object store for snapshots. Endpoint left blank by
# default so a fresh `helm install` doesn't try to talk to a real bucket.
snapshotStore:
  endpoint: ""
  bucket: sandbawks-snapshots
  region: us-east-1
  # Credentials read from the secret named here. Created out-of-band.
  credentialsSecret: sandbawks-s3-credentials

# Install CRDs as part of the chart. Set to false if CRDs are managed
# separately (e.g. ArgoCD's CRD sync).
installCrds: true
```

- [ ] **Step 3: `templates/_helpers.tpl`**

```yaml
{{/* Standard labels applied to every resource. */}}
{{- define "sandbawks.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "sandbawks.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
```

- [ ] **Step 4: `templates/serviceaccount.yaml`**

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sandbawks-controller
  labels: {{- include "sandbawks.labels" . | nindent 4 }}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sandbawks-api
  labels: {{- include "sandbawks.labels" . | nindent 4 }}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sandbawks-node
  labels: {{- include "sandbawks.labels" . | nindent 4 }}
```

- [ ] **Step 5: `templates/rbac.yaml`**

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: sandbawks-controller
  labels: {{- include "sandbawks.labels" . | nindent 4 }}
rules:
  - apiGroups: ["sandbawks.io"]
    resources: ["sandboxes", "sandboxtemplates", "sandboxwarmpools", "teamapikeys"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["sandbawks.io"]
    resources: ["sandboxes/status", "sandboxwarmpools/status"]
    verbs: ["get", "update", "patch"]
  - apiGroups: [""]
    resources: ["pods", "services", "configmaps", "secrets", "events"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["networking.k8s.io"]
    resources: ["networkpolicies"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sandbawks-controller
  labels: {{- include "sandbawks.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sandbawks-controller
subjects:
  - kind: ServiceAccount
    name: sandbawks-controller
    namespace: {{ .Release.Namespace }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: sandbawks-api
  labels: {{- include "sandbawks.labels" . | nindent 4 }}
rules:
  - apiGroups: ["sandbawks.io"]
    resources: ["sandboxes", "sandboxtemplates", "teamapikeys"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sandbawks-api
  labels: {{- include "sandbawks.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sandbawks-api
subjects:
  - kind: ServiceAccount
    name: sandbawks-api
    namespace: {{ .Release.Namespace }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: sandbawks-node
  labels: {{- include "sandbawks.labels" . | nindent 4 }}
rules:
  - apiGroups: ["sandbawks.io"]
    resources: ["sandboxes"]
    verbs: ["get", "list", "watch", "update", "patch"]
  - apiGroups: ["sandbawks.io"]
    resources: ["sandboxes/status"]
    verbs: ["get", "update", "patch"]
  - apiGroups: [""]
    resources: ["pods", "nodes"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sandbawks-node
  labels: {{- include "sandbawks.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sandbawks-node
subjects:
  - kind: ServiceAccount
    name: sandbawks-node
    namespace: {{ .Release.Namespace }}
```

- [ ] **Step 6: `templates/controller-deployment.yaml`**

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
          # Plan 02 wires up the real controller binary; for now this
          # is a placeholder deployment that will fail to start. helm
          # install + helm lint must still succeed though.
          args: ["--placeholder"]
          resources:
            {{- toYaml .Values.controller.resources | nindent 12 }}
```

- [ ] **Step 7: `templates/api-deployment.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sandbawks-api
  labels: {{- include "sandbawks.labels" . | nindent 4 }}
spec:
  replicas: {{ .Values.api.replicas }}
  selector:
    matchLabels:
      {{- include "sandbawks.selectorLabels" . | nindent 6 }}
      app.kubernetes.io/component: api
  template:
    metadata:
      labels:
        {{- include "sandbawks.selectorLabels" . | nindent 8 }}
        app.kubernetes.io/component: api
    spec:
      serviceAccountName: sandbawks-api
      containers:
        - name: api
          image: "{{ .Values.images.api.repository }}:{{ .Values.images.api.tag }}"
          imagePullPolicy: {{ .Values.images.api.pullPolicy }}
          args: ["--placeholder"]
          ports:
            - name: http
              containerPort: 8080
          resources:
            {{- toYaml .Values.api.resources | nindent 12 }}
```

- [ ] **Step 8: `templates/api-service.yaml`**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: sandbawks-api
  labels: {{- include "sandbawks.labels" . | nindent 4 }}
spec:
  type: {{ .Values.api.service.type }}
  ports:
    - port: {{ .Values.api.service.port }}
      targetPort: http
      name: http
  selector:
    {{- include "sandbawks.selectorLabels" . | nindent 4 }}
    app.kubernetes.io/component: api
```

- [ ] **Step 9: `templates/node-daemonset.yaml`**

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: sandbawks-node
  labels: {{- include "sandbawks.labels" . | nindent 4 }}
spec:
  selector:
    matchLabels:
      {{- include "sandbawks.selectorLabels" . | nindent 6 }}
      app.kubernetes.io/component: node
  template:
    metadata:
      labels:
        {{- include "sandbawks.selectorLabels" . | nindent 8 }}
        app.kubernetes.io/component: node
    spec:
      serviceAccountName: sandbawks-node
      hostPID: true   # required for talking to local CRI socket
      containers:
        - name: node
          image: "{{ .Values.images.node.repository }}:{{ .Values.images.node.tag }}"
          imagePullPolicy: {{ .Values.images.node.pullPolicy }}
          args: ["--placeholder"]
          securityContext:
            privileged: true   # CRI checkpoint API requires this
          volumeMounts:
            - name: containerd-sock
              mountPath: /run/containerd/containerd.sock
            - name: snapshot-cache
              mountPath: /var/lib/sandbawks/snapshots
          resources:
            {{- toYaml .Values.node.resources | nindent 12 }}
      volumes:
        - name: containerd-sock
          hostPath:
            path: /run/containerd/containerd.sock
            type: Socket
        - name: snapshot-cache
          hostPath:
            path: {{ .Values.node.hostPathSnapshotCache }}
            type: DirectoryOrCreate
```

- [ ] **Step 10: Copy CRDs into the chart's `crds/` directory**

Helm installs files under `crds/` first, before templates. They must be flat YAML (no templating).

```bash
mkdir -p deploy/helm/sandbawks/crds
cp deploy/crds/*.yaml deploy/helm/sandbawks/crds/
```

- [ ] **Step 11: Run `helm lint`**

```bash
helm lint deploy/helm/sandbawks
```
Expected: `1 chart(s) linted, 0 chart(s) failed`.

- [ ] **Step 12: Run `helm template` to verify rendering**

```bash
helm template test deploy/helm/sandbawks | head -40
```
Expected: rendered YAML for the four CRDs, three SAs, three ClusterRoles + ClusterRoleBindings, two Deployments, one Service, one DaemonSet.

- [ ] **Step 13: Commit**

```bash
git add deploy/helm/sandbawks
git commit -m "$(cat <<'EOF'
feat(deploy): Helm chart scaffold for sandbawks v0.1.0

api Deployment + Service, controller Deployment, node DaemonSet,
ServiceAccounts + ClusterRoles + ClusterRoleBindings, CRDs installed
under crds/. Workload containers run with --placeholder until later
plans land the real binaries.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 11: Workspace-wide green build

**Files:** none new — just verifies everything compiles together.

- [ ] **Step 1: Full workspace test run**

Run: `cargo test --workspace 2>&1 | tail -30`
Expected: all crates compile; ~16 tests pass (smoke 2 + resources 6 + sandbox 4 + sandbox_template 2 + warm_pool 2 + team_api_key 2 + crdgen 1 — adjust if numbers differ slightly, but every test must pass).

- [ ] **Step 2: Clippy clean**

Run: `cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -30`
Expected: no warnings.

- [ ] **Step 3: Format check**

Run: `cargo fmt --all -- --check 2>&1 | tail -10`
Expected: no output (formatted).

If `cargo fmt --all -- --check` fails, run `cargo fmt --all`, commit the result.

- [ ] **Step 4: Doc build**

Run: `cargo doc --workspace --no-deps 2>&1 | tail -10`
Expected: docs built without warnings.

---

## Task 12: GitHub Actions CI

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Create the workflow**

```yaml
name: ci

on:
  push:
    branches: [main]
  pull_request:

jobs:
  rust:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install Rust toolchain
        uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy
      - name: Cache cargo
        uses: actions/cache@v4
        with:
          path: |
            ~/.cargo/registry
            ~/.cargo/git
            target
          key: ${{ runner.os }}-cargo-${{ hashFiles('**/Cargo.lock') }}
      - name: fmt
        run: cargo fmt --all -- --check
      - name: clippy
        run: cargo clippy --workspace --all-targets -- -D warnings
      - name: test
        run: cargo test --workspace --all-targets
      - name: docs
        run: cargo doc --workspace --no-deps

  crd-yaml-up-to-date:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - name: Regenerate CRDs
        run: |
          cargo run -p sandbawks-types --bin crdgen -- /tmp/crds
          diff -ru deploy/crds /tmp/crds

  helm:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: azure/setup-helm@v4
        with:
          version: v3.16.0
      - name: helm lint
        run: helm lint deploy/helm/sandbawks
      - name: helm template
        run: helm template test deploy/helm/sandbawks > /dev/null
      - name: CRDs in chart match deploy/crds
        run: diff -ru deploy/crds deploy/helm/sandbawks/crds
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "$(cat <<'EOF'
ci: rust + helm checks, CRD yaml regen-drift gate

Three jobs: standard fmt/clippy/test/doc; regen CRDs from code and
diff against deploy/crds (catches stale generated yaml); helm lint
and confirm crds/ in the chart matches deploy/crds.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 13: Changelog entry

**Files:**
- Create: `changelog/2026-05-21-plan-01-types-and-crds.md`

- [ ] **Step 1: Write the changelog**

```markdown
# 2026-05-21 — Plan 01: Types crate + CRDs + Helm scaffold

## What landed

- Converted root crate to a Cargo workspace.
- New crate `crates/sandbawks-types`: shared CRD types, error types, resource quantities.
- Four CRDs: `Sandbox`, `SandboxTemplate`, `SandboxWarmPool`, `TeamAPIKey` (group `sandbawks.io/v1alpha1`).
- `crdgen` binary emits CRD YAML to `deploy/crds/`.
- Helm chart scaffold at `deploy/helm/sandbawks/` with placeholder workloads, full RBAC, CRD install.
- CI: fmt + clippy + test + doc, CRD-regen drift check, helm lint.

## What's deliberately not here

- Any runtime behavior. No controller reconcile loop, no API handlers, no node DaemonSet logic.
  Plans 02+ land each.

## Next plan

`docs/superpowers/plans/2026-05-21-sandbawks-02-controller-skeleton.md` —
`sandbawks-controller` with the Sandbox state machine driving a fake
`RuntimeAdapter` (no real pause/resume yet).
```

- [ ] **Step 2: Commit**

```bash
git add changelog/2026-05-21-plan-01-types-and-crds.md
git commit -m "docs(changelog): plan 01 landed"
```

---

## Self-review checklist (run before declaring this plan done)

- [ ] `cargo test --workspace` is green.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` is silent.
- [ ] `cargo fmt --all -- --check` is silent.
- [ ] `cargo run -p sandbawks-types --bin crdgen -- deploy/crds` is idempotent (no diff after rerun).
- [ ] `helm lint deploy/helm/sandbawks` passes.
- [ ] `diff -ru deploy/crds deploy/helm/sandbawks/crds` is empty.
- [ ] `git log --oneline` shows ~12 small focused commits, each runnable.

## What this plan does NOT do (rejected scope)

- No reconcile logic. Controller skeleton is plan 02.
- No HTTP handlers. API skeleton is plan 04.
- No RuntimeAdapter impls. gVisor adapter is plan 05.
- No envd-rs. That's plan 03.
- No real container images. The Helm chart references images that don't exist yet; that's fine for `helm lint` and `helm template`. They'll be built and pushed in later plans.
