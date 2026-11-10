# Issue #273 — make `SandboxTemplate.spec.envdImage` optional

Date: 2026-06-02T20:37:15Z
Branch: `chore-issue-273-envd-image-optional`

## Problem

With node-staged envd delivery (issue #265), the cold-start and restore
paths use the cluster-wide envd binary staged by the `sandbawks-node`
DaemonSet (Helm `envd.image`). `SandboxTemplate.spec.envd_image` remained a
**required** CRD field but was **no longer consumed** on those paths. A
template author setting it to a custom value got no error and no effect — a
silent no-op footgun.

## Resolution (Option 1 from the issue)

Made the field non-load-bearing-optional rather than reviving per-template
staging (the larger Option 3, tracked by #270).

### Type change — `crates/sandbawks-types/src/sandbox_template.rs`

- `envd_image: String` -> `envd_image: Option<String>`.
- Added `#[serde(default, skip_serializing_if = "Option::is_none")]` so the
  field is non-required in the generated CRD schema and `None` is omitted
  from serialized YAML (no `envdImage: null`).
- Rewrote the doc comment to state the field is optional, that setting it is
  currently a no-op, and that it is reserved for future per-template staging
  (#270).

### CRD regeneration — `deploy/crds/sandboxtemplates.sandbawks.io.yaml`

- Regenerated via `cargo run -p sandbawks-types --bin crdgen -- deploy/crds`.
- `envdImage` dropped from the `spec.required` set; remaining required
  fields: `defaultResources`, `defaultRuntimeClass`, `defaultTimeoutSeconds`,
  `image`.

### Call sites

- `crates/sandbawks-controller/src/store.rs`: the lenient-mode
  `default_template()` now sets `envd_image: None`.

### Fixtures (dropped from the no-longer-required set)

- `deploy/kind/fixtures/sandbox-smoke.yaml` — removed `envdImage`, updated
  the header comment.
- `.forgejo/workflows/kind-smoke.yaml` — removed `envdImage` from the
  api-smoke template.
- `kustomize/sandbox-template.yaml` — removed `envdImage`, added a comment
  explaining the omission.

### Stale doc fix

- `deploy/helm/sandbawks/values.yaml` — the `envd.image` comment claimed
  "per-template overrides via `SandboxTemplate.spec.envdImage` continue to
  win", which contradicts the current behaviour. Updated to say the
  cluster-wide value is authoritative and the per-template field is a no-op.

### Tests — `crates/sandbawks-types/tests/sandbox_template.rs`

- `yaml_roundtrip` now uses `envd_image: None` and asserts the field does not
  appear in serialized YAML (exercises `skip_serializing_if`).
- New `envd_image_is_optional_in_crd_schema` walks the generated CRD's
  `spec.required` set and asserts `envdImage` is absent while `image` stays
  present — a regression guard that directly encodes the issue's acceptance
  criterion.

## Acceptance

A template author can now tell `envdImage` is non-load-bearing without
reading source: it is optional in the CRD schema and its doc comment / the
Helm values comment say it is a no-op on cold-start and restore.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — all workspace tests pass, including the new CRD-schema guard.
