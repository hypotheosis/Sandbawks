# envd OpenTelemetry endpoint forwarding

**Date:** 2026-06-05T01:25:07Z
**Crate:** `sandbawks-controller`
**File:** `crates/sandbawks-controller/src/pod_adapter.rs`

## Symptom

`envd` logged a nonsensical OTLP endpoint at startup:

```
envd  INFO sandbawks_observability::init: OpenTelemetry initialised
      service_name=envd service_version="0.1.0" protocol="http/protobuf"
      endpoint=<default http://localhost:4318> otel_resource_attributes=
```

`http://localhost:4318` is the OpenTelemetry SDK's built-in default — unreachable
from inside a sandbox, and it triggers a flood of export-retry warnings.

## Root cause

The `api`, `controller`, and `node` workloads receive their OTLP configuration
from the shared `sandbawks.observabilityEnv` Helm helper, which injects
`OTEL_EXPORTER_OTLP_ENDPOINT` (etc.) — or `OTEL_SDK_DISABLED=true` when
observability is disabled — into each container's `env:`.

`envd` is different: it does not run as a Helm-templated workload. The
controller's `PodAdapter::pod_spec_for` builds the sandbox Pod and was passing
**only** `sandbox.spec.env` to the envd container. envd therefore inherited none
of the `OTEL_*` vars, and the SDK fell back to `localhost:4318`.

## Fix

`PodAdapter::pod_spec_for` now forwards the controller's own observability
configuration into the envd container. The controller process already carries
these vars (the same Helm helper is included in `controller-deployment.yaml`), so
this is zero-config: envd exports to whatever collector the controller does, and
honors the same `OTEL_SDK_DISABLED` kill-switch.

- New const `FORWARDED_OTEL_VARS` (7 keys) + pure helper
  `forwarded_observability_env(lookup)` reads the controller's environment and
  builds the matching `env:` entries, preserving declaration order. The lookup is
  a parameter so the helper is unit-testable without mutating the process env.
- `OTEL_SDK_DISABLED` is listed first so the disabled-by-default case (controller
  has only that var) still forwards cleanly and envd short-circuits to fmt-only
  logging instead of probing `localhost:4318`.
- **Security:** `OTEL_EXPORTER_OTLP_HEADERS` is deliberately **not** forwarded.
  It can carry the collector's auth token (Helm sources it from a Secret), and
  the sandbox Pod is customer-controlled — forwarding it would leak a credential
  into an untrusted environment. The recommended topology keeps the token on an
  in-cluster collector and points workloads at its plaintext intake, so envd
  needs only the endpoint.
- `POD_NAME` is injected via the downward API (`fieldRef: metadata.name`) on the
  envd container rather than forwarded, so `service.instance.id` is the *sandbox*
  Pod's name, not the controller's (issue #295 parity).
- Customer-supplied `sandbox.spec.env` wins on key collisions: the controller
  fills the gap but never clobbers explicit sandbox config.

On a **restore** Pod these spec entries are inert for the already-checkpointed
process (it keeps the env it had at checkpoint time); they are applied uniformly
to keep the cold-start and restore Pod shapes identical.

## Verification

- `cargo fmt --check` — clean
- `cargo clippy -p sandbawks-controller --all-targets -- -D warnings` — clean
- `cargo test -p sandbawks-controller` — all pass, including:
  - 3 new unit tests: `forwards_only_present_vars_in_declaration_order`,
    `forwards_sdk_disabled_kill_switch`, `never_forwards_otlp_headers_credential`
  - existing 13 `tests/pod_adapter.rs` integration tests unchanged
