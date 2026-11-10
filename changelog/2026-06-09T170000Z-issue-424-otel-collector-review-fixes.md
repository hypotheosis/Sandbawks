# OTel Collector trust-boundary — review fixes (issue #424 / PR #439)

Date: 2026-06-09T17:00:00Z

Addresses the review comments on PR #439 (self-review #188). All changes are
hardening/robustness refinements on top of the already-landed feature; runtime
behavior with no values overrides is unchanged.

## Changes

### `deploy/helm/sandbawks/templates/otel-collector-configmap.yaml`

- **OTTL nil guard (comment #12221).** `transform/sandbox_service_name` now uses
  a two-statement form: prefix the reported `service.name` when present, else
  stamp `<prefix>unknown`. A customer-controlled envd that never sets
  `service.name` previously hit `Concat` against a nil OTTL value, whose
  behavior is contrib-version-dependent (silent `<prefix>` vs per-signal
  evaluation error). The `where` clauses make the outcome explicit.
- **Prefix validation (comment #12222).** `sandboxServiceNamePrefix` is now
  validated against `^[A-Za-z0-9._/-]+$` with a Helm `fail` when split pipelines
  are enabled. The prefix is spliced unescaped into an OTTL string literal; a
  value containing a quote, backslash, or bracket would break the OTTL parse and
  surrounding YAML, leaving the collector unable to start. The footgun is now a
  `helm install`-time error.

### `deploy/helm/sandbawks/templates/otel-collector-rbac.yaml`

- **Drop `namespaces` verb (comment #12224).** The Role granted `pods` and
  `namespaces`, but the configured `k8sattributes` extractors (`k8s.pod.name`,
  `k8s.pod.uid`, `k8s.namespace.name`) all derive from the Pod object. A
  `namespaces` watch is only needed for namespace-level labels/annotations,
  which this config does not extract. Dropped to `pods` only — least privilege,
  and now matches the values.yaml comment.

### `deploy/helm/sandbawks/templates/NOTES.txt` (new)

- **NetworkPolicy-without-split warning (comment #12223).** Emits a post-install
  warning when `networkPolicy.enabled=true` while `splitPipelines.enabled=false`:
  the policy restricts collector ingress but does NOT wire in the
  `service.name` scrubber, so a sandbox can still spoof `service.name`. Prevents
  the "false sense of hardening" misconfiguration.

### `deploy/helm/sandbawks/values.yaml`

- **`pod_association: from: connection` assumption (comment #12220).** Documented
  that k8sattributes keys pod identity on the sender's source IP, which breaks
  if anything re-NATs between sandbox and collector (service-mesh sidecar,
  egress gateway, Ingress fronting the collector).

### `crates/sandbawks-controller/src/pod_adapter.rs`

- **Negative-case test (comment #12218).** `sandbox_override_alone_forwards_as_endpoint`
  pins the `or_else` precedence: when `SANDBAWKS_OTEL_SANDBOX_ENDPOINT` is set
  but the controller's own `OTEL_EXPORTER_OTLP_ENDPOINT` is unset, the override
  alone is forwarded as the endpoint (and the override var never leaks verbatim).

## Left as-is

- **Lift override decision out of `filter_map` closure (comment #12219).** The
  reviewer flagged this as minor ("the current form is readable enough"). The
  `clone()` is one short `String` on the single matching iteration; lifting it
  would add a binding without a measurable win. No change.
