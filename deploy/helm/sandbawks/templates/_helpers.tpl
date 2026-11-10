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

{{/*
Resolve the Secret name holding the api gateway's HMAC signing key. If
the operator pinned `.Values.api.signingSecret.existingSecret`, that name
wins; otherwise we fall back to the chart-managed `sandbawks-api-signing`
Secret rendered by `api-signing-secret.yaml`.
*/}}
{{- define "sandbawks.signingSecretName" -}}
{{- if .Values.api.signingSecret.existingSecret -}}
{{ .Values.api.signingSecret.existingSecret }}
{{- else -}}
sandbawks-api-signing
{{- end -}}
{{- end -}}

{{/*
Resolve the Secret name holding the snapshot-store credentials the node
DaemonSet (and the controller's retention sweeper) mounts via `envFrom`.
For `provider: azure` the operator's `.Values.snapshotStore.azure.existingSecret`
wins; for `provider: s3` the operator's `.Values.snapshotStore.existingSecret`
wins. Either way an empty value falls back to the chart-managed
`sandbawks-snapshot-store` Secret rendered by `snapshot-store-secret.yaml`.
*/}}
{{- define "sandbawks.snapshotStoreSecretName" -}}
{{- if eq .Values.snapshotStore.provider "azure" -}}
{{- if .Values.snapshotStore.azure.existingSecret -}}
{{ .Values.snapshotStore.azure.existingSecret }}
{{- else -}}
sandbawks-snapshot-store
{{- end -}}
{{- else -}}
{{- if .Values.snapshotStore.existingSecret -}}
{{ .Values.snapshotStore.existingSecret }}
{{- else -}}
sandbawks-snapshot-store
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Canonical restore staging root (issue #512). Three sites MUST agree on it:
  - the controller's `SANDBAWKS_RESTORE_STAGING_ROOT` env (the host path it
    records in each restore Pod's `io.sandbawks/restore-image-path` annotation),
  - the node DaemonSet's `staging-cache` mountPath AND hostPath (mounted
    `mountPath == hostPath` so the shim resolves the recorded path verbatim),
  - the node binary's `SANDBAWKS_STAGING_ROOT` flag (where it stages, sweeps,
    and confines bundles).
All three resolve through this helper, so they cannot silently diverge — an
operator relocates the staging root by overriding `node.hostPathStagingCache`
alone. Fails the render fast if that value is unset or not absolute (the
host-side shim reads it verbatim off the node filesystem; a relative path
cannot resolve across the worker and the shim).
*/}}
{{- define "sandbawks.stagingRoot" -}}
{{- $root := .Values.node.hostPathStagingCache -}}
{{- if not $root -}}
{{- fail "node.hostPathStagingCache must be set: it is the restore staging root shared by the controller annotation, the node DaemonSet mountPath, and the node binary (issue #512)." -}}
{{- end -}}
{{- if not (hasPrefix "/" $root) -}}
{{- fail (printf "node.hostPathStagingCache must be an absolute path (got %q): the restore shim reads it verbatim off the node filesystem and the node mounts it at mountPath==hostPath (issue #512)." $root) -}}
{{- end -}}
{{- $root -}}
{{- end -}}

{{/*
Stable name of the in-namespace OTel Collector (issue #421) — shared by its
Deployment, Service, ConfigMap, Secret, and the endpoint default below so
they all agree.
*/}}
{{- define "sandbawks.otelCollectorName" -}}
sandbawks-otel-collector
{{- end -}}

{{/*
Cluster-internal OTLP/HTTP base URL for INTERNAL workloads (api, controller,
node) when the chart-shipped collector is enabled and `observability.endpoint`
is left blank. Per-namespace by construction.

When split pipelines are enabled (issue #424), the internal workloads use a
dedicated receiver on port 4320 so the collector can apply separate scrubbing
rules to untrusted sandbox signals arriving on 4318. Without split pipelines,
the single pipeline uses port 4318 (legacy behavior).
*/}}
{{- define "sandbawks.otelCollectorEndpoint" -}}
{{- if .Values.observability.collector.splitPipelines.enabled -}}
http://{{ include "sandbawks.otelCollectorName" . }}.{{ .Release.Namespace }}.svc:4320
{{- else -}}
http://{{ include "sandbawks.otelCollectorName" . }}.{{ .Release.Namespace }}.svc:4318
{{- end -}}
{{- end -}}

{{/*
Cluster-internal OTLP/HTTP base URL for SANDBOX envd (issue #424). Always
points at port 4318, which is the sandbox pipeline receiver when split
pipelines are enabled. The controller emits this as
`SANDBAWKS_OTEL_SANDBOX_ENDPOINT` on its own Deployment so pod_adapter can
redirect sandbox envd to the scrubbed pipeline instead of the internal one.
Only rendered onto the controller Deployment when splitPipelines.enabled.
*/}}
{{- define "sandbawks.otelCollectorSandboxEndpoint" -}}
http://{{ include "sandbawks.otelCollectorName" . }}.{{ .Release.Namespace }}.svc:4318
{{- end -}}

{{/*
Resolve the Secret name holding the downstream (Traceway) OTLP bearer token
the collector forwards with. Operator-pinned
`.Values.observability.collector.exporter.existingTokenSecret` wins; otherwise
the chart-managed Secret rendered by `otel-collector-secret.yaml`.
*/}}
{{- define "sandbawks.otelCollectorTokenSecretName" -}}
{{- if .Values.observability.collector.exporter.existingTokenSecret -}}
{{ .Values.observability.collector.exporter.existingTokenSecret }}
{{- else -}}
{{ include "sandbawks.otelCollectorName" . }}
{{- end -}}
{{- end -}}

{{/*
OpenTelemetry env block shared by every sandbawks server workload (api,
controller, node — issue #294/#295, Phase 7). Emits a list of `env:`
entries with NO leading indentation; each call site controls placement with
`{{- include "sandbawks.observabilityEnv" . | nindent <n> }}`, so the same
block lands correctly under each container's `env:`.

Each binary stamps its own `service.name` at compile time
(`init_telemetry("sandbawks-<tier>", ...)`), so the wiring here is identical
across the three workloads — only `OTEL_RESOURCE_ATTRIBUTES` is shared.

When `observability.enabled`:
  - the standard `OTEL_*` export vars are set (plaintext OTLP only in Phase 1),
  - `POD_NAME` is injected so the binary derives a stable `service.instance.id`
    rather than a fresh random UUID per restart (issue #295).
When DISABLED (the default), only `OTEL_SDK_DISABLED=true` is set: this
short-circuits the SDK to fmt-only stderr logging so the binary never attempts
(and floods warnings about) exports to the default `localhost:4318` endpoint.
*/}}
{{- define "sandbawks.observabilityEnv" -}}
{{ if .Values.observability.enabled -}}
{{- /* When the chart-shipped collector is enabled (issue #421), workloads
       default to it (plaintext, in-namespace) unless the operator pinned an
       explicit endpoint. The controller forwards this same value into every
       sandbox's envd via FORWARDED_OTEL_VARS, so envd follows automatically —
       and never needs the credential the collector holds. */ -}}
{{- $endpoint := .Values.observability.endpoint -}}
{{- if and (not $endpoint) .Values.observability.collector.enabled -}}
{{- $endpoint = include "sandbawks.otelCollectorEndpoint" . -}}
{{- end -}}
# OpenTelemetry export (issue #294/#295). Plaintext OTLP only in Phase 1 —
# `observability.endpoint` must be a plaintext collector / intake. With
# `observability.collector.enabled` it defaults to the in-namespace collector.
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ $endpoint | required "observability.endpoint is required when observability.enabled=true (or set observability.collector.enabled=true to default to the in-namespace collector)" | quote }}
- name: OTEL_EXPORTER_OTLP_PROTOCOL
  value: {{ .Values.observability.protocol | quote }}
- name: OTEL_TRACES_SAMPLER
  value: {{ .Values.observability.sampler | quote }}
- name: OTEL_TRACES_SAMPLER_ARG
  value: {{ .Values.observability.samplerArg | quote }}
{{- with .Values.observability.resourceAttributes }}
- name: OTEL_RESOURCE_ATTRIBUTES
  value: {{ . | quote }}
{{- end }}
{{- with .Values.observability.timeoutMs }}
- name: OTEL_EXPORTER_OTLP_TIMEOUT
  value: {{ . | quote }}
{{- end }}
{{- if .Values.observability.existingHeadersSecret }}
- name: OTEL_EXPORTER_OTLP_HEADERS
  valueFrom:
    secretKeyRef:
      name: {{ .Values.observability.existingHeadersSecret }}
      key: otlp-headers
{{- else if .Values.observability.headers }}
- name: OTEL_EXPORTER_OTLP_HEADERS
  value: {{ .Values.observability.headers | quote }}
{{- end }}
{{- if .Values.observability.captureGenaiContent }}
# GenAI content capture (issue #426). Opt-in: when true, a sandbox's envd
# records the (redacted) command line on its `execute_tool` spans. Set on every
# workload here, but only meaningful for envd — the controller forwards it into
# each sandbox via FORWARDED_OTEL_VARS. PRIVACY: this puts best-effort-redacted
# command lines into your traces; the redactor is a denylist of shapes, not a
# guarantee, and the sandbox is customer-controlled. Leave off unless you accept
# that. Defaults off.
- name: OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT
  value: "true"
{{- end }}
# Stable service.instance.id (issue #295): the binary reads POD_NAME and
# falls back to a random UUID per restart without it, which would churn the
# per-replica identity in dashboards.
- name: POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
{{- else -}}
# Observability disabled: short-circuit the SDK to fmt-only stderr logging so
# the binary never attempts (and warns about) exports to the default
# localhost:4318 endpoint.
- name: OTEL_SDK_DISABLED
  value: "true"
{{- end }}
{{- end -}}
