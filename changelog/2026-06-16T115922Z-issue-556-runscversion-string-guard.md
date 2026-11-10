# Issue #556 — fail-fast guard: node.runscVersion must be a quoted string

**Date:** 2026-06-16
**Branch:** `fix-issue-556-runscversion-string-guard`
**Scope:** Helm-only hardening of the node chart against a silent
values-file footgun surfaced while validating the `just runsc-versions`
recipe (PR #555).

## Symptom

A deployed `sandbawks-node` DaemonSet rendered `SANDBAWKS_RUNSC_VERSION` as
`"2.0260608e+07"` — a float in scientific notation. That value never equals a
`release-<ver>` token, so the #546 reconciler and #550 upgrader silently treat
every node as drifted, and the `just runsc-versions` drift check auto-detects
garbage as the desired version.

## Root cause

`node.runscVersion` is a `YYYYMMDD.N` **string**, rendered with
`{{ .Values.node.runscVersion | quote }}`. Set **unquoted** in a values file,
YAML parses it to a float64, and `| quote` then emits Go's float formatting.
Verified with `helm template`:

| values.yaml | rendered |
|---|---|
| `runscVersion: 20260608.0` | `"2.0260608e+07"` |
| `runscVersion: 20260608` | `"2.0260608e+07"` |
| `runscVersion: "20260608.0"` | `"20260608.0"` |

(`--set` is unaffected — only values files hit the YAML float coercion.)

## Fix

The original string can't be reliably recovered from a coerced float (`int64`
drops the `.N` suffix), so coercing it back is wrong — **fail the render**. Added
to `templates/node-daemonset.yaml`, gated to when the value is actually consumed
(reuses the existing `$runscHostBinNeeded` = `runscReconcile.enabled` OR
`runscUpgrade.enabled`):

```gotemplate
{{- if and $runscHostBinNeeded (not (kindIs "string" .Values.node.runscVersion)) }}
{{- fail "node.runscVersion must be a quoted string (e.g. \"20260525.0\") — an unquoted YAML number is coerced to a float and renders as e.g. 2.0260608e+07 in SANDBAWKS_RUNSC_VERSION, breaking the release-<ver> match" }}
{{- end }}
```

Also added a "MUST stay QUOTED" note next to `runscVersion` in `values.yaml`.
Matches the chart's existing fail-fast guards (hostPath absolutes, leader
election, the #550 mutual-exclusion guards).

## Validation (`helm template` / `helm lint`)

- `helm lint` — clean.
- Unquoted float **and** unquoted int values file → render **fails** with the
  actionable message. ✅
- Default / quoted-in-file → renders `SANDBAWKS_RUNSC_VERSION` verbatim
  (`"20260525.0"` / `"20260608.0"`). ✅
- Unquoted float with `runscReconcile.enabled=false` + `runscUpgrade.enabled=false`
  (value unused) → does **not** fail. ✅
- `Chart.yaml` `0.1.14` → `0.1.15`.

Helm-only; no Rust/CRD change.
