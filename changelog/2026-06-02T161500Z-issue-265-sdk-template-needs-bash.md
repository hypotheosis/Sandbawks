# Issue #265 follow-up — SDK-compat template needs /bin/bash

**Date:** 2026-06-02T16:15:00Z
**Branch:** `fix-issue-265-coldstart-template-image`
**Issue:** #265 / PR #266

## Symptom

The SDK compat smoke (Plan 06 T2) failed: every `commands.run` test returned
`Code.internal: an internal error occurred` (HTTP 500 from envd's process
Start RPC). File-ops and lifecycle tests passed; `test_pause_resume_exec_roundtrip`
got all the way through `files.write → pause → connect → files.read` and only
failed on the trailing `commands.run` — confirming envd runs and **pause/resume
works on the node-staged shape**.

## Root cause

This is correct fallout from the #265 fix. The e2b SDK wraps every
`commands.run` as `cmd="/bin/bash", args=["-l","-c", …]` (visible in the
traceback). The SDK-compat template (`api-smoke-template`, seeded by
`deploy/kind/fixtures/api-smoke.sh`) used `docker.io/library/busybox:1.36` as a
**placeholder** `spec.image` — which was ignored before #265 (cold start always
ran the python-based envd image, which has bash). Now that cold start honors
`spec.image`, the container rootfs is busybox, which has only `/bin/sh` — so
`exec("/bin/bash")` fails with ENOENT and envd returns 500.

## Fix

Point the SDK-compat template at a workload image that ships `/bin/bash`:

- `deploy/kind/fixtures/api-smoke.sh`: `spec.image` →
  `docker.io/library/debian:12-slim` (minimal Debian base with bash +
  coreutils). envd is unaffected — it is node-staged and mounted in read-only,
  not part of this image.

## Scope

Only the SDK-driven template needs bash. Left on busybox (legitimately
exercising the minimal-image path — envd runs, file ops work, no shell exec):

- `deploy/kind/fixtures/sandbox-smoke.yaml` — controller phase-transition smoke.
- `deploy/kind/fixtures/pause-resume-smoke.sh` — writes/reads a sentinel file
  across pause/resume; no shell exec.
- `.forgejo/workflows/kind-smoke.yaml` bench template — uploads payload files to
  fill RSS; no shell exec (see `tests/bench/src/main.rs` workload notes).

## Note

Any real customer template used with the e2b SDK's `commands.run` must provide
`/bin/bash` — this is an inherent e2b SDK contract, surfaced now that
`spec.image` is honored.
