# Issue #271 — kind+gVisor pause/resume smoke on the injection model

**Date:** 2026-06-02T19:15:00Z
**Branch:** `test-issue-271-kind-gvisor-injection-smoke`
**Issue:** #271 (sub-issue of #265, follow-up to PR #266)

## What #271 set out to retire

PR #266 delivers envd without forcing customers to bake it into their image.
The binary is **not** part of `rootfs-delta.tar.zst`; it arrives on the
restore Pod from a **re-created mount**. #271's "one genuine unknown" was
whether `runsc restore` tolerates the exec'd binary coming from that
re-created mount rather than the checkpointed rootfs — a question only a real
kind+gVisor cluster can answer.

Note the design the issue body was written against (an init container +
shared `emptyDir`) was **superseded during #266** by the node-staged
read-only hostPath model (see
`changelog/2026-06-02T153000Z-issue-265-node-staged-envd.md`): the
`sandbawks-node` DaemonSet's `stage-envd` init container `cp`s the static
envd binary to the node hostPath `/var/lib/sandbawks/envd`, and **every**
sandbox Pod — cold-start and restore, identically — mounts it **read-only**
at `/sandbawks/bin` and runs `/sandbawks/bin/envd` as its entrypoint. The
unknown is the same either way (re-created mount vs. rootfs delta); only the
mechanism changed (node hostPath, not a per-Pod emptyDir). That #265
changelog explicitly deferred the cluster confirmation to this issue.

## What was run

On a local `SANDBAWKS_KIND_GVISOR=1 just cluster-up` cluster (2-node kind,
gVisor `sandbawks-runsc` RuntimeClass, garage S3 backend, node DaemonSet
staging envd):

1. `just pause-resume-smoke` — full `PAUSE_RESUME_SMOKE_FULL=1` round-trip:
   create → write sentinel → pause → assert S3 bundle → **delete the original
   Pod** (forces resume onto a fresh Pod) → resume → assertions → byte-for-byte
   sentinel round-trip → kill → GC.
2. `just bench` with the CI gate config (`BENCH_ITERATIONS=5`,
   `BENCH_PAYLOAD_MIB=16`, `BENCH_INCLUDE_RESUME=1`, `BENCH_RESUME_P95_MS=6500`)
   against the same cluster.

## Result — green

Smoke `SUCCESS` on `sbx-58fd85974dc5`. The resume landed on a fresh Pod and:

- **Shim path** confirmed: `runtimeClassName=sandbawks-runsc` +
  `io.sandbawks/restore-image-path` annotation present.
- **Injection model re-delivered on restore** (the #271 check, now asserted by
  the smoke): the resumed Pod's envd container entrypoint is
  `/sandbawks/bin/envd`, backed by the `sandbawks-envd-bin` hostPath volume
  pointing at `/var/lib/sandbawks/envd`, mounted **read-only**. The binary was
  re-mounted from the node, not restored from the rootfs delta.
- **`runsc restore` tolerated it**: the restored pod sandbox + envd container
  reported Ready, and the `/tmp/sentinel.txt` round-trip matched byte-for-byte
  (sha256 `267f5235…74c1b5`) — i.e. envd resumed from the memory image and exec
  (its file-IO RPC surface) still works post-resume.

Bench (5 iterations, 16 MiB payload, eager memory-only restore):

| phase  | p50    | p95    | max    | gate    | verdict |
|--------|--------|--------|--------|---------|---------|
| pause  | 1015ms | 1174ms | 1207ms | 8000ms  | PASSED  |
| resume | 5285ms | 5696ms | 5726ms | 6500ms  | PASSED  |

`bench: perf gate PASSED`. Resume p95 5696 ms sits under the 6500 ms eager-restore
tripwire (Plan 07; the spec §13 2 s target assumes Plan 08 lazy restore), so the
acceptance "stays within the existing resume p95 bench gate" holds.

## Code change

`deploy/kind/fixtures/pause-resume-smoke.sh`: the `assert-shim-path` phase now
also asserts the **resumed** Pod re-injects the node-staged envd — its envd
`command` array execs `/sandbawks/bin/envd`, `sandbawks-envd-bin` hostPath =
`/var/lib/sandbawks/envd`, read-only mount. The three fields are read from a
single `kubectl get pod -o json` via `jq` (a hard dependency of the script
already), and the entrypoint check matches the whole `command` array rather
than `command[0]` so a future `sh -c` wrapper still localizes "node-staged
envd is not the entrypoint" instead of a confusing positional string diff.
Previously the smoke proved the shim restore path
(runtimeClass + restore-image-path) but never that the envd binary was
re-delivered via the re-created mount on the restore Pod — exactly #271's
"the restore Pod re-runs the inject" criterion. A mismatch now fails the smoke
with a `#271 regression` message that points at #270 (digest pinning) / a
mount-placement fix, instead of surfacing later as a generic restore failure.

## Regressions

None. No mount-placement fix or #270 digest work is required — `runsc restore`
reconnects the node-provided read-only file cleanly. The "genuine unknown" is
retired: the node-staged injection model round-trips pause→resume on kind+gVisor.

## Validation

- `just fmt` / `cargo clippy --all-targets -- -D warnings` / `just test` — all green.
- `bash -n deploy/kind/fixtures/pause-resume-smoke.sh` — syntax clean.
- `just pause-resume-smoke` + `just bench` (CI gate config) on kind+gVisor — both green (above).
