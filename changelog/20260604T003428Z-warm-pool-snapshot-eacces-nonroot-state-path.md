# Warm-pool Pods crash on auto-pause: snapshot flush `EACCES` under non-root `USER`

- **Date:** 2026-06-04T00:34:28Z
- **Area:** `crates/envd` (default `--state-file`), `crates/sandbawks-controller` (pod-adapter comment)
- **Kind:** Bug fix

## Symptom

Warm-pool Pods (`sbx-warm-*`) terminated with exit code 1 immediately after
boot. Pod logs ended with:

```
ERROR envd: envd shutting down with error error=creating snapshot parent directory /var/lib/envd
Error: creating snapshot parent directory /var/lib/envd
Caused by:
    Permission denied (os error 13)
```

The Sandbox then landed in `Failed`; because `Failed` members are dropped
from the warm pool's ready/in-flight buckets, the reconciler cold-booted
replacements forever and the pool never filled.

## Root cause

The warm-pool reconciler auto-pauses freshly-booted members
(`spec.desiredPhase = Paused`). Pause delivers `SIGTERM` to envd, whose
shutdown handler flushes its pause/resume snapshot via `write_snapshot`,
which calls `create_dir_all` on the parent of `--state-file`.

The default `--state-file` was `/var/lib/envd/state.json`. The workload
container runs envd as the image's declared `USER`, which for
`ghcr.io/hypotheosis/compound-sandbox:latest` is `dev` (confirmed via
`skopeo inspect`: `"User": "dev"`). `/var/lib` is owned by `root:root`
mode `0755`, so a non-root process cannot create `/var/lib/envd` —
`create_dir_all` fails with `EACCES (os error 13)`.

The Pod's `securityContext` is `{}`, so nothing overrides the image `USER`.
The design assumption baked into `pod_adapter.rs` ("envd creates
`/var/lib/envd/state.json` on the overlay rootfs, so no writable mount is
needed") silently required envd to run as **root** — which does not hold
for any image with a non-root `USER`. Warm-pool is simply the path that
exposes it deterministically, because its whole purpose is
pause-immediately-after-boot.

The two 30s drain timeouts and the OTel `HTTP export failed` warnings in
the same log are unrelated: the drains are graceful-shutdown doing its job,
and the OTel warnings are an unreachable collector endpoint. The non-zero
exit comes solely from the snapshot `EACCES`.

## Fix

Changed envd's default `--state-file` from `/var/lib/envd/state.json` to
`/var/tmp/envd/state.json`.

`/var/tmp` is a sticky, world-writable directory (mode `1777`) present in
effectively every image, so envd can create its `envd/` snapshot dir
regardless of the UID it runs as. Crucially, `/var/tmp` is a plain
directory on the container's overlay rootfs — captured by the
`runsc checkpoint` rootfs-delta exactly as `/var/lib/envd` was — so the
pause/restore path is unchanged; only the permission constraint is lifted.
`/var/tmp` was chosen over `/tmp` because some runtimes back `/tmp` with an
internal tmpfs, whereas `/var/tmp` always behaves like the previous
overlay-dir path.

Both the on-shutdown write and the boot-time `load_boot_seed` read use the
same default, so the change is internally symmetric. No code outside
`envd.rs` hardcodes the path: the node captures envd state via the HTTP
`/__state` probe (stored as the opaque `envd-state.json` snapshot part),
independent of the in-container file path.

### Changed

- `crates/envd/src/bin/envd.rs`
  - `DEFAULT_STATE_FILE` → `/var/tmp/envd/state.json`, with an expanded
    doc comment explaining the non-root rationale.
  - Updated stale `/var/lib/envd` references in the `load_boot_seed` and
    `write_snapshot` doc comments.
  - New regression test
    `default_state_file_nests_under_world_writable_temp_root` pinning the
    default under a world-writable temp root.
- `crates/sandbawks-controller/src/pod_adapter.rs`
  - Updated the envd-delivery comment to reflect the new path and document
    why a non-root `USER` forces a world-writable state location.

## Verification

- `cargo test -p envd` — all suites pass (unit, `binary_smoke`,
  `lifecycle`, `state`, `fingerprint`, property tests).
- `cargo test -p sandbawks-controller` — all suites pass (`pod_adapter`,
  `warm_pool`, state machine).
- `cargo clippy -p envd -p sandbawks-controller --all-targets` — clean.
- `cargo fmt` — applied.

## Operational note

Existing warm Sandboxes already in `Failed` will not self-heal; once a node
restages the rebuilt envd binary (the DaemonSet `stage-envd` step) and the
pool reconciles, new warm members will pause and persist cleanly. No CRD or
manifest change is required — the fix ships in the envd binary.
