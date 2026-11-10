# Kata pause/resume fails closed — remove the destructive CH-direct path (epic #594)

**Date:** 2026-06-19T12:47:14Z
**Issue:** #594
**Branch:** `fix/kata-pause-fail-closed`

## Why

PR #595 shipped a `KataAdapter` that paused/resumed a Kata sandbox by driving
its Cloud Hypervisor VMM directly over the API socket
(`vm.pause`/`vm.snapshot`/`vm.restore`). A live viability trial on AKS
(cloud-hypervisor `v51.1.0` on MSHV) disproved the approach:

- ✅ `vm.snapshot` **does** produce a valid bundle — `config.json` + `state.json`
  + `memory-ranges` (~1.35 GB guest RAM). The capture mechanism is real.
- ❌ **Driving it out-of-band — behind the `containerd-shim-kata-v2` that owns
  the VM lifecycle — wedged and destroyed the running VM.** After `vm.pause` +
  `vm.snapshot` the CH API went unresponsive, `vm.resume` failed, the
  `/run/vc/vm/<id>/` dir was torn down, the pod died, and the sandbox folded to
  `Failed`.
- ❌ `vm.restore` needs a *fresh* cloud-hypervisor process with no booted VM to
  restore into, but the kata shim always boots the VM itself — there is no
  restore-ready VMM to target without shim cooperation.

So the CH-direct approach cannot work cleanly regardless of fixing its
socket-id/mount plumbing. Leaving the code in place is a latent hazard: a later
"fix" of its socket resolution (it currently fails early because it keyed the
socket on the pod UID instead of the CRI sandbox id) would *enable* the
VM-destroying path.

## Change

Replace the ~600-line CH-direct `KataAdapter` (CH HTTP client, socket locator,
snapshot-bundle orchestration) with a minimal **fail-closed** adapter:

- `pause` / `resume` return `RuntimeError::Unsupported` with a clear message
  pointing at #594. The controller treats a failed pause as fail-closed and
  leaves the workload Pod running — no snapshot is attempted, so a Kata sandbox
  is never destroyed.
- `stage_snapshot` / `validate_restorable` keep their trait defaults
  (`Unsupported` / no-op) — never reached, since a Kata sandbox is never paused.
- Node binary: `build_kata_adapter` and the `--kata-run-root` /
  `--kata-api-socket-name` flags are removed; the `kata`/`kata-fc` arm now
  constructs `KataAdapter::new()`.

Net `-741/+87` lines. No chart/CRD change — `node.runtime: kata` (from #600)
still selects this adapter and still sets the `kata` staging-gate handler, so
Kata **cold start → Running** is unchanged.

## Scope

This is a safety fix: it makes Kata pause **impossible to attempt destructively**
and keeps Kata working for cold-start / Running workloads. It does **not** add
working Kata pause/resume — that needs a **shim-mediated** mechanism (Kata's
experimental `pause_with_snapshot`/`resume_from_snapshot`, absent from stock AKS
Kata), tracked as the path decision in #594.

## Tests

- New: `pause_fails_closed_without_touching_storage` (asserts `Unsupported` AND
  that the snapshot store stays empty — no destructive capture),
  `resume_fails_closed`, `runtime_class_defaults_and_overrides`.
- `cargo fmt --check`, `cargo clippy -p sandbawks-node --all-targets -D
  warnings`, `cargo test -p sandbawks-node` — all green.
