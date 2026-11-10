# Issue #558 (option 1) — take over a converged upgrade-lease holder instead of waiting out its TTL

**Date:** 2026-06-16
**Branch:** `fix-issue-558-converged-holder-takeover`
**Scope:** `sandbawks-node` only — the #550 fleet upgrader's lease handoff.

## Problem

The #550 fleet upgrade *worked* (installs succeed, nodes converge) but crawled at
the lease-TTL cadence — ~1 node per `leaseDurationSeconds` (default 600s). Root
cause: the design assumed the installer's k3s restart kills the DaemonSet Pod and
a **new** Pod releases the lease on convergence. **On k3s the Pod is not killed** —
the embedded containerd + shims keep the container running and the kubelet just
reconnects. So that release path never fires; release falls to the *surviving*
Pod's loop, whose post-restart apiserver call can wedge, pinning the lease until
its TTL. The whole rollout then advances only at TTL expiry.

(Diagnosed live: a holder converged its host runsc in ~23s at 12:02, but the lease
only handed off at 12:11:57 — 9s after its 600s TTL expired.)

## Fix (option 1 of #558)

Let **any** node take over a lease whose current holder has **already converged**,
without waiting for the TTL. Convergence is read from the holder Node's
`sandbawks.dev/runsc-version` label (stamped by the #546 reconciler): label ==
desired ⇒ the holder's install is done ⇒ the lease is takeable. The rollout then
advances at the label-update cadence regardless of whether the disrupted holder
ever releases.

- `decide_lock` gains a `holder_converged: bool` parameter; the
  `holder != self && !expired` branch now takes over when `expired || holder_converged`
  (a takeover, so it bumps the transition counter). The self/expiry/empty paths are
  unchanged, and the self branch is checked first so the flag can't affect our own
  lease.
- `KubeLeaseUpgradeCoordinator` gains a cluster-scoped `Api<Node>` and the desired
  `release-<ver>` token; `acquire()` does one extra Node GET **only when another
  node holds the lease**, comparing that node's label to desired. A missing holder
  Node or a non-matching/absent label ⇒ `false` (fall back to the TTL/expiry path).
- The holder's own `AlreadyConverged → release()` fast path is unchanged — it stays
  the prompt path when it works, with convergence-takeover and then the TTL as the
  backstops.
- Module docs (rail #3) corrected: documents the k3s pod-survival reality and the
  takeover mechanism, replacing the false "the new Pod releases the lease" claim.

### Safety / concurrency bound

The takeover preserves the rail's real invariant — **never a fleet-wide
simultaneous k3s restart** — but it does **not** guarantee zero overlap between two
nodes, and an earlier draft of this note overstated that.

The #546 reconciler stamps the label from the host runsc *binary* version, and
`install-runsc-k3s.sh` installs the binary **before** its k3s restarts (then runs
up to three `restart_k3s` passes, each polling `crictl` for up to 30 s). So a #546
tick on the holder can flip the label while the holder is still bouncing k3s, after
which a taker may start its own install concurrently. The overlap is therefore
bounded by the holder's **residual restart sequence (tens of seconds)**, not
"sub-second," and to **two** nodes — never the fleet.

It is acceptable because (a) it is at most two nodes, not the fleet-wide restart the
rail prevents; and (b) both the holder and the taker pass rail 2's drained-node gate
(`LiveSandboxCounter`, checked before `acquire()` each tick), so neither has a live
sandbox a concurrent restart could disrupt. Fully eliminating the overlap would need
a "holder fully done" signal stronger than the binary-version label — deferred to
the #558 follow-ups (options 2-4).

## Out of scope (the rest of #558)

Options 2–4 (apiserver-call timeouts so the holder's own release can't wedge; a
fast post-launch re-check cadence; re-baselining the default TTL) are follow-ups.
Option 1 alone fixes the user-visible TTL-paced crawl.

## Validation

- New unit tests: `decide_lock` takes over a converged-but-valid holder (bumps
  transitions), and the self branch ignores the flag.
- New wire tests: `acquire` does GET-lease → GET-holder-Node → PUT-takeover when the
  holder is converged, and GET-lease → GET-holder-Node → no-write (Contended) when
  it isn't.
- `just fmt`, `cargo clippy --workspace --all-targets -- -D warnings`, `just test`
  (66 binaries) — all green. No Helm/CRD change.
