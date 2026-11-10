# node: gate envd-staged on node.status.runtimeHandlers, not a runsc binary

Issue: #417 (supersedes the closed PR #440)

## Symptom

Warm-pool sandbox Pods were scheduled onto nodes whose containerd had no
`runsc` runtime handler registered, looping forever on:

```
FailedCreatePodSandBox: no runtime for "runsc" is configured
```

## Root cause

The `RuntimeClass: gvisor` resource (handler `runsc`) only tells the scheduler
which runtime to *request*; it does not verify containerd has the handler
registered on a node. Sandbox scheduling is gated only by the
`sandbawks.dev/envd-staged` label, which the `sandbawks-node` DaemonSet
stamped after checking only the staged `envd` binary — never the runtime
handler.

## Why the first attempt (PR #440) was wrong

PR #440 added a probe for a `runsc` *binary* on the node container's `$PATH`.
That is a no-op by default: `crates/sandbawks-node/Dockerfile:186` bakes
`runsc` into the DaemonSet image (`COPY --from=runsc /usr/local/bin/runsc ...`)
for the node's own `runsc checkpoint`/`restore`, so the probe resolved that
baked-in binary on every node regardless of host containerd state. A binary on
the node's filesystem is independent of whether the **host** containerd CRI
plugin has the `io.containerd.runsc.v1` handler registered — which is what
actually produces the error. (The branch was also cut from a stale base, so it
spuriously "reverted" #432/#433/#434.)

## Correct signal

The kubelet already reports each node's registered CRI runtime handlers in
`Node.status.runtimeHandlers` — the same ground truth the scheduler sees.
Verified on the live cluster (k3s v1.32.3):

```
k3s-...-server-01   →  runc, runhcs-wcow-process          ← no runsc (the failure node)
k8s-libvirt-...     →  runc, runsc, sandbawks-runsc        ← gVisor-provisioned
```

## Change

The `sandbawks-node` DaemonSet now gates the `envd-staged` label on its own
`Node.status.runtimeHandlers` containing the gVisor handler (`runsc`).

### `crates/sandbawks-node/src/node_label.rs`

- `runtime_handler_gate(node, required) -> RuntimeHandlerGate` (pure
  classifier) with three outcomes:
  - `Present` — handler listed → node can host gVisor sandboxes.
  - `Absent` — non-empty list without the handler → positive evidence it's
    missing → withdraw the label.
  - `Unknown` — no handler list reported (older CRI/kubelet) → do **not**
    block; graceful degradation to pre-#417 behavior rather than stranding
    every node on such a cluster.
- `fetch_runtime_handler_gate` does the Node `get` (one read per reconcile
  tick; the node ClusterRole already grants `get` on `nodes`).
- New `AdvertiseOutcome::WithdrawnRunscAbsent` (non-sticky, unlike the #379
  `WithdrawnRunscRoot`) and `DEFAULT_GVISOR_RUNTIME_HANDLER = "runsc"`.
- `advertise_envd_staged` now returns `AdvertiseOutcome`. Crucially, a missing
  handler at boot does **not** error/CrashLoop (restarting can't install
  runsc) — it withdraws the label and returns `WithdrawnRunscAbsent`; the
  reconcile loop re-asserts automatically once the handler appears. A missing
  *envd binary* still fails closed by erroring (restart re-runs `stage-envd`).
- Reconcile gate priority: sticky `runsc_root_failed` (#379) → handler present
  (#417) → envd binary present (#282).
- The old `AdvertiseError::Patch` variant is renamed `NodeApi` since the kube
  error channel now covers the `get` as well as the patch.

### `crates/sandbawks-node/src/envd_advertised_metrics.rs`

- New `ReconcileResult::RunscAbsent` → `result="runsc_absent"` on the
  `envd_advertised.total` counter. Also refreshed the stale module doc table
  (it predated `runsc_root_failed`).

### `crates/sandbawks-node/src/bin/sandbawks-node.rs`

- New `--gvisor-runtime-handler` / `SANDBAWKS_GVISOR_RUNTIME_HANDLER` arg
  (default `runsc`), threaded into the boot advertise and the reconcile loop.
  The boot caller logs the outcome explicitly (advertised vs. withheld).

### Helm

- `values.yaml` (`gvisor.enabled`) and `runtimeclass-gvisor.yaml`: document
  that the RuntimeClass does not install the runtime, that nodes must be
  pre-provisioned per `ci/kind-node/Dockerfile`, and that the DaemonSet now
  gates scheduling on `status.runtimeHandlers` automatically.

## Out of scope (per issue)

- DaemonSet-level auto-install of `runsc` on non-kind nodes.
- A separate `runsc-ready` label distinct from `envd-staged`.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — full workspace suite green (exit 0).
- Unit tests: `runtime_handler_gate` Present / Absent / Unknown(no-status,
  empty-list).
- Wire tests (`tests/node_label.rs`), GET-then-PATCH per the new read:
  - `advertise_withholds_label_when_runtime_handler_absent` — boot returns
    `WithdrawnRunscAbsent`, null patch, no error.
  - `reconcile_withdraws_label_when_runtime_handler_absent` — `WithdrawnRunscAbsent`.
  - `reconcile_advertises_when_runtime_handlers_unreported` — Unknown → advertise.
  - `reconcile_withdraws_label_when_runsc_root_failed_is_set` — sticky #379 gate
    short-circuits before the GET (no read issued).
  - existing #282/#272 paths updated to answer the GET.
- `helm template --set gvisor.enabled=true` renders cleanly.
- `--gvisor-runtime-handler` present in `--help` with the env var and `runsc`
  default.
- Signal confirmed against the live k3s cluster: `server-01` reports no
  `runsc` handler (would be gated out); libvirt gVisor nodes report it.
