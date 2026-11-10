# Handoff: Epic #603 — PVC-backed filesystem persistence + "filesystem" suspend mode

_Context dump for resuming work on epic #603 and its sub-issues. Written 2026-06-19._

## TL;DR

Bringing up the AKS Kata pool exposed that sandbawks pause/resume is gVisor/`runsc`-only and **does not work on Kata**. A live trial proved the Cloud-Hypervisor-direct approach is a dead end (it destroys the VM). The pragmatic answer — chosen with the user — is a new **runtime-agnostic `filesystem` persistence mode**: pause = delete Pod / keep PVC, resume = cold-boot a fresh Pod that re-mounts the same PVC. State survives on PVCs, not in memory. Modeled on `kubernetes-sigs/agent-sandbox`. Tracked as epic **#603**, decomposed into **#604–#608**.

**Start implementation at #604** (CRD foundation). It unlocks the chain.

## Current state of Kata on AKS

- ✅ Kata sandboxes **cold-start and run** end to end (proven: a sandbox ran agentic work to completion).
- ✅ Kata pause/resume is **safely fail-closed** (PR #602 — see below). It cannot destroy a sandbox.
- ❌ Kata has **no working pause/resume** yet. #603 delivers it via `filesystem` mode.
- gVisor clusters are unaffected throughout; all changes default to current behavior.

## How we got here — PRs/issues (numbers from the pre-release tracker)

Merged (squash):
- **#593** — `SandboxTemplate` tolerations/nodeSelector (sandbox-Pod scheduling).
- **#595** — node-side Kata support: chart plumbing + the (now-dead) CH-direct `KataAdapter`.
- **#596** — cold-start fix: gate the gVisor envd capture-relocation env on gVisor (`mkdir /sandbawks/run` EACCES'd on Kata's non-root rootfs).
- **#600** — single `node.runtime: kata` chart knob (selects adapter + defaults the staging handler).

Open:
- **PR #602** — makes Kata pause/resume **fail-closed**, removes the destructive CH-direct code. **Should be merged** — it's the safety net #603 Phase 4 (#607) explicitly builds on, and removes a live hazard. Independent of the epic timeline.
- **#594** — epic: node-side Kata; holds the pause/resume "path decision" (shim-mediated vs filesystem-only). #603 is effectively the answer.
- **#597** — api bug: one malformed `TeamAPIKey` 502s ALL auth (whole-list deser is fail-closed). Separate, p2.
- Not filed: controller appends duplicate `Failed` conditions every reconcile (~70 identical) instead of deduping by `type` — cosmetic.

## Why `filesystem` mode (the validated findings behind #603)

- **CH-direct snapshot is a dead end.** Live trial (cloud-hypervisor v51 on MSHV): `vm.snapshot` *works* (produces `config.json`+`state.json`+`memory-ranges`, ~1.35 GB), but driving it out-of-band behind the `containerd-shim-kata-v2` **destroyed the VM** (API went unresponsive, `vm.resume` failed, run dir torn down, sandbox → Failed). `vm.restore` needs a fresh VMM the shim won't provide. So CH-direct can't work without shim cooperation (the experimental upstream `pause_with_snapshot` path, absent from stock AKS Kata).
- **Most use-cases don't need live-memory suspend** — they need files (workspace/checkout/outputs) to survive + low idle cost. `agent-sandbox` does exactly this with Pod-delete + cold-boot + Sandbox-owned PVCs. Runtime-agnostic; works on Kata, gVisor, runc.

## Decisions locked (with the user)

1. **Runtime-agnostic** — `filesystem` mode works on all runtimes; it's Kata's only pause mechanism and an opt-in alternative on gVisor.
2. **Configured on `SandboxTemplate`, overridable per-sandbox.** New `persistenceMode` (`snapshot` default | `filesystem`). Default `snapshot` ⇒ current behavior unchanged.
3. `snapshot` mode on Kata is **rejected at admission** (#607); #602's fail-closed adapter stays as the node-side backstop.

## The epic: #603 → sub-issues

Dependency graph: `#604 → #605 → #606 → {#607, #608}` (607/608 fork off 606, parallel). #603 depends on all five.

- **#604 — Phase 1: CRD.** `PersistenceMode` enum + `VolumeClaimTemplate` type; optional `persistence_mode` + `volume_claim_templates` on `SandboxTemplateSpec`/`SandboxSpec`; template→sandbox inheritance; regen CRDs. *(no dep)*
- **#605 — Phase 2: PVC reconciliation + Pod mount.** Sandbox-owned `{template}-{sandbox}` PVCs created in `create_pod`; inject volumes/mounts. *(dep #604)*
- **#606 — Phase 3: filesystem suspend.** Branch Pause/Resume on resolved mode: `filesystem` → delete Pod (keep PVC) → Paused; resume → recreate fresh Pod re-mounting PVCs; no SnapshotJob. *(dep #605)*
- **#607 — Phase 4: runtime + API guardrails.** Kata enablement; reject `snapshot`-on-Kata; E2B per-mode docs; warm-pool VCT cold-boot fallthrough. *(dep #606)*
- **#608 — Phase 5 (optional): policy + retention.** `volumeClaimTemplatesPolicy`; paused-PVC retention/GC. *(dep #606, parallel to #607)*

## Code anchors for implementation (file:line, verified)

- **Specs:** `crates/sandbawks-types/src/sandbox_template.rs` (optional-field pattern: `network_policy` ~74, `tolerations` ~91, `node_selector` ~98 — copy this `#[serde(default, skip_serializing_if = "Option::is_none")] Option<…>`). `crates/sandbawks-types/src/sandbox.rs` (`desired_phase` ~67, `network_policy` ~85, `tolerations` ~125; `SandboxPhase` enum ~254; pause status fields `snapshot_ref` ~293, `assigned_node` ~275).
- **Template→sandbox inheritance:** `crates/sandbawks-controller/src/reconcile.rs` `resolve_pod_inputs()` ~2186-2251 (fill-empty-slot logic ~2216-2242). Both cold-start and warm-pool mint pass through here.
- **Pause/resume state machine:** `crates/sandbawks-controller/src/state_machine.rs` — `(Running, PauseRequested) => (Pausing, Action::Pause)` ~361/363; `(Paused, ResumeRequested) => (Resuming, Action::CreateRestorePod)` ~409; `(Pausing, SnapshotValidated) => (Paused, Noop)` ~393. `desired_phase` → trigger mapping in `reconcile.rs` ~1132-1133. SnapshotJob dispatch (`Action::Pause`) `reconcile.rs` ~2730-2767.
- **PodAdapter:** `crates/sandbawks-controller/src/pod_adapter.rs` — `pod_spec_for` volumes vec ~1632, volume_mounts ~1859; `network_policy_for` ~859 (builder pattern to mirror for `pvc_for`); `sandbox_owner_references` ~1880 (controller ownerRef for cascade GC); child-resource create in `create_pod` ~2232-2251 (Secret→NetworkPolicy→Pod sequence, idempotent on 409); `delete_pod` ~2322-2358 (foreground + poll-to-404).
- **Warm claim:** `crates/sandbawks-api/src/warm_claim.rs` — `claim_for` ~151, `ClaimOverrides` ~44-78 (no VCT support → request forces cold-boot, already returns `None`), `apply_overrides` ~125/378.
- **API mint:** `crates/sandbawks-api/src/routes/sandboxes.rs` — cold-start create ~821-933, template resolution ~855-887, warm claim ~904.
- **CRD gen:** `cargo run -p sandbawks-types --bin crdgen -- deploy/crds` (`crates/sandbawks-types/src/bin/crdgen.rs`); outputs `deploy/crds/*.sandbawks.dev.yaml`.
- PVCs are **greenfield** — zero existing `PersistentVolumeClaim` references in the codebase.

## Ops / environment facts (gotchas)

- **Node containers are distroless** (no shell). To inspect a node: `kubectl debug node/<NODE> --image=docker.io/nicolaka/netshoot --profile=sysadmin --attach=false -- sh -c '...'` (host root at `/host`, hostPID). Debug pods land in the kubeconfig's default namespace.
- **Helm skips CRDs** — after any CRD change, `kubectl apply -f deploy/crds` (a stale CRD silently prunes new fields). And **bump `deploy/helm/sandbawks/Chart.yaml` `version`** on any chart change.
- **Standard gates:** `just fmt`, `cargo clippy --all-targets -- -D warnings`, `just test`. Dated changelog under `changelog/` per change.
- **Kata facts:** RuntimeClass `kata-vm-isolation` (handler `kata`); node taint `kata=true:NoSchedule`; node label `kubernetes.azure.com/kata-vm-isolation=true`; deploy needs `node.runtime: kata`. CH socket `/run/vc/vm/<CRI-sandbox-id>/clh-api.sock` (CRI sandbox id = shim `-id`, NOT pod UID), CH v51 on MSHV.

## Open questions (deferred to #608)

- Paused `filesystem` sandbox PVC: default TTL/retention, or live until the Sandbox is deleted?
- Warm pool for `filesystem` mode: cold-boot-only for v1, or pre-bind generic PVCs into warm members later?

## Headline acceptance test (the thing to demo)

`filesystem`-mode Kata sandbox with a PVC at `/workspace` → write a file → `desiredPhase: Paused` (Pod gone, PVC kept, no `snapshotRef`) → `desiredPhase: Running` (fresh VM, file still there). Same round-trip on gVisor.
