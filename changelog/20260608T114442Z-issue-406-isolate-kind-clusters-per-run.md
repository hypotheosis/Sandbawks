# Issue #406 — isolate kind clusters per CI run

**Date:** 2026-06-08T11:44:42Z
**Issue:** #406 — ci(kind): concurrent runs collide on static `sandbawks-{smoke,bench}` cluster names — isolate per-run
**Type:** fix (`kind/fix`, `area/deploy`, `prio/p1`)

## Background

`.forgejo/workflows/kind-smoke.yaml` runs two cluster-CI jobs — `kind-smoke`
and `bench` — against a Docker-in-Docker (dind) sidecar that the Forgejo runner
**shares across concurrent jobs and across runs on different refs**. Both jobs
named their kind clusters statically (`sandbawks-smoke`, `sandbawks-bench`) and
pinned static apiserver ports (`:6443`, `:6444`). Each job's create step ran a
destructive, name-keyed cleanup first:

```bash
kind delete cluster --name sandbawks-bench
docker rm -f $(docker ps -aq --filter "label=io.x-k8s.kind.cluster=sandbawks-bench")
```

When a second run's `bench` job started while a first run's `bench` was still
live on the same dind, that cleanup **force-removed the first run's control-plane
container out from under it**. The victim died abruptly — its kube-apiserver
(`dind.internal:6444`) vanished mid-iteration — and surfaced as a misleading
application-layer error (`connection closed before message completed` while
PUTting a 1-MiB upload chunk). The existing `concurrency.group:
cluster-ci-${{ github.ref }}` guard only serializes runs on the **same** ref, so
cross-PR / PR-vs-`main` runs were never mutually excluded.

This was a CI-infrastructure bug, not an application defect. Chosen fix is
**Option 2** from the issue: per-run-unique cluster isolation so concurrent runs
coexist rather than bulldozing each other.

## What changed

All changes are in `.forgejo/workflows/kind-smoke.yaml`.

### Per-run-unique cluster names

- Added a job-level `CLUSTER_NAME` env to both jobs, suffixed with
  `${{ github.run_id }}` (unique per run):
  - `kind-smoke`: `sandbawks-smoke-${{ github.run_id }}`
  - `bench`: `sandbawks-bench-${{ github.run_id }}`
- Rewired **every** name-keyed operation to `${CLUSTER_NAME}`: `kind create`,
  `kind delete`, the `docker rm -f --filter label=io.x-k8s.kind.cluster=…`
  backstop, `kind load docker-image --name`, and the
  `kubectl config set-cluster kind-… / unset clusters.kind-….…` rewrites.
- Re-attempts intentionally reuse the `run_id` (no `run_attempt` suffix): a
  re-run's create-time cleanup then reaps its own prior orphans, while staying
  isolated from every *other* run.

### Dynamic apiserver ports

- Both clusters now set `apiServerPort: 0`, so kind binds a free ephemeral port
  instead of the static `:6443` / `:6444`. Those pins existed only to keep the
  two static clusters from colliding on the shared dind; once any number of
  per-run clusters can be live at once, a static pin would merely relocate the
  collision from cluster-name to port.
- The kubeconfig rewrite now **preserves the port kind chose** instead of forcing
  a static one, by capturing it in a regex group:

  ```bash
  sed -i -E "s|server: https://[^:/]+:([0-9]+)|server: https://${DIND_HOST}:\1|" "$HOME/.kube/config"
  ```

  Only the host is rewritten (so the workflow container can reach dind); the
  dynamic port is carried through via `\1`.

### Run-scoped teardown + age-based orphan sweep

- Added a `Tear down kind cluster` step (`if: always()`) to **both** jobs, placed
  **after** the diagnostics/reporter steps (which need the cluster alive). This
  is now *essential*, not optional: with per-run-unique names nothing else ever
  reaps a cluster (the next run creates a differently-named one and the old
  static-name cleanup is gone), so without it every run would leak its cluster
  onto the shared dind forever.
- Teardown is scoped strictly to `${CLUSTER_NAME}`, so a single run's teardown
  never touches a concurrent run's cluster.
- Each teardown also runs an **age-based orphan sweep**: it force-removes any
  `sandbawks-` kind node container older than 2 h (well past the longest job
  timeout of 75 min). The age gate guarantees a *live* co-resident run — whose
  containers are far younger than its timeout — is never swept, while a run that
  was force-killed mid-create (and whose owning job no longer exists to clean up
  after itself) cannot leak forever.

### Comment accuracy

- Updated the top-level `concurrency` comment: it no longer claims to prevent
  cluster races (it only serializes same-ref runs); cross-ref isolation is now
  noted as handled structurally by per-run-unique names.
- Updated the smoke job's apiserver/kubeconfig rationale comment for the
  `apiServerPort: 0` + port-preserving rewrite.

## Acceptance criteria (issue #406)

- **Two runs on different refs coexist on one dind** — independent cluster names
  (`…-<run_id>`) and ephemeral apiserver ports mean neither the name-keyed
  cleanup nor the port allocation can collide.
- **A run's teardown never touches another run's clusters/containers** — teardown
  is `${CLUSTER_NAME}`-scoped; the only cross-run reaper is the age-gated orphan
  sweep, which by construction skips live runs.
- **No happy-path regression** — a single-run PR creates, uses, and tears down
  exactly one smoke and one bench cluster as before; only the names/ports moved
  from static to per-run.

### Capacity caveat (carried forward, not resolved here)

Option 2 lets multiple GiB-scale clusters coexist on one dind. The issue flags
that the runner/dind must have the memory + disk headroom for 2+ concurrent
clusters each moving ~1 GiB payloads through Garage; if it does not, this should
be paired with a runner-wide concurrency cap (Option 3). That capacity decision
is out of scope for this isolation change and is left to operators to validate
against real dind headroom.

## Validation

- `python3 -c "yaml.safe_load(...)"` → workflow parses.
- `just fmt-check` → PASS (no Rust touched; zero workspace drift — only the
  workflow YAML changed).
- Capture-group `sed` verified locally to rewrite the host while preserving a
  dynamic port (`https://127.0.0.1:43217` → `https://dind.internal:43217`).
- Orphan-sweep `date -d <RFC3339-nano>` parsing + age comparison verified
  locally.
- `shellcheck` on the new teardown block is clean apart from SC2046 on
  `docker rm -f $(docker ps -aq …)` — the intentional, pre-existing idiom in this
  file (word-splitting is *required* to pass multiple container IDs as separate
  args); kept consistent with the surrounding code rather than diverging.

The full effect (two concurrent cross-ref runs not destroying each other) is only
observable on the shared Forgejo dind and cannot be reproduced locally; it is
left as the reviewer's manual test plan on the PR.
