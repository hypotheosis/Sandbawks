# Cluster CI — fold `bench.yaml` into `kind-smoke.yaml`

## Why

`bench.yaml` and `kind-smoke.yaml` both ran a 4-image
`build-images` matrix (controller, api, envd, node) before their
respective downstream jobs. The two matrices were byte-identical
(same Dockerfiles, same buildx cache refs, same tag plan for the
commit-scoped sha tag the downstreams pull). Per PR that's two
runs of the same build steps — ~30 s of wall + extra Harbor pulls
+ extra runner-slot occupancy, every time, even on cache hit.

The bench docstring explicitly documented the duplication
("Forgejo Actions doesn't expose a clean cross-workflow
dependency primitive"), but that reasoning only blocks
*cross-workflow* dependencies — putting both jobs in the same
workflow as siblings of one shared `build-images` is a
straightforward `needs:` graph that Forgejo supports.

## What changed

- `.forgejo/workflows/kind-smoke.yaml` renamed (in the workflow
  `name:` field, not the file name) from `build + kind smoke` to
  `build + cluster tests`. File name stays the same so existing
  status-check names (`build + cluster tests / build-images`,
  `… / kind-smoke`, `… / bench`) survive the rename without
  resetting branch protections.
- Top-level docstring expanded to describe the three-job shape:
  `build-images` → `{kind-smoke, bench}` parallel siblings.
- Top-level `concurrency:` block added (group
  `cluster-ci-${{ github.ref }}`, cancel-in-progress). The bench
  leg previously had this gate inline; folding it up here gives
  the smoke leg the same auto-cancel behavior PR #192 review
  flagged as a gap.
- `bench` job appended verbatim from the deleted `bench.yaml`,
  with `needs: build-images` pointing at the merged workflow's
  shared build job. Comments retouched to refer to "kind-smoke"
  as a sibling job in the same workflow rather than a separate
  workflow.
- `.forgejo/workflows/bench.yaml` deleted.

## What stayed the same

- Build matrix: still the same 4 images, same Dockerfiles, same
  buildx cache, same Harbor tag plan (sha + pr-N + latest +
  semver — all carried over from kind-smoke's richer set, which
  is a strict superset of what bench needed).
- Test-job contents: both downstream jobs are byte-identical to
  what they ran before — same kind cluster shape, same helm
  install, same fixture seeding, same assertions. Only the
  `needs:` target and a few comment lines moved.
- Fork-PR skip predicate: both downstreams gate on
  `pull_request.head.repo.full_name == github.repository` the
  same way they did before.

## Wall-clock impact

- Per PR: one `build-images` matrix instead of two. ~30 s saved
  on whichever workflow used to start second (mostly cache hit
  overhead + extra Harbor logins), plus one less round of
  per-image cooker re-validation.
- Trade-off accepted: a `build-images` failure now blocks both
  downstreams. This is the right blast radius — if the images
  don't build, neither downstream's results are valid anyway, so
  blocking on a real build failure beats running two stale-image
  test passes.

## Out of scope

- Renaming the file itself (`kind-smoke.yaml` → `cluster-ci.yaml`).
  Would break existing branch-protection rules / status checks
  pinned to the file's job names. Worth a follow-up cleanup once
  the merge is proven stable in production CI for a week or two.
- Folding `kind-node-image.yaml` / `ci-runner-image.yaml` /
  `ci.yml` into the same workflow. Those have different cadences
  (image rebuilds on Dockerfile changes; ci.yml is the rust
  build/test gate); merging them is a separate question.
