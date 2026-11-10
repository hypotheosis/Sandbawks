# Chore: fail PR CI fast when the branch is behind `main`

**Date:** 2026-06-12T14:26:15Z
**Issue:** #501
**Area:** CI (`.forgejo/`)

## Goal

A PR whose branch does not yet contain the current tip of `main` will have its
CI results invalidated the moment it is rebased / merged-up. Building it now
burns runner CPU for nothing — most expensively the four-image matrix build plus
the kind-smoke e2e run and the pause/resume bench. This change adds a cheap
(~5 s) gate that runs first on `pull_request` events and **fails fast** when the
PR head is behind `main`, so every heavy downstream job is *skipped* rather than
executed. The red gate also doubles as a "rebase me" signal under branch
protection.

## What changed

### `.forgejo/actions/branch-up-to-date/action.yml` (new)

Local composite action holding the detection logic in one place (reused by both
PR-triggered workflows). On a `pull_request` event it:

1. Fetches the **live** tip of the PR's base branch
   (`git fetch --no-tags origin "$BASE_REF"`) — deliberately *not* trusting
   `github.event.pull_request.base.sha`, which is the base recorded when the PR
   event fired and can already be stale.
2. Tests whether that base tip is an ancestor of the PR **head** commit
   (`git merge-base --is-ancestor FETCH_HEAD "$HEAD_SHA"`). Up to date ⇒ exit 0;
   behind ⇒ `::error::` + exit 1.

On any non-`pull_request` event (push to `main`, tags) it short-circuits to
success.

Two correctness subtleties, both handled:

- **Test the PR head, not the checked-out tree.** `actions/checkout` checks out
  the synthetic merge ref (`refs/pull/N/merge`) whose parents already include
  the base tip, so an ancestor test against the checked-out `HEAD` would always
  report "up to date". The action reads `github.event.pull_request.head.sha`
  explicitly.
- **History depth.** `--is-ancestor` needs enough history to connect the two
  commits, so each calling job checks out with `fetch-depth: 0`; a shallow clone
  can otherwise report a false "behind".

Event fields are passed via `env:` (not inline `${{ }}`) so a branch name with
shell metacharacters can never be interpolated into the script body.

### `.forgejo/workflows/ci.yml`

Added a `branch-up-to-date` gate job (checkout `fetch-depth: 0` →
`uses: ./.forgejo/actions/branch-up-to-date`) and made the root `detect-changes`
job `needs:` it. Every other job (`rust`, `go`, `crd-yaml-up-to-date`, `helm`,
`chart-version-bump`) already chains off `detect-changes`, so a failed gate
cascade-skips the whole workflow.

### `.forgejo/workflows/kind-smoke.yaml`

Added the same gate job and made `build-images` `needs:` it. `kind-smoke` and
`bench` already `needs: build-images`, so one failed gate cascade-skips the
expensive legs before a single image is built.

## Validation

Workflow-YAML-only change — no Rust/Go touched, so `just fmt` / `cargo clippy` /
`just test` do not apply to the diff.

- All three YAML files parse cleanly (`yaml.safe_load`).
- The ancestry logic was unit-tested in a throwaway git repo across three
  scenarios: branch behind base ⇒ `BEHIND` (exit 1); branch containing base
  tip ⇒ `UP-TO-DATE` (exit 0); non-PR event ⇒ short-circuit.
- Job-graph wiring confirmed: `branch-up-to-date → detect-changes → {all ci.yml
  jobs}` and `branch-up-to-date → build-images → {kind-smoke, bench}`.

## Out of scope

- **"Up to date" is strict.** Any commit on `main` not contained in the PR marks
  it behind, so every merge to `main` invalidates open PRs until they rebase.
  A softer policy (overlap-only, or an N-commits-behind threshold) is a possible
  future refinement.
- No auto-rebase of the PR branch; the gate only signals.
- The four `push`/`dispatch`-only workflows never run on `pull_request`, so they
  are untouched.
