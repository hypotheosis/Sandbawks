# kind-smoke — pin ci-runner image to short-sha tag

**Date:** 2026-05-24
**Touches:** `.forgejo/workflows/kind-smoke.yaml`
**Follow-up to:** PR #98 / issue #93

## Symptom

After publishing a corrected ci-runner image (commit `e574650`,
rebased onto `node:20-bookworm-slim` so JS actions can find `node`),
the `kind-smoke` job kept failing in 6–7 seconds at
`https://github.com/actions/checkout@v4`:

```
OCI runtime exec failed: exec failed: unable to start container
process: exec: "node": executable file not found in $PATH
::error::exitcode '127': command not found
```

The same failure that the `e574650` Dockerfile fix was supposed to
clear. ci-runner-image.yaml's Run #142 had succeeded; Harbor had the
new image.

## Root cause

`kind-smoke.yaml`'s `container.image` was pinned to
`ghcr.io/hypotheosis/sandbawks-ci-runner:latest`. The Forgejo runner
(act_runner) defaults to `container.force_pull: false` — once it has
pulled an image tag, it keeps reusing the cached layer until the tag
string changes or the cache is manually purged. So even after the new
`:latest` was published to Harbor, the runner kept exec'ing into the
pre-node-rebase layer it had cached when PR #98 first landed. Same
class of bug as a Kubernetes deployment with `imagePullPolicy: IfNotPresent`
behind a mutable tag.

PR #98 had explicitly flagged this trade-off and queued the fix as a
follow-up:

> "a follow-up PR should tighten this to the dated/sha tag of a
> specific published runner build […] so a silent runner-image change
> can never red-x the smoke gate"

The same property that prevents a *silent* runner-image change from
red-x'ing the gate also prevents a *desired* runner-image change from
clearing it — because the runner won't go look for a new digest behind
a mutable tag. The fix is the same in both directions: stop using a
mutable tag.

## Fix

Re-pinned `kind-smoke.yaml`'s `container.image` to
`ghcr.io/hypotheosis/sandbawks-ci-runner:e574650`. That's the
`type=sha,prefix=,format=short` tag that `ci-runner-image.yaml`'s
metadata-action publishes alongside `:latest` for every build; `e574650`
is the merge sha of the node-rebase commit that's currently the
authoritative ci-runner image.

The runner has no `:e574650` layer cached yet, so it'll do a real pull
the next time `kind-smoke` runs and pick up the node-bearing image.

Expanded the comment block above the `container:` line to spell out the
sha-pin rationale (and the bidirectional cache-staleness it solves), so
the next maintainer who wonders why kind-smoke isn't picking up a
newer runner image gets a one-line answer instead of having to
re-derive it.

## Validation

- `git rev-parse --short=7 HEAD` (pre-rebase-commit-push) reports
  `e574650`, matching the metadata-action default-format short sha.
- The actual end-to-end validation is the next `kind-smoke` run after
  this commit lands: expect the checkout step to clear instantly and
  the rest of the smoke (kindest/node load, kind create, helm install,
  smoke-assert.sh) to behave as it did before issue #93.

## Procedure for future ci-runner image bumps

1. Edit `ci/runner/Dockerfile` (and `ci-runner-image.yaml` if the
   tool-version `ARG`s move).
2. Push to a branch / PR; the path filter fires `ci-runner-image.yaml`
   and publishes a new `:<short-sha>` plus `:latest`.
3. Read the published short sha out of the workflow run (or
   `git rev-parse --short=7 HEAD` on the merge commit).
4. In the same or a follow-up PR, update `kind-smoke.yaml`'s
   `container.image` to the new `:<short-sha>`.
5. Land. The runner re-pulls because the tag string changed.

The two-PR variant of this is fine; the same-PR variant requires the
ci-runner-image.yaml run to finish first, so `kind-smoke` in that PR
will be red until the publish completes (matching PR #98's situation).

## Out of scope

- Server-side runner config changes (e.g. setting `container.force_pull: true`
  in act_runner config.yaml). That'd let us keep `:latest`, but it
  requires runner-host access and would silently change behaviour for
  every workflow on that runner.
- Automating step 3–4 of the bump procedure (e.g. a follow-up workflow
  that opens a PR bumping the kind-smoke pin after ci-runner-image
  publishes). Worth doing if these bumps become frequent.
