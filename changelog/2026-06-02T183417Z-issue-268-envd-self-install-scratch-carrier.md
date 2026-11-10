# 2026-06-02 — Issue #268: `envd self-install` + slim the carrier image to `scratch`

Sub-issue of #265 (injection model); follow-up to PR #266 and #269
(carrier/template split).

## Context

Under the injection model, the node `sandbawks-node` DaemonSet's
`stage-envd` init container stages the static `envd` binary onto a
node hostPath, and every sandbox Pod mounts that dir read-only and execs
the binary. Staging was done with `cp /usr/local/bin/envd
/sandbawks/envd/envd`, which forced the carrier image
(`.Values.envd.image`) to ship a full userland just to provide `cp` —
so the carrier's final stage was `python:3.12-slim`. That pulls a ~120 MB
python base onto every node to copy a single ~5.6 MB binary, and couples
the carrier to a userland it never otherwise needs.

Note: the issue text predates the node-staged refactor (#269 / PR #275).
It describes the `cp` living in `pod_adapter.rs`'s per-Pod init container,
but that init container was already removed — staging now happens once
per node in the DaemonSet. The fix therefore targets the DaemonSet's
`stage-envd` command, not `pod_adapter.rs` (whose pod test already
asserts *no* init container, so no test change was needed there). The
acceptance criteria are unchanged.

## Change

- **`crates/envd/src/bin/envd.rs`** — converted the flat `Cli` into a
  `clap` parser with an optional `command: Option<Command>` subcommand
  (`None` preserves the historical bare-`envd` server invocation) and
  added a `self-install <dest>` subcommand. `self_install` copies
  `/proc/self/exe` — the kernel's handle on the running executable — to
  `<dest>` with no shell or `cp`. The copy logic is factored into
  `install_from(src, dest)`: it creates `dest`'s parent dir if absent,
  writes to a hidden PID-tagged temp sibling, then `rename`s it into
  place (atomic within one filesystem, so a Pod racing the stage step
  (#272) never execs a half-written binary) and preserves the source's
  executable bits.
- **`deploy/helm/sandbawks/templates/node-daemonset.yaml`** — switched
  the `stage-envd` init container `command` from
  `["cp", "/usr/local/bin/envd", "/sandbawks/envd/envd"]` to
  `["/usr/local/bin/envd", "self-install", "/sandbawks/envd/envd"]`, and
  refreshed the now-stale `securityContext` comment (the `scratch`
  carrier sets no `USER`, so `runAsUser: 0` is the explicit, image-
  independent guarantee rather than an override of a uid-1000 default).
- **`crates/envd/Dockerfile`** — replaced the `python:3.12-slim` final
  stage (and its `useradd` / `USER` / `/var/lib/envd` / `EXPOSE` setup)
  with `FROM scratch` carrying only the static binary. The carrier is
  only ever run by `stage-envd` (`self-install`, as root), so it needs
  no userland, user, state dir, or exposed ports — those belong to the
  server path, which runs from the node-staged binary inside the
  customer rootfs.
- **`justfile`** — updated the `envd-image-run` comment (loads on
  `scratch`, not `python:3.12-slim`).

## Tests

- `crates/envd/src/bin/envd.rs` unit tests: `install_from` produces a
  byte-identical executable copy (binary payload incl. NUL bytes) and
  creates a missing parent directory.
- `crates/envd/tests/binary_smoke.rs`: `self_install_produces_byte_
  identical_copy` runs the real compiled binary's `self-install`
  subcommand end-to-end and asserts the installed bytes equal the
  running binary's — the issue's primary acceptance criterion.

## Validation

- `just fmt`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `just test` (52 suites) — all green.
- `just envd-image-build` → `scratch` image builds; the #267 static-link
  guard passes (`statically linked, stripped`). Image size **8.03 MB**
  (was a `python:3.12-slim` base, ~120 MB+).
- `docker run --rm envd:dev --help` loads under `scratch`;
  `--entrypoint /bin/sh` fails with `stat /bin/sh: no such file or
  directory` — confirming no userland.
- `docker run -v <hostdir>:/out:Z envd:dev self-install /out/envd`
  produces a file whose sha256 equals the in-image
  `/usr/local/bin/envd` (mode `0755`) — the real DaemonSet staging path,
  byte-identical.
- `helm template`/`helm lint` render the DaemonSet with the new
  `self-install` command cleanly.

## Operational notes

- **Roll the chart and the carrier image together.** The DaemonSet
  `stage-envd` command and the carrier base change land in the same
  chart revision, so an in-order upgrade is fine — but a *mismatched*
  pairing crashes `stage-envd`: the old chart's `cp` is absent from the
  new `scratch` image, and the new chart's `self-install` subcommand is
  absent from a still-cached old `python:3.12-slim` image. An operator
  pinning `.Values.envd.tag` to an old image tag (or rolling the chart
  back without the image) will hit this. Today both come from this
  repo's own pipeline, so it is not a blocker — just don't split them.
- **Carrier contract change.** Dropping `USER`/`/var/lib/envd`/`EXPOSE`
  means the carrier is no longer drop-in usable as a
  `SandboxTemplate.spec.envdImage` sidecar that expects to run as uid
  1000 with the state dir pre-created and ports exposed. The reference
  deploy already routes the workload rootfs through `compound-sandbox`
  and runs the server from the node-staged binary inside the customer
  rootfs, so the contract change is contained — but external consumers
  using this image directly as a running sidecar would be affected.

## Acceptance criteria

- [x] `envd self-install <dest>` produces a byte-identical copy of the
  running binary.
- [x] The carrier image contains no python/userland and the init
  container no longer depends on `cp`.
- [x] Cold start path: binary loads under `scratch` and stages byte-
  identically; full in-cluster cold start is covered by the kind-smoke
  workflow on merge.
