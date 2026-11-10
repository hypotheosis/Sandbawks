# sandbawks Plan 03 — `envd-rs` in-sandbox agent + reference template image

> **Scope.** Third of ~9 sub-plans decomposing the sandbawks MVP. After Plan 02
> the controller drives the `Sandbox` state machine against a `FakeRuntimeAdapter`
> that no-ops — there is nothing inside a sandbox Pod to talk to. Plan 03 lands
> the first piece of the data plane: the agent that lives inside every sandbox
> Pod and exposes the gRPC contract that every later component (API gateway,
> gVisor adapter, warm pool) speaks to.
>
> See [`docs/superpowers/specs/2026-05-21-sandbawks-mvp-design.md`](../specs/2026-05-21-sandbawks-mvp-design.md)
> §3.1 (component table — `envd` line item), §4.2 (envd port 49983), §6 (pause/
> resume mechanics + `envd-state.json` in §6.1), §14 sub-plan 3 (scope).
>
> Tracked end-to-end via epic #53
> (closed). Implementation PRs #85–#97; per-task issues #54–#65. This document
> was authored after the fact (see issue
> #166) to fill the gap in the
> `docs/superpowers/plans/` series.

## Predecessor plans

- [Plan 01 — types + CRDs + Helm scaffold](2026-05-21-sandbawks-01-types-and-crds.md)
- [Plan 02 — controller skeleton + state machine](2026-05-22-sandbawks-02-controller-skeleton.md)

## Goal

Ship the `crates/envd` in-sandbox agent with the full E2B-compatible gRPC
surface on port 49983, plus a reference OCI template image built and pushed to
Harbor (`ghcr.io/hypotheosis/envd`) by a Forgejo Actions workflow. After this plan,
Plan 04 (`sandbawks-api`) and Plan 05 (`GvisorAdapter`) can be built against a
real, running envd — no more mocks for the in-sandbox contract.

Plans 04–09 are then purely additive on top of envd: each one writes *callers*
of the envd gRPC surface; none modify envd itself.

## Architectural decisions locked in this plan

These were resolved during brainstorming and during the per-task PRs;
**implementers of later plans should not relitigate them**.

### 1. Wire-compatibility via vendored, byte-identical E2B `.proto` files

The `.proto` schemas live under `crates/envd/proto/{filesystem,process}/`,
pinned byte-identical to upstream
[`e2b-dev/infra`](https://github.com/e2b-dev/infra) (commit hash and Apache-2.0
attribution recorded in `crates/envd/proto/NOTICE` and `proto/SOURCE`).
`tonic-build` regenerates the Rust bindings at build time; we never hand-edit
the generated code or the vendored protos in-tree.

- **Why:** Plan 06 will run `pip install e2b` against the gateway and exercise
  the published SDKs end-to-end. Wire-compat by construction is the cheapest
  way to keep that test honest as upstream evolves.

### 2. File **metadata** over gRPC; file **content** over HTTP

The vendored `filesystem.proto` is metadata-only — `Stat`, `MakeDir`, `Move`,
`ListDir`, `Remove` plus the `WatchDir` event-stream trio. **File content
(read/write of bytes) is not exposed over gRPC** and is deferred to Plan 04's
`sandbawks-api` presigned-URL proxy (spec §14 sub-plan 4).

- **Why:** Streaming bytes over gRPC inside a single per-pod TCP session bottlenecks
  large transfers and complicates back-pressure. Spec §14 sub-plan 4 already
  scopes file content into the gateway's HMAC-signed-URL proxy, which streams
  bodies into envd's filesystem on the server side via the kube exec/cp path
  rather than over gRPC.
- **Consequence:** `FilesystemService` is small and stable. Plan 04 carries the
  file-content burden alone.

### 3. Path-traversal safety via a single `resolve()` chokepoint

Every `FilesystemService` request path is canonicalised through one
`resolve()` guard that rejects `../etc/passwd`-style escapes regardless of
intermediate symlinks. There is exactly one chokepoint; service methods cannot
forget to call it because the typed input is the resolved path.

- **Why:** envd runs as PID 1 inside the sandbox. If an SDK caller can write
  outside the sandbox root, the runtime adapter's isolation story collapses.

### 4. Process / PTY event fan-out via `tokio::sync::broadcast` with retention

`ProcessService` and `PtyService` both share a `Connect`-shaped streaming
contract. Each running child has a `tokio::sync::broadcast` channel that
fans-out stdout/stderr/exit events. After a child exits, the channel keeps
serving the final events for a short retention window so a late `Connect`
call still observes the `EndEvent` rather than racing to silence.

- **Why:** SDK clients legitimately call `Connect` after `Start`; without the
  retention window, a fast-exit process (`echo hi`) would race the SDK and
  return an empty stream. The race-freedom argument is recorded in the
  `crate::process` module doc comment.
- **Rejected:** Per-call mpsc fan-out (forces a single subscriber and breaks
  `List → Connect → SendInput` reconnection) and unbounded channel buffering
  (DoS surface).

### 5. PTY uses the same `Connect` shape as `Process`

`PtyService::Create / Resize / Stream / Kill` deliberately mirrors the
`ProcessService` surface. PTY allocation is via the `portable-pty` crate
(cross-platform); resize is a synchronous `ioctl`; the bidirectional stream
piggy-backs on the same per-call broadcast subscription used by exec.

- **Why:** Uniform SDK ergonomics — the published E2B SDKs do not distinguish
  the two surfaces in their `commands.run` / `pty.create` APIs at the
  WebSocket layer, so the gRPC surface matching pays off downstream in Plan 04
  (which bridges WS frames straight to either gRPC stream by frame kind).

### 6. `envd-state.json` is BTreeMap-keyed for deterministic serialisation

`crate::state::EnvdSnapshot` uses `BTreeMap` for env vars (not `HashMap`) and
an ordered `Vec<ProcessSnapshot>` keyed by start-time. Round-trips through
`serde_json` are byte-identical given identical state.

- **Why:** Plan 05's `GvisorAdapter` will checkpoint and restore this file as
  part of the pause/resume snapshot bundle (spec §6.1). Deterministic JSON
  means snapshots are content-addressable: the same in-flight state produces
  the same bytes, which gives Plan 05's sha256 chunk-dedup story (decided in
  Plan 05 §6) something stable to hash. Property-tested with `hegel` for
  round-trip stability.

### 7. The envd listener is plain h2 on port 49983

No TLS on the envd gRPC surface. The trust boundary is the in-pod network
namespace and the eventual Plan 05 `NetworkPolicy`; `sandbawks-api` (Plan 04)
terminates TLS at the cluster edge and proxies over the internal Pod network.

- **Why:** TLS inside a one-process Pod network namespace buys nothing the
  cluster's existing network model doesn't already provide, and adds cert
  rotation surface. Revisit only if envd is ever exposed off-cluster
  (currently out of scope).

## File map

```
crates/
  envd/                                  # NEW CRATE
    Cargo.toml
    build.rs                             # drives tonic-build over proto/
    Dockerfile                           # multi-stage; produces a small image
                                         # on python:3.12-slim
    proto/
      NOTICE                             # Apache-2.0 attribution + upstream
                                         # commit hash for both proto subtrees
      SOURCE                             # human-readable upstream URLs
      filesystem/                        # byte-identical from e2b-dev/infra
      process/                           # byte-identical from e2b-dev/infra
    src/
      lib.rs                             # module map; re-exports proto::*
      proto.rs                           # tonic-build include + cleanup
      filesystem.rs                      # FilesystemService impl
      process.rs                         # ProcessService impl (non-PTY exec)
      pty.rs                             # PtyService impl
      state.rs                           # EnvdSnapshot + (de)serialisation
      bin/
        envd.rs                          # binary entrypoint
    tests/
      smoke.rs                           # CRATE_NAME link check
      filesystem.rs                      # tempdir-backed RPC tests
      process.rs                         # echo/sleep/cat exec tests
      pty.rs                             # script-style PTY harness
      state.rs                           # snapshot round-trip
      state_properties.rs                # hegel round-trip properties
      lifecycle.rs                       # in-process gRPC server end-to-end
      binary_smoke.rs                    # spawns the binary + --help

deploy/
  helm/sandbawks/values.yaml             # +envd.{image,tag} default block

.forgejo/workflows/
  envd-image.yml                         # NEW (T10) — build + push to Harbor.
                                         # Later folded into kind-smoke.yaml's
                                         # build-images matrix in PR #135
                                         # (post-Plan-03); see "Deviations".

changelog/
  2026-05-23-plan-03-envd-and-template-image.md     # omnibus narrative
  2026-05-23T<ts>-plan-03-t<n>-<slug>.md            # per-task entries (T1–T8, T11)

justfile                                 # +envd-image-build, +envd-image-run
```

New workspace deps added in `Cargo.toml` `[workspace.dependencies]`:

```toml
# gRPC (Plan 03)
tonic = { version = "0.12", default-features = false, features = [
    "transport", "codegen", "prost", "router",
] }
prost = "0.13"
prost-types = "0.13"
tonic-build = { version = "0.12", default-features = false, features = [
    "prost", "transport",
] }

# PTY + signal plumbing (Plan 03)
nix = { version = "0.29", features = ["signal", "process"] }
notify = "6"

# CLI parsing for the binary (Plan 03)
clap = { version = "4", features = ["derive", "env"] }

# Test-time scratch dirs (Plan 03 dev-dep)
tempfile = "3"

# Hegel property tests (Plan 03 dev-dep)
hegel = "..."  # workspace-pinned; used by state_properties + later plans
```

`tokio`, `tokio-stream`, `futures`, `tracing`, `tracing-subscriber`, `anyhow`,
`serde`, `serde_json` are already in `[workspace.dependencies]` from Plans 01–02.

---

## Task 1: Workspace + `crates/envd` scaffold

**Issue:** #54  ·
**PR:** #85  ·
**Changelog:** `changelog/2026-05-23T123341Z-plan-03-t1-envd-scaffold.md`

**Files touched**

- `Cargo.toml` (workspace): add `crates/envd` to `members`; add `tonic`,
  `prost`, `prost-types`, `tonic-build`, `nix`, `notify`, `clap`, `tempfile`,
  `hegel` to `[workspace.dependencies]`.
- `crates/envd/Cargo.toml`: new crate manifest with `[lib]` and `[[bin]]`
  targets. `build = "build.rs"` declared (the build script itself lands in T2;
  T1's placeholder is empty).
- `crates/envd/src/lib.rs`: module map (`pub mod filesystem; pub mod process;
  pub mod proto; pub mod pty; pub mod state;`) + `pub const CRATE_NAME`.
  Bodies are empty stubs.
- `crates/envd/src/bin/envd.rs`: `clap`-parsed binary stub printing usage.
- `crates/envd/tests/smoke.rs`: links against `envd::CRATE_NAME`.

**Smoke**

- `cargo build -p envd` succeeds (modules empty but compile).
- `cargo test -p envd --test smoke` green.
- Workspace gates (`fmt-check`, `clippy -- -D warnings`, `test`) stay green.

**Commit**: `feat(envd): workspace member + module-map scaffold (Plan 03 T1)`

---

## Task 2: Vendor E2B `.proto` schemas + `tonic-build` codegen

**Issue:** #55  ·
**PR:** #86  ·
**Changelog:** `changelog/2026-05-23T124342Z-plan-03-t2-envd-proto-vendor.md`

**Files touched**

- `crates/envd/proto/NOTICE`: Apache-2.0 attribution + upstream commit hash.
- `crates/envd/proto/SOURCE`: human-readable upstream URLs.
- `crates/envd/proto/filesystem/*.proto`: byte-identical copy from
  `e2b-dev/infra`. Metadata-only surface (no content RPCs).
- `crates/envd/proto/process/*.proto`: byte-identical copy from
  `e2b-dev/infra`. Covers both `Process` and `Pty` services.
- `crates/envd/build.rs`: `tonic_build::configure().compile_protos(&[..], &["proto"])`.
- `crates/envd/src/proto.rs`: `tonic::include_proto!` for both packages;
  re-exports the generated types from the crate's `proto::*`.

**Verification**

- `cargo build -p envd` regenerates the bindings; the crate compiles.
- `git diff` shows zero changes to `proto/` after `cargo build` — the
  vendored files are byte-stable across builds.

**Commit**: `feat(envd): vendor E2B .proto schemas + tonic-build codegen (Plan 03 T2)`

---

## Task 3: `Filesystem` service impl

**Issue:** #56  ·
**PR:** #87  ·
**Changelog:** `changelog/2026-05-23T132550Z-plan-03-t3-filesystem-service.md`

**Files touched**

- `crates/envd/src/filesystem.rs` (replaces empty stub): `FilesystemService`
  struct holding the configured sandbox root. RPC handlers for `Stat`,
  `MakeDir`, `Move`, `ListDir`, `Remove`, `WatchDir` (+ the event-stream
  helpers). Private `resolve()` chokepoint that joins user-supplied paths
  to the sandbox root and rejects any result that escapes via canonicalisation.
- `crates/envd/tests/filesystem.rs`: tempdir-backed tests covering each RPC
  plus explicit `../`, symlink-escape, and absolute-path-rebase cases for the
  resolver.

**Verification**

- `cargo test -p envd --test filesystem --all-features` green.
- Path-escape tests assert `tonic::Code::InvalidArgument`.

**Commit**: `feat(envd): Filesystem service impl (Plan 03 T3)`

---

## Task 4: `Process` (exec) service impl

**Issue:** #57  ·
**PR:** #88  ·
**Changelog:** `changelog/2026-05-23T140417Z-plan-03-t4-process-service.md`

**Files touched**

- `crates/envd/src/process.rs`: `ProcessService` struct holding the active
  child registry. RPCs: `Start` (spawn via `tokio::process::Command`),
  `Connect` (subscribe to per-child `broadcast` channel — see decision §4),
  `SendInput` / `StreamInput` / `CloseStdin` (mpsc forwarder into the child's
  stdin), `SendSignal` (via `nix::sys::signal::kill`), `List`, `Update`.
  Post-exit retention window keeps the broadcast channel serving the final
  events for late subscribers. Race-freedom argument recorded inline.
- `crates/envd/tests/process.rs`: exec tests against `echo`, `sleep`, `cat`,
  plus deliberately racing `Start → immediate Connect` to exercise retention.

**Verification**

- `cargo test -p envd --test process --all-features` green.
- Race test wraps `Connect` in `tokio::time::timeout(50ms)` and asserts the
  final `EndEvent` lands.

**Commit**: `feat(envd): Process (exec) service impl (Plan 03 T4)`

---

## Task 5: `Pty` service impl

**Issue:** #58  ·
**PR:** #89  ·
**Changelog:** `changelog/2026-05-23T173927Z-plan-03-t5-pty-service.md`

**Files touched**

- `crates/envd/src/pty.rs`: `PtyService` struct mirroring `ProcessService`'s
  shape per decision §5. PTY allocation via `portable-pty`, resize via
  `ioctl(TIOCSWINSZ)`, bidirectional stream piggy-backing the same broadcast
  fan-out pattern used by exec. `Kill` routes through the same
  `nix::sys::signal::kill` path as `ProcessService::SendSignal`.
- `crates/envd/tests/pty.rs`: `script`-style harness that drives `bash -i`
  through the PTY, asserts prompt echo, resize semantics, and signal delivery.

**Verification**

- `cargo test -p envd --test pty --all-features` green.

**Commit**: `feat(envd): PTY service impl (Plan 03 T5)`

---

## Task 6: `envd-state.json` serialization + hegel property tests

**Issue:** #59  ·
**PR:** #90  ·
**Changelog:** `changelog/2026-05-23T181252Z-plan-03-t6-envd-state-snapshot.md`

**Files touched**

- `crates/envd/src/state.rs`: `EnvdSnapshot` struct holding `BTreeMap<String,
  String>` env, `Vec<ProcessSnapshot>` (ordered by start-time), and the
  current PTY-session and watch-subscription tables. `Serialize` +
  `Deserialize` derive. `EnvdSnapshot::dump_to_writer` /
  `restore_from_reader` helpers.
- `crates/envd/tests/state.rs`: hand-written round-trip + byte-identity
  asserts.
- `crates/envd/tests/state_properties.rs`: `hegel` property tests asserting
  `restore(dump(s)) == s` for arbitrary snapshots, and that dump is
  byte-deterministic for equal inputs.

**Verification**

- `cargo test -p envd --test state --test state_properties --all-features` green.
- `hegel` test runs ≥256 cases per property by default.

**Commit**: `feat(envd): state.rs snapshot + hegel property tests (Plan 03 T6)`

---

## Task 7: `envd` binary — listener, tracing, graceful shutdown, `--state-file`

**Issue:** #60  ·
**PR:** #91  ·
**Changelog:** `changelog/2026-05-23T182313Z-plan-03-t7-envd-binary.md`

**Files touched**

- `crates/envd/src/bin/envd.rs`: `clap`-parsed CLI with `--listen` (default
  `0.0.0.0:49983`), `--state-file` (optional path to restore from on startup
  and dump to on graceful shutdown), and `--log-level` (env-fallback to
  `RUST_LOG`). Installs `tracing_subscriber` with the `env_filter` + `fmt`
  layers. Binds the listener, wires the three services into a
  `tonic::transport::Server`, and shuts down cleanly on `SIGINT`/`SIGTERM`
  via `tokio::signal`. Restores from `--state-file` if it exists; dumps on
  shutdown if `--state-file` is set.
- `crates/envd/tests/binary_smoke.rs`: spawns the binary with `--help` and
  asserts exit 0 + usage text.

**Verification**

- `cargo run -p envd --bin envd -- --help` prints usage; exits 0.
- `cargo test -p envd --test binary_smoke` green.

**Commit**: `feat(envd): T7 binary entrypoint with graceful shutdown`

---

## Task 8: Full-lifecycle integration test

**Issue:** #61  ·
**PR:** #92  ·
**Changelog:** `changelog/2026-05-23T191429Z-plan-03-t8-lifecycle-integration-test.md`

**Files touched**

- `crates/envd/tests/lifecycle.rs`: spins up an in-process
  `tonic::transport::Server` on `127.0.0.1:0` via `TcpListenerStream`,
  drives the full SDK flow against the live gRPC surface:
  1. `MakeDir /workspace/scratch`, `Write` (via local fs setup — read/write
     of bytes is Plan 04's gateway path, not gRPC), `Stat`, `List`, `Move`.
  2. `Process.Start` `bash -c 'while true; do echo tick; sleep 1; done'`,
     `Connect` to read three ticks, `SendSignal SIGTERM`, observe `EndEvent`.
  3. `Pty.Create`, `Resize`, `Stream` a few commands, `Kill`.
  4. `state.dump` → drop the server → re-spin a fresh server with
     `--state-file` → assert restored snapshot matches.

**Verification**

- `cargo test -p envd --test lifecycle --all-features` green.

**Commit**: `test(envd): full-lifecycle integration test (Plan 03 T8)`

---

## Task 9: Reference template `Dockerfile` + justfile recipes

**Issue:** #62  ·
**PR:** #94  ·
**Per-task changelog:** rolled into the omnibus
`changelog/2026-05-23-plan-03-envd-and-template-image.md`.

**Files touched**

- `crates/envd/Dockerfile`: distroless multi-stage build. Builder stage
  uses `ghcr.io/hypotheosis/sandbawks-ci-runner:latest` (Rust toolchain + sccache
  + cargo-chef) and builds `cargo build --release --bin envd`. Runtime
  stage is `python:3.12-slim` (the reference template base so user code
  written for E2B's default image runs unchanged). COPY `envd` binary to
  `/usr/local/bin/envd`; `ENTRYPOINT ["envd"]`.
- `justfile`: adds
  - `envd-image-build` → `docker build -t ghcr.io/hypotheosis/envd:dev -f crates/envd/Dockerfile .`
  - `envd-image-run` → `docker run --rm ghcr.io/hypotheosis/envd:dev --help`

**Verification**

- `just envd-image-build` succeeds locally.
- `just envd-image-run` exits 0 and prints usage.

**Commit**: `feat(envd): reference template Dockerfile + justfile recipes (Plan 03 T9)`

---

## Task 10: Forgejo Actions workflow — build + push `envd` image to Harbor

**Issue:** #63  ·
**PR:** #95  ·
**Per-task changelog:** rolled into the omnibus
`changelog/2026-05-23-plan-03-envd-and-template-image.md`.

**Files touched**

- `.forgejo/workflows/envd-image.yml` (new): triggers on `push` to `main`
  with path filter `crates/envd/**` plus `workflow_dispatch`. Builds
  `crates/envd/Dockerfile` and pushes `ghcr.io/hypotheosis/envd:{sha-<sha>,latest}`
  to Harbor. All `uses:` are fully-qualified `https://github.com/...` per
  repo convention (bare `owner/repo@ref` resolves against `data.forgejo.org`
  and 404s).

**Verification**

- Workflow runs green on the T10 merge to `main`.
- `docker pull ghcr.io/hypotheosis/envd:latest && docker run --rm ghcr.io/hypotheosis/envd:latest --help`
  exits 0.

**Commit**: `feat(ci): envd image build + push to Harbor (Plan 03 T10)`

**Deviation from epic:** the standalone `envd-image.yml` workflow was later
(post-Plan-03) folded into a unified `.forgejo/workflows/kind-smoke.yaml`
with a `build-images` matrix — see PR
#135 (`ci: unify image build
+ kind-smoke into one workflow with a matrix build job`). The Helm-chart
`envd.tag` pin in T11 still references the sha (`2e1e237`) that T10's
original workflow published, so the rename is purely a CI consolidation and
does not affect the chart's runtime image source.

---

## Task 11: Wire `envd` image default into Helm chart

**Issue:** #64  ·
**PR:** #96  ·
**Changelog:** `changelog/2026-05-23T222845Z-plan-03-t11-envd-chart-default.md`

**Files touched**

- `deploy/helm/sandbawks/values.yaml`: add
  ```yaml
  envd:
    image: ghcr.io/hypotheosis/envd
    tag: 2e1e237   # bumped when a new envd image lands
  ```
  with a comment block explaining that `SandboxTemplate.spec.envdImage`
  overrides take precedence; the value here is the default the controller
  applies when a template doesn't pin one.
- `deploy/helm/sandbawks/templates/controller-deployment.yaml`: pass
  `SANDBAWKS_DEFAULT_ENVD_IMAGE={{ .Values.envd.image }}:{{ .Values.envd.tag }}`
  to the controller as an env var. (The controller doesn't read it yet —
  consuming it lands with `PodAdapter` in Plan 04 T7.)

**Verification**

- `helm lint deploy/helm/sandbawks` clean.
- `helm template test deploy/helm/sandbawks | grep SANDBAWKS_DEFAULT_ENVD_IMAGE`
  renders `ghcr.io/hypotheosis/envd:2e1e237`.

**Commit**: `feat(deploy): wire envd image default into Helm chart (Plan 03 T11)`

---

## Task 12: Workspace-wide green gate + changelog

**Issue:** #65  ·
**PR:** #97  ·
**Changelog:** `changelog/2026-05-23-plan-03-envd-and-template-image.md`
(serves double duty as the omnibus narrative and the T12 green-gate record).

**Files touched**

- `changelog/2026-05-23-plan-03-envd-and-template-image.md` (new) — the
  Plan 03 omnibus narrative: what landed (per-task summaries), what's
  deliberately not here, next-plan preview. Same shape as Plan 02's and
  Plan 04's omnibus changelogs.
- Spot fixes to module-level rustdoc to clear `cargo doc` warnings surfaced
  during the sweep (no behaviour change).

**Verification gates**

All of these passed before T12 closed:

- `cargo test --workspace --all-features` green (Plan 01 + Plan 02 +
  ~40 new envd tests).
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  silent.
- `cargo fmt --all -- --check` silent.
- `cargo doc --workspace --no-deps` warning-free.
- `cargo run -p sandbawks-types --bin crdgen -- deploy/crds` still
  idempotent (Plan 03 does not touch CRDs).
- `helm lint deploy/helm/sandbawks` clean.
- `helm template test deploy/helm/sandbawks` references
  `ghcr.io/hypotheosis/envd:<pinned-sha>`.
- T10's `envd-image.yml` workflow runs green on `main` and publishes
  `ghcr.io/hypotheosis/envd:{sha,latest}` to Harbor.
- A locally-pulled `ghcr.io/hypotheosis/envd:latest` runs `envd --help`
  successfully (binary smoke).

**Commit**: `docs: Plan 03 green-gate sweep + changelog`

---

## Deviations from the epic plan

Recorded for completeness; none of these affected the definition-of-done.

1. **T10 workflow filename + matrix consolidation.** Epic specified
   `.forgejo/workflows/envd-image.yaml`; T10 landed as `envd-image.yml`
   (`.yml` vs `.yaml`, matching the surrounding convention at the time).
   Later (post-Plan-03) PR #135
   folded all per-image workflows into `kind-smoke.yaml`'s `build-images`
   matrix; the standalone file no longer exists on `main`. The Helm
   `envd.tag` pin still references the SHA that T10's original workflow
   published, so the chart's runtime image source is unchanged.

2. **Per-task changelog coverage.** T1–T8 and T11 each landed with an
   individual `changelog/2026-05-23T<ts>-plan-03-t<n>-<slug>.md` file.
   T9, T10, and T12 are covered by the omnibus
   `2026-05-23-plan-03-envd-and-template-image.md` only. This was deliberate
   for T12 (it *is* the green-gate sweep) and pragmatic for T9/T10 (small,
   self-explanatory build-system changes). The intent of the changelog
   directory — a navigable per-task history — is preserved either way, since
   the omnibus narrative has a one-paragraph subsection per task.

3. **File content over gRPC, redux.** The epic body listed `Read` and
   `Write` under `Filesystem` service RPCs ("**`Filesystem` service impl:**
   `Read`, `Write`, `List`, `Stat`, ..."). Brainstorming during T3 resolved
   the ambiguity with spec §14 sub-plan 4: file **content** is not on gRPC
   and is deferred to Plan 04's HMAC-signed-URL proxy. The shipped service
   is metadata-only. The omnibus changelog records this; this plan document
   records it as decision §2 above so a Plan 04 / Plan 06 reader sees the
   contract boundary up front.

4. **`portable-pty` instead of raw `nix::pty`.** Epic T5 mentioned
   `nix::pty`; T5 actually used `portable-pty`, which wraps `nix::pty` on
   Linux but also gives the test harness a uniform API across platforms (the
   crate compiles + tests on the maintainer's macOS dev box, even though
   only Linux ships to production). `nix` is still pulled in for signal
   delivery (`nix::sys::signal::kill`), as the epic intended.

---

## Definition of done

All Task 12 verification gates above are green. Epic #53 is closed.
PRs #85–#97 are merged on `main`.

## Next plan

[Plan 04 — `sandbawks-api` E2B-compatible HTTP/WS gateway](2026-05-23-sandbawks-04-api-gateway.md):
lifecycle endpoints (create / list / kill / pause / connect) translated to
CRD operations, SSE events, exec WebSocket bridging to envd's gRPC surface,
and the file upload / download HMAC-signed-URL proxy that streams bytes into
envd's filesystem on the server side. Plan 04 also pulls forward a minimal
Pod-materializing `PodAdapter` so the gateway is end-to-end testable on
`kind` against a real envd.
