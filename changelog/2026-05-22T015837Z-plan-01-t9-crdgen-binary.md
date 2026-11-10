# 2026-05-22 — Plan 01 Task 9: `crdgen` binary + generated CRD YAML

Closes #10.

## What landed

`crates/sandbawks-types/src/bin/crdgen.rs` — a small binary that
serializes each of the four CRDs (`Sandbox`, `SandboxTemplate`,
`SandboxWarmPool`, `TeamAPIKey`) to YAML, one file per kind, into a
directory passed as `argv[1]`.

Filename layout: `<plural>.<group>.yaml`. Hard-coded rather than derived
from `crd.metadata.name` so the invariant is explicit and easy to grep
for from CI.

Invocation pattern used in dev and CI:

```bash
cargo run -p sandbawks-types --bin crdgen -- deploy/crds
```

Failure mode is a non-zero exit (`ExitCode::FAILURE`) with the error
written to stderr. Missing argument produces `ExitCode::from(2)` and a
short `usage:` line.

`Cargo.toml` gains a `[[bin]]` section pointing at `src/bin/crdgen.rs`.

`deploy/crds/` is committed with the four generated files:

- `sandboxes.sandbawks.io.yaml`
- `sandboxtemplates.sandbawks.io.yaml`
- `sandboxwarmpools.sandbawks.io.yaml`
- `teamapikeys.sandbawks.io.yaml`

## Tests

`crates/sandbawks-types/tests/crdgen.rs` (2 tests):

- `crdgen_emits_four_files` — invokes the binary via
  `env!("CARGO_BIN_EXE_crdgen")` against a per-process temp directory
  under `CARGO_TARGET_TMPDIR`, then asserts that each expected filename
  exists, starts with `apiVersion: apiextensions.k8s.io/v1`, and
  contains `kind: CustomResourceDefinition`.
- `crdgen_is_idempotent` — runs the binary three times into the same
  directory and asserts byte-identical output across runs. This is the
  property the CI drift-check job (Task 12 / #13) will depend on.

The test deliberately avoids pulling in `tempfile`; `CARGO_TARGET_TMPDIR`
gives us a hermetic scratch dir that `cargo clean` removes.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — full workspace test suite passes (20 tests total:
  2 new crdgen + 6 resources + 4 sandbox + 2 sandbox_template + 2 smoke
  + 2 team_api_key + 2 warm_pool).
- `cargo run -p sandbawks-types --bin crdgen -- deploy/crds` is
  idempotent on the committed tree (re-running produces no diff).
- `kubectl --dry-run=client apply -f deploy/crds/*.yaml` reports all
  four CRDs created (dry run) — the generated YAML is schema-valid.

## What's next

Task 10 — Helm chart scaffold at `deploy/helm/sandbawks/` that copies
the generated CRDs into the chart's `crds/` directory.
