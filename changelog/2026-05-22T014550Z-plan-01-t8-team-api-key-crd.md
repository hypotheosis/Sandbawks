# 2026-05-22 — Plan 01 Task 8: TeamAPIKey CRD (hash-only)

Closes #9.

## What landed

`crates/sandbawks-types/src/team_api_key.rs` — auth records for the
sandbawks-api ingress. Group `sandbawks.io`, version `v1alpha1`, kind
`TeamAPIKey`, plural `teamapikeys`, short name `tak`, namespaced.

Types:

- `TeamApiKeySpec` — `key_prefix` (8-char indexed prefix, e.g.
  `"sbk_a1b2"`), `key_hash` (`"sha256:..."` digest of the full key),
  optional `scopes` (`Vec<String>`, MVP honors `sandboxes:*`). All
  camelCase on the wire; empty scopes are omitted on serialize.

Security note baked into the doc comment: only the SHA-256 hash of the
issued key is stored. The full key is shown to the user once at issue
time and is not recoverable. The 8-char prefix exists to make lookup
O(1) before the constant-time hash comparison runs.

Print column wired into `kubectl get teamapikeys`: Prefix.

`lib.rs` re-exports `TeamAPIKey` (the kube-derived CRD struct, named
after the `kind`) and `TeamApiKeySpec`.

## Tests

`crates/sandbawks-types/tests/team_api_key.rs` (2 tests):

- `crd_metadata` — group/kind/plural assertions match the spec.
- `yaml_roundtrip` — `TeamAPIKey::new(...).spec` survives
  `serde_yaml::to_string` → `from_str` unchanged.

## Validation

- `just fmt` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean.
- `just test` — full workspace test suite passes (2 new + existing).

## What's next

Task 9 — `crdgen` binary that emits all four CRD YAML files into
`deploy/crds/`.
