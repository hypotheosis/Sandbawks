# Leader election — PR #437 review fixes (issue #436)

Date: 2026-06-09T16:37:46Z
Branch: `feat-issue-436-leader-election-ha`

Addresses the review feedback on PR #437. The original feature commit
(`eddffa4`) landed the leader-election gate but left the only novel logic
(expiry/holder decision, renewal loop) in the binary with no tests and a
split-brain hole. This iteration extracts that logic into a tested library
module and closes the correctness gaps the review flagged.

## Code changes

### New module: `crates/sandbawks-controller/src/leader_election.rs`

Moved `LeaderElectionConfig`, `make_lease`, and the acquire/renew logic out of
the binary into the library crate so it is unit-testable without `Api<Lease>`.

- **Pure `decide(now, observed, self_identity) -> LeaseDecision`.** The
  expiry/holder state machine is now a pure function: `Create` (no Lease),
  `Renew` (we hold it), `Takeover` (another holder, expired), `Standby`
  (another holder, valid). `try_acquire_or_renew` is a thin I/O shell that maps
  the decision onto one kube call. **13 unit tests** cover every branch
  including the exact expiry boundary, missing `renewTime`, and the
  `lease_duration_seconds`-defaulting path.
- **Split-brain fix (the blocking bug).** The renewal task now tracks
  `last_success: Instant` and exits when `last_success.elapsed() >
  renew_deadline`, not merely when a single call times out. A sustained run of
  fast-failing renew calls no longer lets a deposed leader keep reconciling
  while a standby legitimately takes the Lease. Matches the upstream client-go
  `lastRenewTime` deadline model.
- **Config validation.** `LeaderElectionConfig::validate` enforces
  `0 < retry_period < renew_deadline < lease_duration` and is called from
  `from_env`, so a misconfigured chart fails fast at boot. 4 validate tests.
- **`DEFAULT_LEASE_DURATION_SECS` const** used by both the `from_env` fallback
  and the `decide` expiry math, so a fresh Lease and a Lease with a missing
  duration field are judged against the same TTL.
- **Richer 409 handling.** `try_acquire_or_renew` returns
  `LeaseOutcome::{Held, Standby, Conflict}`. A PUT 409 is `Conflict`, not a
  confirmed loss: the renewal task re-reads on the next tick rather than
  exiting on a benign third-party write (`kubectl edit`, webhook). A confirmed
  `Standby` (another holder seen via GET) still exits immediately.
- **Clock-skew dependency** documented on `decide` (no skew buffer, same simple
  model as upstream kube-controller-manager).
- **Dev-mode holder identity** is now `controller-<hostname>-<pid>` (was
  `controller-<pid>`), so off-cluster identities don't collide across hosts.
  In-cluster `POD_NAME` still wins.
- **`process::exit` telemetry note** added at both renewal-task exit sites.

### Helm chart

- `controller-deployment.yaml`: `{{ fail }}` guard rejects `replicas > 1` with
  `leaderElection.enabled: false` at render time.
- `rbac.yaml`: leases verbs trimmed from the full set to `get/create/update`
  (the only verbs leader election uses); comment notes the namespaced-Role
  alternative.
- `values.yaml`: HA section documents the boot-time validation and the
  TTL-bounded takeover gap on graceful (non-deleting) leader shutdown.

## Not changed

The gvisor `HttpEnvdProbe` error-message concern raised in the review was
already resolved by the `main` merge (`1bcc16b`): `{addr}` is stripped from all
probe error messages and the five exception-grouping tests are present in HEAD.

## Validation

- `cargo fmt`
- `cargo clippy -p sandbawks-controller --all-targets -- -D warnings`
- `cargo test -p sandbawks-controller` (adds 13 leader-election unit tests)
- `helm template` with `replicas=2,leaderElection.enabled=false` fails as
  intended; with `leaderElection.enabled=true` renders clean.
