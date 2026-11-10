# envd seed lifetime — stop monotonic growth across pause/resume cycles

Date: 2026-05-26T18:39:36Z
Issue: #178

## Summary

Issue #178 flagged that the boot-time seed restored from `envd-state.json`
into `ProcessService::with_seed` / `FilesystemService::with_seed` had no
lifetime bound. The pre-fix collect methods blended `seed ∪ live` into
every snapshot, which meant the shutdown writer persisted `live + seed`
back out to disk on every pause/resume cycle. Across N cycles the seed
grew monotonically: real users that paused and resumed a long-lived
sandbox would see `envd-state.json` accumulate stale entries
proportional to how often their sandbox churned processes.

This commit picks option 2 from the issue: the seed is held read-only on
the service for its lifetime, addressable through new `boot_seed_*`
accessors for the future rehydration path, and is **never** blended into
`collect_*_snapshots`. Live capture is now live-only, which makes the
on-disk snapshot byte-comparable to the live registry and stops the
growth at its source.

The dedup logic that PR #177 / commit `45fb627` landed against live PIDs
and PTY `(id, pid)` is now redundant and has been removed alongside the
seed-blend.

## Files touched

- `crates/envd/src/process.rs` — `collect_process_snapshots` and
  `collect_pty_snapshots` walk the live `entries` map only. Added
  `boot_seed_processes()` and `boot_seed_ptys()` returning `&[…]` views
  of the original seed. The `next_pty_id` counter still initialises past
  the seeded max so the future rehydration path can re-mint live entries
  under the seeded ids without racing fresh `Start(pty)` allocations.
- `crates/envd/src/filesystem.rs` — `collect_watch_snapshots` walks the
  live watcher map only. Added `boot_seed_watches()`. Same counter-init
  reasoning for `next_watcher_id` as on the process side.
- `crates/envd/src/bin/envd.rs` — updated the snapshot-lifecycle module
  doc so it matches the new policy (seed no longer "rides through"
  `/__state` or the shutdown snapshot; remains addressable through
  `boot_seed_*` for rehydration).
- `crates/envd/tests/process.rs` — replaced
  `collect_snapshots_returns_seeded_entries` with
  `boot_seed_is_addressable_but_not_in_collected_snapshots`, which pins
  the new contract: the seed shows up via `boot_seed_processes()` and
  does **not** appear in `collect_process_snapshots()`.
- `crates/envd/tests/filesystem.rs` — same rename and contract change
  for the watch-snapshot side.
- `crates/envd/tests/seed_lifecycle.rs` (new) — integration test
  exercising the issue's acceptance criterion: feed a seed into both
  services, capture, feed that capture forward as a second-boot seed,
  capture again, and assert none of the original ghost identifiers
  (process pid 4242, pty id 77, watch subscriber_id 99) survived into
  the second-boot snapshot.

## Why option 2 (and not 1, 3, or 4)

Option 1 (one-shot clear after first shutdown write) leaves an awkward
intermediate state where `/__state` reports the seed pre-shutdown and
omits it post-shutdown. Option 3 (time-bound the seed at construction)
is the same end-state as 2 but conflates "the seed is informational" with
"the seed is purged immediately" — the rehydration follow-up still wants
to read the seed. Option 4 (track consumed entries) is the most faithful
to the eventual rehydration design but adds a state machine the current
code doesn't need yet.

Option 2 is the smallest change that satisfies the issue's literal
acceptance criterion ("the second-boot snapshot does not contain
entries that originated in a previous Pod's seed") while keeping the
seed addressable for the rehydration path that issue #178 was filed as a
precondition for.

## Validation

- `just fmt`
- `cargo clippy --all-targets -- -D warnings`
- `just test` — all suites pass, including the new
  `seed_lifecycle::two_boot_cycle_does_not_persist_seed` integration
  test and the renamed `boot_seed_is_addressable_but_not_in_collected_snapshots`
  unit tests in `tests/process.rs` and `tests/filesystem.rs`.
