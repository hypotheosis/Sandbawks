# 2026-05-22 — CI smoke T1: workflows path migration

## What landed

- Renamed `.github/workflows/ci.yml` → `.forgejo/workflows/ci.yml` via `git mv` (history preserved).
- Removed the now-empty `.github/workflows/` and `.github/` directories.
- No content changes to the workflow itself; triggers, jobs, and runners are untouched.

## Why

Forgejo recognizes both `.github/workflows/` and `.forgejo/workflows/`, but the rest of the CI smoke epic (#66) and Plan 03 (#53) will add new workflows under `.forgejo/`. Keeping every workflow under the Forgejo-native path avoids a confusing split and makes "where do CI jobs live?" answerable with one `ls`.

## Validation

- `find .github -type f` returns nothing.
- `find .forgejo -type f` returns `.forgejo/workflows/ci.yml`.
- `just fmt`, `cargo clippy --all-targets -- -D warnings`, and `just test` all pass locally.
- Forgejo Actions will re-fire `ci` (rust/clippy/test/docs, crd-yaml-up-to-date, helm lint+template) on the next push from the new path.

## Scope

Issue #67 (part of epic #66). Pure path migration — no job-logic, runner, or trigger changes.
