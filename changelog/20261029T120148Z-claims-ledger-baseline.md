# Claims ledger baseline (2026-10-29)

This change adds `claims.json`, the repo's claims-mcp ledger. It holds 188
atomic claims about sandbawks, extracted from the MVP spec, the later design
specs, OBSERVABILITY.md, TRACING.md, the operator docs and `.claude/CLAUDE.md`.
Each claim has a FEVER verdict. Each leaf claim's evidence was gathered
independently. The ten parent and component claims (CLM-0001 to CLM-0010)
carry only roll-up `analysis` evidence derived from their children.

## Why

Other repos (e2b_client_rs) run claims audits that touch sandbawks behaviour,
but their `CLM-` ids live in their own ledgers. This ledger gives sandbawks its
own id space to track and cite. When citing another repo's claim, qualify it,
for example `e2b_client_rs:CLM-0118`.

## How it was produced

- The coordinator (claude-opus-5-5) registered a root claim (CLM-0001) and six
  area claims (CLM-0002 to CLM-0007).
- Six claude-sonnet-5-5 auditors, one per area, registered the leaf claims,
  gathered evidence and recorded verdicts. Their evidence came from code
  reading, targeted `cargo test` runs, a `crdgen` diff, `helm lint`/`template`,
  `cargo clippy -D warnings` and `cargo fmt --check`.
- The coordinator then rolled the results up into the parent verdicts.
- No live cluster was used, so claims that need one are `not_enough_info`.

| Area | Parent | Leaves | Supported | Refuted | Not enough info |
|---|---|---|---|---|---|
| API | CLM-0002 | 28 | 20 | 7 | 1 |
| Controller / CRDs | CLM-0003 | 31 | 25 | 6 | 0 |
| Snapshot / runtime | CLM-0004 | 34 | 25 | 6 | 3 |
| envd | CLM-0005 | 29 | 24 | 3 | 2 |
| Observability | CLM-0006 | 24 | 19 | 5 | 0 |
| Docs / ops / constraints | CLM-0007 | 32 (+3 components) | 23 | 7 | 2 |

Totals over all 188 active claims, parents included: 136 supported, 43 refuted,
9 not enough info. The integrity seals verify. During review, CLM-0031 (`.env`
in `.gitignore`) was fixed in this PR and re-audited to supported, which moves
the totals to 137 / 42 / 9.

## Highest-severity findings (integrity level 4)

- CLM-0076: `POST /files/mint` does not check that the caller owns the
  sandbox, so any authenticated team can mint file tokens for any sandbox id.
  See `crates/sandbawks-api/src/routes/files.rs` `mint_url`. Tracked in #693;
  the related file-route scope gap (CLM-0075) is #694.
- CLM-0081: the per-sandbox envd proxy is unauthenticated and resolves only in
  `default_namespace`. This is documented as deliberate, but it contradicts
  spec §9. Authentication is tracked in #683 (fix in PR #691), the namespace
  half in #695.
- CLM-0096: envd `process.Connect` does not emit a `StartEvent` first frame,
  which the E2B SDK expects when reconnecting. Tracked in #677, item 1.
- CLM-0188 (not enough info): it is unknown whether the source sandbox
  survives a pause that fails after the checkpoint. Tracked in #697; the
  related fixed snapshot keys (CLM-0165) are #696.

Every claim with a tracking issue carries an `issue:N` tag in the ledger. The
22 refuted doc-drift claims are tracked together in #699.

## Working with the ledger

Use the claims MCP tools only. Never edit `claims.json` by hand, because every
record is sealed. Re-audit refuted and not-enough-info claims after fixes. The
history is append-only.
