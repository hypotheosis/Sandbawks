#!/usr/bin/env node
// Post a sticky coverage comment on PRs, updated in-place on every push.
//
// Sibling to `junit-pr-comment.mjs` and `bench-pr-comment.mjs`: those report
// test pass/fail and perf timings; this one reports line/function/region
// coverage from a `cargo llvm-cov report --json` output file.
//
// Why hand-rolled Node (no deps): Node 20's global `fetch` is all the REST
// calls need, and a script keeps the comment format under our control instead
// of a third-party reporter action's.
//
// Usage:
//   node ci/coverage-pr-comment.mjs <coverage.json>
//
// Behaviour mirrors the JUnit and bench reporters:
//   - A missing or unparseable file is rendered as "unavailable", never an error.
//   - The comment is idempotent via an HTML marker — it edits its own prior
//     comment instead of stacking a new one per push.
//   - Best-effort: any API failure (no token, missing PR, non-2xx) is logged and
//     the script exits 0 — it is a reporting nicety, never a gate. The gate is
//     enforced by `cargo llvm-cov report --fail-under-lines 80` in the CI step
//     that runs AFTER this script.
//   - On a non-pull_request event the summary is printed to stdout only.

import { readFileSync } from "node:fs";

// HTML comment used to find and PATCH an existing coverage comment rather than
// stacking a new one per push. Must be unique across all CI comment scripts.
const MARKER = "<!-- sandbawks-coverage-report -->";

// Minimum line coverage percentage — read from the env so the comment icon
// and [!CAUTION] block stay in sync with the `--fail-under-lines N` value
// in the workflow YAML without a second place to update.
const GATE_PCT = Number.parseFloat(process.env.COVERAGE_GATE ?? "80") || 80;

/**
 * Parse the `cargo llvm-cov report --json` output and return the totals object,
 * or null on any failure (file absent, wrong format, etc.).
 *
 * The JSON is `llvm-cov export --summary-only` format:
 * { "data": [{ "totals": { "lines": {...}, "functions": {...}, "regions": {...} } }], ... }
 */
function parseCoverage(path) {
  let text;
  try {
    text = readFileSync(path, "utf8");
  } catch {
    return null;
  }
  try {
    const json = JSON.parse(text);
    const totals = json?.data?.[0]?.totals;
    if (!totals?.lines || !totals?.functions || !totals?.regions) return null;
    return totals;
  } catch {
    return null;
  }
}

const pct = (n) => Number.isFinite(n) ? n.toFixed(2) : "?";

/**
 * Render the coverage markdown body.
 *
 * @param {string} path - Path to the coverage.json file.
 * @returns {{ body: string }} Markdown body with MARKER prefix.
 */
function render(path) {
  const totals = parseCoverage(path);

  if (!totals) {
    const body =
      `${MARKER}\n### Coverage report unavailable\n\n` +
      `Failed to parse \`coverage.json\` — the test run may have failed or the ` +
      `report was not generated.\n`;
    return { body };
  }

  const { lines, functions, regions } = totals;
  const linePct = lines.percent ?? 0;
  const icon = linePct >= GATE_PCT ? "✅" : "❌";

  let body = `${MARKER}\n### ${icon} Coverage — ${pct(linePct)}% lines\n\n`;
  body += "| Metric | Covered | Total | % |\n";
  body += "|---|--:|--:|--:|\n";
  body += `| Lines | ${lines.covered} | ${lines.count} | ${pct(lines.percent)}% |\n`;
  body += `| Functions | ${functions.covered} | ${functions.count} | ${pct(functions.percent)}% |\n`;
  body += `| Regions | ${regions.covered} | ${regions.count} | ${pct(regions.percent)}% |\n`;

  if (linePct < GATE_PCT) {
    body += `\n> [!CAUTION]\n> Line coverage ${pct(linePct)}% is below the ${GATE_PCT}% gate — CI will fail.\n`;
  }

  const sha = (process.env.GITHUB_SHA || "").slice(0, 8);
  const run = process.env.GITHUB_RUN_ID || "";
  const meta = [sha && `commit \`${sha}\``, run && `run ${run}`].filter(Boolean).join(" · ");
  if (meta) body += `\n<sub>${meta}</sub>\n`;

  return { body };
}

/** Resolve the PR number from the GitHub Actions event payload. */
function prNumber() {
  const p = process.env.GITHUB_EVENT_PATH;
  if (!p) return null;
  try {
    const ev = JSON.parse(readFileSync(p, "utf8"));
    return ev.pull_request?.number ?? ev.number ?? null;
  } catch {
    return null;
  }
}

async function upsertComment(body) {
  const api = process.env.GITHUB_API_URL; // e.g. https://api.github.com
  const repo = process.env.GITHUB_REPOSITORY; // owner/repo
  const token = process.env.GITHUB_TOKEN;
  const pr = prNumber();

  if (process.env.GITHUB_EVENT_NAME !== "pull_request") {
    console.log("coverage-pr-comment: not a pull_request event; printing summary only.");
    return;
  }
  if (!api || !repo || !token || !pr) {
    console.warn(
      `coverage-pr-comment: missing API context (api=${!!api} repo=${!!repo} token=${!!token} pr=${pr}); skipping comment.`,
    );
    return;
  }

  const headers = {
    Authorization: `token ${token}`,
    Accept: "application/json",
    "Content-Type": "application/json",
  };

  try {
    // Find an existing comment with our marker and PATCH it (idempotent upsert).
    const listUrl = `${api}/repos/${repo}/issues/${pr}/comments?per_page=100`;
    const list = await fetch(listUrl, { headers });
    let existingId = null;
    if (list.ok) {
      const comments = await list.json();
      const mine = comments.find((c) => typeof c.body === "string" && c.body.includes(MARKER));
      if (mine) existingId = mine.id;
    } else {
      console.warn(`coverage-pr-comment: list comments returned HTTP ${list.status}`);
    }

    const res = existingId
      ? await fetch(`${api}/repos/${repo}/issues/comments/${existingId}`, {
          method: "PATCH",
          headers,
          body: JSON.stringify({ body }),
        })
      : await fetch(`${api}/repos/${repo}/issues/${pr}/comments`, {
          method: "POST",
          headers,
          body: JSON.stringify({ body }),
        });

    if (!res.ok) {
      console.warn(
        `coverage-pr-comment: ${existingId ? "edit" : "create"} returned HTTP ${res.status}: ${await res.text()}`,
      );
    } else {
      console.log(`coverage-pr-comment: ${existingId ? "updated" : "created"} PR #${pr} comment.`);
    }
  } catch (e) {
    console.warn(`coverage-pr-comment: upsert failed: ${e?.message || e}`);
  }
}

const [, , path] = process.argv;
if (!path) {
  console.error("usage: coverage-pr-comment.mjs <coverage.json>");
  process.exit(2);
}

const { body } = render(path);
console.log(
  "\n----- Coverage report (markdown) -----\n" + body + "--------------------------------------\n",
);
await upsertComment(body);
// Best-effort reporter: never fail the job on a comment/parse issue.
process.exit(0);
