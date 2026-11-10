#!/usr/bin/env node
// Render pause/resume perf-bench results into a single, upserted pull-request
// comment.
//
// Sibling to `junit-pr-comment.mjs`: that script reports pass/fail JUnit suites;
// this one reports the *timing* data the perf bench emits. The bench
// (`tests/bench/src/main.rs`) writes a `bench-results.json` with per-phase
// percentiles (p50/p95/max) and per-iteration samples, but CI only uploaded it
// as an artifact, so a reviewer had to download it to see the numbers. This
// script turns that JSON into a markdown timing table and POST/PATCHes it onto
// the PR via the issue-comments REST API, so every bench run's timings show up
// inline.
//
// Why hand-rolled Node (no deps): Node 20's global `fetch` is all the REST
// calls need, and a script keeps the comment format under our control instead
// of a third-party reporter action's.
//
// Usage:
//   node ci/bench-pr-comment.mjs <bench-results.json> [more.json ...]
//
// Each JSON file is rendered as its own section, labelled by its workload size
// (`payload_mib`) so a future GiB-scale run (issue #291) and the CI-scale run can
// be posted side by side and compared at a glance.
//
// Behaviour mirrors the JUnit reporter:
//   - Missing/unparseable files are rendered as "not run", never an error.
//   - The comment is idempotent via an HTML marker — it edits its own prior
//     comment instead of stacking a new one per push.
//   - Best-effort: any API failure (no token, missing PR, non-2xx) is logged and
//     the script still exits 0. The markdown is always printed to stdout too.
//   - Off a pull_request event it prints the summary and exits without posting.

import { readFileSync } from "node:fs";

const MARKER = "<!-- sandbawks-bench-report -->";

/** Phase-name -> top-level threshold field in the bench report. */
const THRESHOLD_FIELD = {
  pause: "pause_p95_threshold_ms",
  resume: "resume_p95_threshold_ms",
};

/** Parse one bench-results.json. Returns null if unreadable/!JSON ("not run"). */
function parseFile(path) {
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}

/** Group thousands so a 5200 ms p95 reads as `5,200 ms`. */
function group(n) {
  return Math.round(n).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

/** Milliseconds rendered with a parenthetical seconds hint once they get big. */
function fmtMs(ms) {
  const base = `${group(ms)} ms`;
  return ms >= 1000 ? `${base} (${(ms / 1000).toFixed(2)} s)` : base;
}

/** Bytes -> the largest binary unit that keeps the number readable. */
function fmtBytes(b) {
  if (b == null) return "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let v = b;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  // Whole bytes print without a decimal; scaled units keep two.
  return i === 0 ? `${v} ${units[i]}` : `${v.toFixed(2)} ${units[i]}`;
}

/**
 * Render one report's phase-summary rows + collapsible per-iteration samples.
 * Returns `{ section, failed }` where `failed` is true if any phase breached its
 * p95 target (so the headline can flag a regression).
 */
function renderReport(path, report) {
  const base = path.split("/").pop();
  if (!report || !Array.isArray(report.phases)) {
    return { section: `#### ${base}\n\n_not run_\n`, failed: false };
  }

  const payload = `${report.payload_mib} MiB`;
  const iters = report.iterations;
  const label = `${payload} workload · N=${iters}`;

  let failed = false;
  const rows = [];
  for (const ph of report.phases) {
    const threshold = report[THRESHOLD_FIELD[ph.phase]];
    // A phase passes when its p95 is within the configured ceiling. An unknown
    // phase (no mapped threshold) is reported but never gates.
    const breached = typeof threshold === "number" && ph.p95_ms > threshold;
    if (breached) failed = true;
    const status = typeof threshold !== "number" ? "—" : breached ? "❌" : "✅";
    const target = typeof threshold === "number" ? `≤ ${group(threshold)} ms` : "—";
    rows.push(
      `| ${ph.phase} | ${fmtMs(ph.p50_ms)} | ${fmtMs(ph.p95_ms)} | ${fmtMs(ph.max_ms)} | ${target} | ${status} |`,
    );
  }

  let section = `#### ${label}\n\n`;
  section += "| Phase | p50 | p95 | Max | p95 target | Status |\n";
  section += "|---|--:|--:|--:|--:|:--:|\n";
  section += rows.join("\n") + "\n";

  // Per-iteration breakdown: zip phases by iteration so pause/resume durations
  // for the same cycle land on one row. `parts`/bundle bytes come from the pause
  // sample (resume re-uses the pause manifest, see the bench's `Sample` doc).
  const byIter = new Map();
  for (const ph of report.phases) {
    for (const s of ph.samples || []) {
      const row = byIter.get(s.iteration) || { iteration: s.iteration };
      row[`${ph.phase}_ms`] = s.duration_ms;
      if (s.bytes_bundle != null && row.bytes_bundle == null) {
        row.bytes_bundle = s.bytes_bundle;
      }
      byIter.set(s.iteration, row);
    }
  }
  const samples = [...byIter.values()].sort((a, b) => a.iteration - b.iteration);
  if (samples.length > 0) {
    const hasResume = report.phases.some((p) => p.phase === "resume");
    section += `\n<details><summary>Per-iteration samples (${samples.length})</summary>\n\n`;
    section += `| Iter | Pause | ${hasResume ? "Resume | " : ""}Bundle |\n`;
    section += `|--:|--:|${hasResume ? "--:|" : ""}--:|\n`;
    for (const s of samples) {
      const resumeCell = hasResume
        ? ` ${s.resume_ms != null ? fmtMs(s.resume_ms) : "—"} |`
        : "";
      section += `| ${s.iteration} | ${s.pause_ms != null ? fmtMs(s.pause_ms) : "—"} |${resumeCell} ${fmtBytes(s.bytes_bundle)} |\n`;
    }
    section += `\n</details>\n`;
  }

  const meta = [
    report.started_at && `started \`${report.started_at}\``,
    report.bundle_floor_ratio != null && `bundle floor ${report.bundle_floor_ratio}`,
  ]
    .filter(Boolean)
    .join(" · ");
  if (meta) section += `\n<sub>${meta}</sub>\n`;

  return { section, failed };
}

function render(files) {
  const sections = [];
  let anyFailed = false;
  let anyRun = false;
  for (const path of files) {
    const report = parseFile(path);
    if (report) anyRun = true;
    const { section, failed } = renderReport(path, report);
    if (failed) anyFailed = true;
    sections.push(section);
  }

  const icon = anyFailed ? "❌" : anyRun ? "✅" : "⚠";
  const headline = anyFailed
    ? "p95 regression — a phase exceeded its target"
    : anyRun
      ? "all phases within p95 targets"
      : "no bench results found";

  let body = `${MARKER}\n### ${icon} Perf bench timings — ${headline}\n\n`;
  body += sections.join("\n") + "\n";

  const sha = (process.env.GITHUB_SHA || "").slice(0, 8);
  const run = process.env.GITHUB_RUN_ID || "";
  const footer = [sha && `commit \`${sha}\``, run && `run ${run}`].filter(Boolean).join(" · ");
  if (footer) body += `\n<sub>${footer}</sub>\n`;
  return { body, failed: anyFailed };
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
    console.log("bench-pr-comment: not a pull_request event; printing summary only.");
    return;
  }
  if (!api || !repo || !token || !pr) {
    console.warn(
      `bench-pr-comment: missing API context (api=${!!api} repo=${!!repo} token=${!!token} pr=${pr}); skipping comment.`,
    );
    return;
  }
  const headers = {
    Authorization: `token ${token}`,
    Accept: "application/json",
    "Content-Type": "application/json",
  };
  try {
    // Find an existing comment carrying our marker and edit it (idempotent).
    const listUrl = `${api}/repos/${repo}/issues/${pr}/comments?per_page=100`;
    const list = await fetch(listUrl, { headers });
    let existingId = null;
    if (list.ok) {
      const comments = await list.json();
      const mine = comments.find((c) => typeof c.body === "string" && c.body.includes(MARKER));
      if (mine) existingId = mine.id;
    } else {
      console.warn(`bench-pr-comment: list comments returned HTTP ${list.status}`);
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
        `bench-pr-comment: ${existingId ? "edit" : "create"} comment returned HTTP ${res.status}: ${await res.text()}`,
      );
    } else {
      console.log(`bench-pr-comment: ${existingId ? "updated" : "created"} PR #${pr} comment.`);
    }
  } catch (e) {
    console.warn(`bench-pr-comment: comment upsert failed: ${e?.message || e}`);
  }
}

const files = process.argv.slice(2);
if (files.length === 0) {
  console.error("usage: bench-pr-comment.mjs <bench-results.json> [more.json ...]");
  process.exit(2);
}
const { body } = render(files);
console.log("\n----- bench report (markdown) -----\n" + body + "-----------------------------------\n");
await upsertComment(body);
// Best-effort reporter: never fail the job on a comment/parse issue.
process.exit(0);
