#!/usr/bin/env node
// Render JUnit XML results into a single, upserted pull-request comment.
//
// Why a hand-rolled Node script instead of a third-party action: we parse the
// XML ourselves (no deps — regex over well-formed JUnit) and POST/PATCH a
// comment through the issue-comments REST API with the job's GITHUB_TOKEN.
// Node 20's `fetch` is global, so nothing has to be installed.
//
// Usage:
//   node ci/junit-pr-comment.mjs <file1.xml> [file2.xml ...]
//
// Behaviour:
//   - Missing files are reported as "not run" rather than failing.
//   - The comment is idempotent: it carries an HTML marker and edits its own
//     prior comment instead of posting a new one on every re-run / push.
//   - Best-effort: any API failure (no token, missing PR, non-2xx) is logged
//     and the script still exits 0 — it is a reporting nicety, never a gate.
//     The summary is always printed to stdout so it shows in the job log too.
//   - Off a pull_request event (e.g. push to main) it prints the summary and
//     exits without commenting.

import { readFileSync } from "node:fs";

const MARKER = "<!-- sandbawks-junit-report -->";

// Human labels for the known result files; falls back to the bare basename.
const LABELS = {
  "pause-resume-smoke-results.xml": "pause/resume smoke",
  "sdk-smoke-results.xml": "sdk-smoke (python)",
  "sdk-smoke-js-results.xml": "sdk-smoke (js)",
  "sdk-smoke-agentic-results.xml": "agentic smoke (python)",
  "sdk-smoke-agentic-js-results.xml": "agentic smoke (js)",
};

/** Parse the attributes of an XML start-tag into a plain object. */
function attrs(tag) {
  const out = {};
  for (const m of tag.matchAll(/([\w:.-]+)\s*=\s*"([^"]*)"/g)) {
    out[m[1]] = m[2];
  }
  return out;
}

const num = (v) => {
  const n = Number.parseFloat(v);
  return Number.isFinite(n) ? n : 0;
};

/**
 * Parse one JUnit file into a summary. Returns null if the file is unreadable
 * (treated as "not run"). Sums across every <testsuite> so a <testsuites>
 * wrapper is never double-counted.
 */
function parseFile(path) {
  let xml;
  try {
    xml = readFileSync(path, "utf8");
  } catch {
    return null;
  }
  const suite = { tests: 0, failures: 0, errors: 0, skipped: 0, time: 0, cases: [] };
  // `<testsuite ` (with whitespace) deliberately does NOT match the
  // `<testsuites>` wrapper — its next char is "s", not whitespace.
  for (const m of xml.matchAll(/<testsuite\s[\s\S]*?>/g)) {
    const a = attrs(m[0]);
    suite.tests += num(a.tests);
    suite.failures += num(a.failures);
    suite.errors += num(a.errors);
    suite.skipped += num(a.skipped);
    suite.time += num(a.time);
  }
  // Collect failed/errored testcases for the details section.
  for (const m of xml.matchAll(/<testcase\b([\s\S]*?)(?:\/>|>([\s\S]*?)<\/testcase>)/g)) {
    const a = attrs("<x " + m[1] + ">");
    const inner = m[2] || "";
    const bad = /<(failure|error)\b/.exec(inner);
    if (!bad) continue;
    const msg = /<(?:failure|error)\b[^>]*\bmessage="([^"]*)"/.exec(inner);
    const name =
      [a.classname, a.name].filter(Boolean).map(decodeXml).join("::") ||
      (a.name ? decodeXml(a.name) : "(unnamed)");
    suite.cases.push({ kind: bad[1], name, message: msg ? decodeXml(msg[1]) : "" });
  }
  return suite;
}

function decodeXml(s) {
  return s
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#0?39;/g, "'")
    .replace(/&amp;/g, "&");
}

const fmtTime = (s) => (s >= 60 ? `${(s / 60).toFixed(1)}m` : `${s.toFixed(1)}s`);

function render(files) {
  const rows = [];
  const totals = { tests: 0, failures: 0, errors: 0, skipped: 0, time: 0 };
  const failingCases = [];
  for (const path of files) {
    const base = path.split("/").pop();
    const label = LABELS[base] || base.replace(/-results\.xml$/, "");
    const s = parseFile(path);
    if (!s) {
      rows.push(`| ${label} | _not run_ | | | | | |`);
      continue;
    }
    const passed = Math.max(0, s.tests - s.failures - s.errors - s.skipped);
    totals.tests += s.tests;
    totals.failures += s.failures;
    totals.errors += s.errors;
    totals.skipped += s.skipped;
    totals.time += s.time;
    rows.push(
      `| ${label} | ${s.tests} | ${passed} | ${s.failures} | ${s.errors} | ${s.skipped} | ${fmtTime(s.time)} |`,
    );
    for (const c of s.cases) failingCases.push({ label, ...c });
  }

  const bad = totals.failures + totals.errors;
  const passed = Math.max(0, totals.tests - totals.failures - totals.errors - totals.skipped);
  const icon = bad > 0 ? "❌" : "✅";
  const headline =
    bad > 0
      ? `${bad} failing · ${passed} passed · ${totals.skipped} skipped`
      : `${passed} passed · ${totals.skipped} skipped`;

  let body = `${MARKER}\n### ${icon} JUnit test report — ${headline}\n\n`;
  body += "| Suite | Tests | ✓ Pass | ✗ Fail | ⚠ Error | Skip | Time |\n";
  body += "|---|--:|--:|--:|--:|--:|--:|\n";
  body += rows.join("\n") + "\n";
  body += `| **Total** | **${totals.tests}** | **${passed}** | **${totals.failures}** | **${totals.errors}** | **${totals.skipped}** | **${fmtTime(totals.time)}** |\n`;

  if (failingCases.length > 0) {
    body += `\n<details open><summary><b>Failures (${failingCases.length})</b></summary>\n\n`;
    for (const c of failingCases.slice(0, 50)) {
      const sym = c.kind === "error" ? "⚠" : "✗";
      // Wrap the message in inline code so embedded `<...>`/quotes render as
      // text, not HTML, in the PR comment. Strip backticks to keep the
      // span intact, collapse whitespace, and truncate.
      const raw = c.message.replace(/`/g, "'").replace(/\s+/g, " ").slice(0, 300);
      const msg = raw ? ` — \`${raw}\`` : "";
      body += `- ${sym} \`${c.label}\` · \`${c.name}\`${msg}\n`;
    }
    if (failingCases.length > 50) body += `- …and ${failingCases.length - 50} more\n`;
    body += `\n</details>\n`;
  }

  const sha = (process.env.GITHUB_SHA || "").slice(0, 8);
  const run = process.env.GITHUB_RUN_ID || "";
  const meta = [sha && `commit \`${sha}\``, run && `run ${run}`].filter(Boolean).join(" · ");
  if (meta) body += `\n<sub>${meta}</sub>\n`;
  return { body, bad };
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
    console.log("junit-pr-comment: not a pull_request event; printing summary only.");
    return;
  }
  if (!api || !repo || !token || !pr) {
    console.warn(
      `junit-pr-comment: missing API context (api=${!!api} repo=${!!repo} token=${!!token} pr=${pr}); skipping comment.`,
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
      console.warn(`junit-pr-comment: list comments returned HTTP ${list.status}`);
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
        `junit-pr-comment: ${existingId ? "edit" : "create"} comment returned HTTP ${res.status}: ${await res.text()}`,
      );
    } else {
      console.log(`junit-pr-comment: ${existingId ? "updated" : "created"} PR #${pr} comment.`);
    }
  } catch (e) {
    console.warn(`junit-pr-comment: comment upsert failed: ${e?.message || e}`);
  }
}

const files = process.argv.slice(2);
if (files.length === 0) {
  console.error("usage: junit-pr-comment.mjs <file.xml> [file.xml ...]");
  process.exit(2);
}
const { body } = render(files);
console.log("\n----- JUnit report (markdown) -----\n" + body + "-----------------------------------\n");
await upsertComment(body);
// Best-effort reporter: never fail the job on a comment/parse issue.
process.exit(0);
