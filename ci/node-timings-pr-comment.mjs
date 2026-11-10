#!/usr/bin/env node
// Render the node-side snapshot pipeline per-phase timings into a single,
// upserted pull-request comment.
//
// Sibling to `bench-pr-comment.mjs`: that script reports the bench's
// end-to-end pause/resume p50/p95; this one reports the *internal* breakdown
// the node DaemonSet emits — one structured line per pause / validate / stage
// (see `PhaseTimer` + the `… phase timings (ms)` logs in
// `crates/sandbawks-node/src/gvisor.rs`). Those lines reveal where each second
// of a GiB pause/resume actually goes (checkpoint vs compress vs S3 upload vs
// the validate re-download vs decompress), which the outer p50/p95 can't show.
//
// Input is a text file of node log lines (the bench job's "Node snapshot phase
// timings" step writes the grep of `kubectl logs ds/sandbawks-node` there). The
// script self-filters to lines containing `phase timings (ms)`, so feeding it
// raw node logs also works.
//
// Why hand-rolled Node (no deps): Node 20's global `fetch` is all the REST
// calls need.
//
// Usage:
//   node ci/node-timings-pr-comment.mjs <node-phase-timings.log>
//
// Behaviour mirrors the bench reporter:
//   - Missing/empty input renders as "not run", never an error.
//   - Idempotent via an HTML marker — edits its own prior comment.
//   - Best-effort: any API failure is logged and the script still exits 0; the
//     markdown is always printed to stdout.
//   - Off a pull_request event it prints the summary and exits without posting.

import { readFileSync } from "node:fs";

const MARKER = "<!-- sandbawks-node-phase-timings -->";

/** Group thousands so 20185 reads as `20,185`. */
function group(n) {
  return Math.round(n).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

/** Milliseconds with a parenthetical seconds hint once they get big. */
function fmtMs(ms) {
  if (ms == null || Number.isNaN(ms)) return "—";
  const base = `${group(ms)} ms`;
  return ms >= 1000 ? `${base} (${(ms / 1000).toFixed(2)} s)` : base;
}

/** Effective decimal MB/s for `bytes` moved in `ms` (— when either is absent). */
function fmtRate(bytes, ms) {
  if (!bytes || !ms) return "—";
  return `${(bytes / 1e6 / (ms / 1000)).toFixed(1)} MB/s`;
}

/** Median of a numeric array (average of the two middles when even). */
function median(xs) {
  const v = xs.filter((x) => typeof x === "number" && !Number.isNaN(x)).sort((a, b) => a - b);
  if (v.length === 0) return null;
  const mid = v.length >> 1;
  return v.length % 2 ? v[mid] : (v[mid - 1] + v[mid]) / 2;
}

/**
 * Parse one log line's `name=number` tokens into a map. The `phases=a=1 b=2 …`
 * field is space-joined `k=v` pairs followed by `total_ms=…`, so a single sweep
 * for `(\w+)=(\d+)` captures every numeric phase plus `total_ms`/`bytes_memory`;
 * string fields (`sandbox=`, `caller=`) are simply not numeric and fall out.
 */
function numbers(line) {
  const out = {};
  for (const m of line.matchAll(/([A-Za-z_]\w*)=(\d+)\b/g)) {
    out[m[1]] = Number(m[2]);
  }
  return out;
}

function classify(line) {
  if (line.includes("pause pipeline phase timings")) return "pause";
  if (line.includes("fetch/verify/lay-out phase timings")) {
    const m = line.match(/caller=(\w+)/);
    return m ? m[1] : "fetch";
  }
  return null;
}

/** Parse the input into ordered arrays keyed by line type. */
function parse(text) {
  const pause = [];
  const validate = [];
  const stage = [];
  const upload = [];
  for (const raw of text.split("\n")) {
    // The per-part upload line (only `memory.zst` emits it) carries the
    // process-vs-transfer attribution counters; collect those separately.
    if (raw.includes("streamed snapshot part to object store") && raw.includes("read_hash_ms=")) {
      upload.push(numbers(raw));
      continue;
    }
    if (!raw.includes("phase timings (ms)")) continue;
    const kind = classify(raw);
    if (!kind) continue;
    const sandbox = (raw.match(/sandbox=(\S+)/) || [])[1] || "—";
    const n = numbers(raw);
    const rec = { sandbox, ...n };
    if (kind === "pause") pause.push(rec);
    else if (kind === "PauseValidate") validate.push(rec);
    else if (kind === "Stage") stage.push(rec);
  }
  return { pause, validate, stage, upload };
}

/** A markdown table: header cols, rows, with a trailing median row. */
function table(headers, aligns, rows, medianRow) {
  let s = `| ${headers.join(" | ")} |\n`;
  s += `|${aligns.join("|")}|\n`;
  for (const r of rows) s += `| ${r.join(" | ")} |\n`;
  if (medianRow) s += `| ${medianRow.join(" | ")} |\n`;
  return s;
}

function renderPause(pause) {
  if (pause.length === 0) return "";
  const rows = pause.map((p, i) => [
    String(i + 1),
    fmtMs(p.checkpoint),
    fmtMs(p.compress),
    fmtMs(p.upload_memory),
    fmtRate(p.bytes_memory, p.upload_memory),
    fmtMs(p.total_ms),
  ]);
  const med = [
    "**median**",
    fmtMs(median(pause.map((p) => p.checkpoint))),
    fmtMs(median(pause.map((p) => p.compress))),
    fmtMs(median(pause.map((p) => p.upload_memory))),
    fmtRate(median(pause.map((p) => p.bytes_memory)), median(pause.map((p) => p.upload_memory))),
    fmtMs(median(pause.map((p) => p.total_ms))),
  ];
  return (
    "#### Pause pipeline (`pause()` — checkpoint → upload)\n\n" +
    table(
      ["Iter", "checkpoint", "compress", "upload_memory", "↑ rate", "total"],
      ["--:", "--:", "--:", "--:", "--:", "--:"],
      rows,
      med,
    )
  );
}

/** Upload internals for `memory.zst` — the process-vs-transfer attribution. */
function renderUpload(upload) {
  if (upload.length === 0) return "";
  const num = (x) => (x == null ? "—" : String(x));
  const rows = upload.map((u, i) => [
    String(i + 1),
    fmtMs(u.read_hash_ms),
    fmtMs(u.send_block_ms),
    fmtMs(u.put_ms),
    fmtMs(u.avg_put_ms),
    num(u.parallelism),
    num(u.max_inflight),
    num(u.skipped),
    fmtMs(u.elapsed_ms),
  ]);
  const med = [
    "**median**",
    fmtMs(median(upload.map((u) => u.read_hash_ms))),
    fmtMs(median(upload.map((u) => u.send_block_ms))),
    fmtMs(median(upload.map((u) => u.put_ms))),
    fmtMs(median(upload.map((u) => u.avg_put_ms))),
    num(median(upload.map((u) => u.parallelism))),
    num(median(upload.map((u) => u.max_inflight))),
    num(median(upload.map((u) => u.skipped))),
    fmtMs(median(upload.map((u) => u.elapsed_ms))),
  ];
  return (
    "#### Upload internals — `memory.zst` (process vs transfer)\n\n" +
    table(
      [
        "Iter",
        "read_hash (disk+sha)",
        "send_block (await uploader)",
        "put_ms (Σ in-flight)",
        "avg_put / chunk",
        "N (cfg)",
        "max_inflight",
        "skipped",
        "total",
      ],
      ["--:", "--:", "--:", "--:", "--:", "--:", "--:", "--:", "--:"],
      rows,
      med,
    ) +
    "\n<sub>`read_hash` ≈ total ⇒ disk/CPU (IOPS) bound · `send_block` ≈ total ⇒ transfer-bound · " +
    "`avg_put` is per-chunk PUT latency (network/backend) · `max_inflight` < 8 ⇒ uploader starved by the reader.</sub>\n"
  );
}

/** Shared renderer for the two read-side tables (validate / stage). */
function renderFetch(recs, title, byBytes) {
  if (recs.length === 0) return "";
  const rows = recs.map((r, i) => [
    String(i + 1),
    fmtMs(r.memory_download),
    fmtRate(byBytes.get(r.sandbox), r.memory_download),
    fmtMs(r.memory_decompress),
    fmtMs(r.small_parts),
    fmtMs(r.total_ms),
  ]);
  const med = [
    "**median**",
    fmtMs(median(recs.map((r) => r.memory_download))),
    fmtRate(median([...byBytes.values()]), median(recs.map((r) => r.memory_download))),
    fmtMs(median(recs.map((r) => r.memory_decompress))),
    fmtMs(median(recs.map((r) => r.small_parts))),
    fmtMs(median(recs.map((r) => r.total_ms))),
  ];
  return (
    `#### ${title}\n\n` +
    table(
      ["Iter", "memory_download", "↓ rate", "memory_decompress", "small_parts", "total"],
      ["--:", "--:", "--:", "--:", "--:", "--:"],
      rows,
      med,
    )
  );
}

function render(text) {
  const { pause, validate, stage, upload } = parse(text);
  // Boundary diagnostics (the CI comment once rendered with empty sections):
  // prove what the parser actually received and extracted.
  console.log(
    `node-timings: input ${text.length} bytes / ${text.split("\n").length} lines · ` +
      `parsed pause=${pause.length} validate=${validate.length} stage=${stage.length} upload=${upload.length}`,
  );
  if (pause[0]) console.log(`node-timings: first pause record keys: ${Object.keys(pause[0]).join(",")}`);
  const anyRun = pause.length + validate.length + stage.length + upload.length > 0;

  let body = `${MARKER}\n### Snapshot pipeline per-phase timings (node-side)\n\n`;
  if (!anyRun) {
    body += "_no `phase timings (ms)` lines found — bench may not have run a pause/resume._\n";
    return body;
  }

  // sandbox -> compressed memory.zst bytes, so the read-side tables can show an
  // effective download rate (their own log lines don't carry the byte count).
  const byBytes = new Map();
  for (const p of pause) if (p.bytes_memory) byBytes.set(p.sandbox, p.bytes_memory);

  const sizes = [...new Set(pause.map((p) => p.bytes_memory).filter(Boolean))];
  if (sizes.length === 1) {
    const b = sizes[0];
    body += `Payload: \`memory.zst\` ≈ ${(b / 1024 / 1024).toFixed(0)} MiB compressed · ${pause.length} iteration(s).\n\n`;
  }

  body += renderPause(pause) + "\n";
  body += renderUpload(upload) + "\n";
  body += renderFetch(validate, "Pause validate (re-download + re-decompress — issue #235)", byBytes) + "\n";
  body += renderFetch(stage, "Resume stage (node-side download + decompress)", byBytes) + "\n";

  // Full pause = pause() + its validate, paired by sandbox; this is what the
  // bench's end-to-end pause p50 reflects (plus controller/scheduling overhead).
  const validBySandbox = new Map(validate.map((v) => [v.sandbox, v]));
  const fullPause = pause
    .map((p) => (validBySandbox.has(p.sandbox) ? p.total_ms + validBySandbox.get(p.sandbox).total_ms : null))
    .filter((x) => x != null);
  if (fullPause.length) {
    body += `<sub>Full pause (pause() + validate, paired) median: **${fmtMs(median(fullPause))}**. `;
    body += `The validate re-download+decompress is pure overhead on every pause (issue #235).</sub>\n`;
  }

  const sha = (process.env.GITHUB_SHA || "").slice(0, 8);
  const run = process.env.GITHUB_RUN_ID || "";
  const store = process.env.KIND_SMOKE_REMOTE_SNAPSHOT_STORE === "1" ? "central garage" : "local garage";
  const footer = [`store: ${store}`, sha && `commit \`${sha}\``, run && `run ${run}`].filter(Boolean).join(" · ");
  if (footer) body += `\n<sub>${footer}</sub>\n`;
  return body;
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
  const api = process.env.GITHUB_API_URL;
  const repo = process.env.GITHUB_REPOSITORY;
  const token = process.env.GITHUB_TOKEN;
  const pr = prNumber();
  if (process.env.GITHUB_EVENT_NAME !== "pull_request") {
    console.log("node-timings-pr-comment: not a pull_request event; printing summary only.");
    return;
  }
  if (!api || !repo || !token || !pr) {
    console.warn(
      `node-timings-pr-comment: missing API context (api=${!!api} repo=${!!repo} token=${!!token} pr=${pr}); skipping.`,
    );
    return;
  }
  const headers = {
    Authorization: `token ${token}`,
    Accept: "application/json",
    "Content-Type": "application/json",
  };
  try {
    const list = await fetch(`${api}/repos/${repo}/issues/${pr}/comments?per_page=100`, { headers });
    let existingId = null;
    if (list.ok) {
      const comments = await list.json();
      const mine = comments.find((c) => typeof c.body === "string" && c.body.includes(MARKER));
      if (mine) existingId = mine.id;
    } else {
      console.warn(`node-timings-pr-comment: list comments returned HTTP ${list.status}`);
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
        `node-timings-pr-comment: ${existingId ? "edit" : "create"} comment returned HTTP ${res.status}: ${await res.text()}`,
      );
    } else {
      console.log(`node-timings-pr-comment: ${existingId ? "updated" : "created"} PR #${pr} comment.`);
    }
  } catch (e) {
    console.warn(`node-timings-pr-comment: comment upsert failed: ${e?.message || e}`);
  }
}

const file = process.argv[2];
if (!file) {
  console.error("usage: node-timings-pr-comment.mjs <node-phase-timings.log>");
  process.exit(2);
}
let text = "";
try {
  text = readFileSync(file, "utf8");
} catch {
  text = "";
}
const body = render(text);
console.log("\n----- node phase timings (markdown) -----\n" + body + "-----------------------------------------\n");
await upsertComment(body);
// Best-effort reporter: never fail the job on a comment/parse issue.
process.exit(0);
