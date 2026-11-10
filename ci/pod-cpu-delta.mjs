#!/usr/bin/env node
// Compute per-pod CPU consumed over a window, from two kubelet
// `/stats/summary` snapshots — the decisive client-vs-server discriminator
// for the pause/resume slowness investigation.
//
// The snapshot pipeline is bottlenecked on *some* saturated resource (raising
// chunk concurrency only inflated per-request latency). That curve looks
// identical whether the bottleneck is our own client CPU (e.g. rust-s3 hashing
// every PUT body for SigV4, ×N concurrent under the node's CPU limit) or
// garage's server-side write path (fsync/LMDB) or the disk under it. CPU usage
// separates them:
//   - `sandbawks-node` near its CPU limit while uploading  -> OUR process is the cap.
//   - `sandbawks-node` near-idle but the run is slow        -> we're waiting on
//     garage / disk I/O (not CPU) — investigate fsync/per-object cost next.
//   - `garage` burning CPU                                   -> garage compute.
//   (All three low + slow -> disk I/O wait, i.e. garage durability on the disk.)
//
// The kubelet Summary API exposes cumulative `cpu.usageCoreNanoSeconds` per
// pod, so we don't need metrics-server. Diff two snapshots taken around the
// bench and divide by the wall window to get average cores.
//
// Usage:
//   node ci/pod-cpu-delta.mjs <before.json> <after.json> [name-substr ...]
// Defaults to reporting pods whose names contain sandbawks-node / garage /
// sandbawks-controller / sandbawks-api. Best-effort: exits 0 even on bad input.

import { readFileSync } from "node:fs";

const DEFAULT_MATCHES = ["sandbawks-node", "garage", "sandbawks-controller", "sandbawks-api"];

function load(path) {
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch (e) {
    console.warn(`pod-cpu-delta: could not read ${path}: ${e?.message || e}`);
    return null;
  }
}

/** Map pod-name -> { cpuNs, timeMs } from a /stats/summary document. */
function index(summary) {
  const m = new Map();
  for (const p of summary?.pods || []) {
    const name = p?.podRef?.name;
    const cpuNs = p?.cpu?.usageCoreNanoSeconds;
    const time = p?.cpu?.time;
    if (name && typeof cpuNs === "number" && time) {
      m.set(name, { cpuNs, timeMs: Date.parse(time) });
    }
  }
  return m;
}

const [beforePath, afterPath, ...matchArgs] = process.argv.slice(2);
if (!beforePath || !afterPath) {
  console.error("usage: pod-cpu-delta.mjs <before.json> <after.json> [name-substr ...]");
  process.exit(2);
}
const matches = matchArgs.length ? matchArgs : DEFAULT_MATCHES;
const before = index(load(beforePath));
const after = index(load(afterPath));

console.log("=== per-pod CPU consumed during the bench window ===");
console.log("pod                                   | CPU-seconds | avg cores");
console.log("--------------------------------------+-------------+----------");
const rows = [];
for (const [name, a] of after) {
  if (!matches.some((s) => name.includes(s))) continue;
  const b = before.get(name);
  if (!b) continue;
  const cpuSec = (a.cpuNs - b.cpuNs) / 1e9;
  const wallSec = (a.timeMs - b.timeMs) / 1000;
  const cores = wallSec > 0 ? cpuSec / wallSec : 0;
  rows.push({ name, cpuSec, wallSec, cores });
}
rows.sort((x, y) => y.cores - x.cores);
for (const r of rows) {
  console.log(
    `${r.name.padEnd(37)} | ${r.cpuSec.toFixed(1).padStart(11)} | ${r.cores.toFixed(2).padStart(8)}`,
  );
}
if (rows.length === 0) {
  console.log("(no matching pods found in both snapshots)");
}
const wall = rows[0]?.wallSec;
if (wall) console.log(`\nwindow ≈ ${wall.toFixed(0)}s. avg cores = CPU-seconds / window.`);
console.log(
  "Read: sandbawks-node near its CPU limit ⇒ OUR client is the cap; near-idle ⇒ " +
    "waiting on garage/disk I/O (not CPU).",
);
process.exit(0);
