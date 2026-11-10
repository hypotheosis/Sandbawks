# Plan 07 resume: large-overlay spill test — memory-only restore holds at scale

- **Issue:** #232 (the gating acceptance criterion). Part of epic #204 (Plan 07).
  Follows the A/B decision in #230 / #231.
- **Date:** 2026-05-31T02:11:37Z
- **Status:** gating stress test **passed for the memory-restore-recovers-overlay-writes
  leg** — memory-only `runsc restore` recovers multi-GiB overlay writes byte-for-byte, and
  overlay overflow fails *at write time*, never as a silent partial snapshot. This does
  **not** close #232: the remaining acceptance criteria (the shim code change, the
  kind-smoke re-confirm, and the fail-closed hardening below) are still open. No production
  code changed.

## Question

#231 proved memory-only restore recovers a *tiny* (`exec`-written) rootfs marker
under `--overlay2=root:self`, but flagged the real risk as an open question: does
a **large** overlay upper stay in the MemoryFile (→ `pages.img` → recovered by the
memory restore), or does it **spill** out so a memory-only restore loses it? And
if the filesystem overflows, can we tell — or do we silently restore a partial
store?

## Harness

Same standalone `runsc 20260525.0`, `--overlay2=root:self`, `--platform=systrap`,
sudo. **Sited on NVMe (btrfs), not tmpfs** — so the `root:self` filestore lands on
real disk, faithful to a production node (a tmpfs harness would have hidden any
true disk spill). Payload is `/dev/urandom` (incompressible) so size can't lie.
Restore always targets a **pristine** bundle copy (no `big.bin`, no filestore), so
a surviving file can only have come from the checkpoint. Script: `spill.sh`
(integrity tiers) + `cap_test.sh` (overflow).

## Results

### Tier 1 — 1.5 GiB: PASS

- Wrote 1,610,612,736 B (1.5 GiB) of urandom into `/big.bin` @ ~424 MB/s.
- `root:self` filestore appeared on disk as `.gvisor.filestore.srcA` (2.0 G).
- `runsc checkpoint` → **`pages.img` = 1,610,985,472 B (≈ the full write)**.
- Memory-only restore into a pristine bundle → `/big.bin` SHA-256 **identical**
  (`fced768d…91e08`). No spill.

### Tier 2 — 8 GiB: PASS (and scales)

- Wrote 8,589,934,592 B (8 GiB) @ ~309 MB/s; filestore 9.0 G on disk.
- `pages.img` = 8,590,307,328 B (≈ the full write).
- Restore took **43.6 s**; `/big.bin` SHA-256 **identical** (`d17ddae2…0584`).
- Host RAM stayed at **88 GiB available throughout** — i.e. the 8 GiB was *not*
  all RAM-resident; much lived on the filestore on disk, yet `checkpoint` still
  captured it in full. This is the mechanism: **`checkpoint` serializes the whole
  committed MemoryFile regardless of page residency** (RAM or paged to the
  filestore). The filestore is the *backing store* for those pages, not a separate
  place data can hide from the checkpoint.

### Tier 3 — overflow a hard cap (`size=256M`, write 300 MiB): fails at write time

- `--overlay2=root:self,size=256M`, then `dd … count=300`:
  - `dd` stopped **exactly at the cap** — `255+0 records out, 268,431,360 B
    (256.0 MB)` — a short write / ENOSPC delivered to the workload **live**.
  - Sandbox stayed **`running`** (no crash, no hang).
  - `runsc checkpoint` still **`rc=0`**, `pages.img` = 268,804,096 B (≈ the
    256 MB that actually fit) — a **complete, consistent** snapshot of what
    exists.

### Note on the numbers

- The filestore sizes (`2.0 G`, `9.0 G`) are `du -h` output (GiB); every other size
  in this doc is exact bytes.
- `pages.img` exceeds the payload by a **constant 372,736 B (364 KiB = exactly 91 × 4 KiB
  pages) at every tier** — 1.5 GiB, 8 GiB, *and* the 256 MB cap (1,610,985,472 −
  1,610,612,736 = 8,590,307,328 − 8,589,934,592 = 268,804,096 − 268,431,360 =
  372,736). It is fixed sentry/MemoryFile-metadata overhead, not payload-proportional —
  which is why each tier reads "≈ the full write" rather than an exact match.

## Conclusions

1. **The memory-restore leg of the gating criterion is met.** Memory-only restore
   recovers multi-GiB overlay writes byte-for-byte (tested to 8 GiB). The "spill to the
   on-disk filestore loses data" failure mode **does not occur** for `root:self`: the
   filestore *is* the MemoryFile backing, and `checkpoint` reads all committed pages.
   Because the upper is **disk-backed** (the `.gvisor.filestore`, and `pages.img` staged
   to the node disk), it is bounded by node *disk*, not RAM — the 8 GiB tier ran with host
   RAM flat at 88 GiB free. The asterisk on "holds at scale": restore still needs
   working-set memory, and the node needs disk headroom for `pages.img`. The 8 GiB tier
   passed with ~88 GiB free, but a production node has far less headroom and the
   committed upper grows monotonically, so "holds at scale" needs a **resource-headroom
   policy** (fail the SnapshotJob when the committed upper / `pages.img` would exceed a
   configured fraction of node-allocatable disk and memory) rather than risking an
   OOM/ENOSPC at restore time. Folded into the hardening list.
2. **There is no silent "overflow → partial memory store" path.** Two regimes,
   both safe: a write that fits is captured whole; a write that exceeds the overlay
   errors *at the workload, at write time*, while the eventual checkpoint stays
   complete and consistent. Overflow can never masquerade as a healthy restore.
3. **The genuine incompleteness risks are elsewhere and are detectable:**
   - **Non-overlay mounts** (bind / gofer-passthrough / out-of-overlay tmpfs) live
     outside `pages.img` — exactly the `--overlay2=none` result in #231 (writes hit
     the gofer host fs; lost by memory restore). This is a **config property known
     at pause time**, not a runtime surprise.
   - **A checkpoint that partially failed** — non-zero exit or truncated/missing
     `*.img`.

## Hardening this motivates (folded into #232 acceptance criteria)

To get fail-closed "treat the suspend as failed rather than restore a partial
store" semantics:

- **Tight checkpoint postconditions** — require `runsc checkpoint` exit 0 *and*
  non-empty `checkpoint.img` / `pages.img` / `pages_meta.img` with `pages_meta`
  consistent with `pages.img`; otherwise mark the SnapshotJob Failed.
- **Never tear down the source until a restore is proven** — keep the running
  sandbox alive until the snapshot validates (ideally a throwaway restore +
  envd health check), so a bad snapshot is a recoverable "resume failed", never
  data loss.
- **Envd rootfs fingerprint** — hash the writable rootfs into `envd-state.json`
  at pause; re-verify after restore. One deterministic oracle for *any* divergence
  (spill, truncation, corruption, missed non-overlay mount).
- **Mount-set assertion at pause** — assert every writable mount is overlay-backed
  (capturable); flag sandboxes that mount state outside the overlay.
- **Resource-headroom policy** — gate pause/restore on node disk (to stage `pages.img`)
  and restore-time memory: fail the SnapshotJob rather than stage/restore a snapshot whose
  committed upper would exceed a configured fraction of node-allocatable disk/memory. The
  8 GiB tier passed only because the host had ~88 GiB free; a busy node needs this guard.

## Reproduce

The harness lives on the author's host (it is local gVisor tooling, not committed to
the tree — same practice as the #230 changelog). It is reproduced inline here so the
results stand on their own from a fresh clone. Needs `sudo`, `docker`, `jq`, and a
`WORK` dir on **real disk** (not tmpfs, or the `root:self` filestore would be RAM-backed
and the spill question becomes meaningless).

### Setup (runsc + a pristine no-network busybox bundle)

```bash
export WORK=${WORK:-/var/tmp/gvisor-spill}     # pick a real-disk path
mkdir -p "$WORK/bin"; cd "$WORK"
curl -fsSLO https://storage.googleapis.com/gvisor/releases/release/20260525.0/x86_64/runsc
curl -fsSLO https://storage.googleapis.com/gvisor/releases/release/20260525.0/x86_64/runsc.sha512
chmod +x runsc && mv runsc bin/
sha512sum -c <(sed "s| .*| bin/runsc|" runsc.sha512)        # verify against published digest
mkdir -p pristine/rootfs
cid=$(docker create busybox:latest); docker export "$cid" | tar -C pristine/rootfs -xf -; docker rm "$cid"
( cd pristine && "$WORK/bin/runsc" spec \
  && jq '.process.args=["sh","-c","echo SENTINEL_V1 > /sentinel.txt; sync; while true; do sleep 1; done"]
        | .process.terminal=false | .root.readonly=false' config.json > c && mv c config.json )
```

### `spill.sh` — integrity tiers (memory-only restore must reproduce the write byte-for-byte)

```bash
#!/bin/bash
set -u
WORK=${WORK:-/var/tmp/gvisor-spill}
RUNSC="$WORK/bin/runsc"; ROOT=$WORK/rt; MEMCK=$WORK/memck; PRISTINE=$WORK/pristine
SIZE_MB=${1:-1536}                                          # 1.5 GiB default
FLAGS="--ignore-cgroups --network=none --platform=systrap --overlay2=root:self --root $ROOT"
human() { numfmt --to=iec --suffix=B "$1" 2>/dev/null || echo "$1"; }
fresh_bundle() { rm -rf "$1"; cp -a "$PRISTINE" "$1"; chown -R 0:0 "$1/rootfs"; }
stop_all() { for c in srcA dstB; do $RUNSC $FLAGS kill $c KILL 2>/dev/null; $RUNSC $FLAGS delete -force $c 2>/dev/null; done; }
stop_all; rm -rf "$ROOT" "$MEMCK"; mkdir -p "$ROOT" "$MEMCK"

SRCB=$WORK/bundle_src; fresh_bundle "$SRCB"
$RUNSC $FLAGS run -detach --bundle "$SRCB" srcA >/dev/null 2>&1; sleep 1
$RUNSC $FLAGS exec srcA sh -c "dd if=/dev/urandom of=/big.bin bs=1M count=${SIZE_MB} 2>&1 | tail -1; sync"
SRC_SHA=$($RUNSC $FLAGS exec srcA sha256sum /big.bin | awk '{print $1}')
SRC_SZ=$($RUNSC $FLAGS exec srcA sh -c 'wc -c < /big.bin')
echo "source: $SRC_SZ B ($(human $SRC_SZ))  sha=$SRC_SHA  filestore=$(du -sh "$SRCB/rootfs" | cut -f1)"

$RUNSC $FLAGS checkpoint -image-path "$MEMCK" -leave-running srcA; echo "checkpoint exit:$?"
PAGES=$(stat -c %s "$MEMCK/pages.img"); echo "pages.img=$PAGES ($(human $PAGES)) vs written $SRC_SZ"
[ "$PAGES" -ge $(( SRC_SZ * 8 / 10 )) ] && echo "NO spill (in MemoryFile)" || echo "SPILLED — restore will MISS data"

DSTB=$WORK/bundle_dst; fresh_bundle "$DSTB"                 # pristine: big.bin absent
$RUNSC $FLAGS create --bundle "$DSTB" dstB
time $RUNSC $FLAGS restore -image-path "$MEMCK" -detach dstB; sleep 1
DST_SHA=$($RUNSC $FLAGS exec dstB sha256sum /big.bin 2>&1 | awk '{print $1}')
[ "$DST_SHA" = "$SRC_SHA" ] && echo "INTEGRITY PASS ($SIZE_MB MiB byte-for-byte)" \
                           || echo "INTEGRITY FAIL src=$SRC_SHA dst=$DST_SHA"
stop_all
```

Run: `sudo -E bash spill.sh 1536` then `sudo -E bash spill.sh 8192` (`-E` keeps `$WORK`).

### `cap_test.sh` — overflow a hard-capped overlay (must error at write time, not spill)

```bash
#!/bin/bash
set -u
WORK=${WORK:-/var/tmp/gvisor-spill}
RUNSC="$WORK/bin/runsc"; ROOT=$WORK/rtc; BUN=$WORK/bcap; MEMCK=$WORK/mcap
sudo pkill -9 runsc 2>/dev/null; sleep 1
rm -rf "$ROOT" "$BUN" "$MEMCK"; mkdir -p "$ROOT" "$MEMCK"
cp -a "$WORK/pristine" "$BUN"; chown -R 0:0 "$BUN/rootfs"
F=(--ignore-cgroups --network=none --platform=systrap "--overlay2=root:self,size=256M" --root "$ROOT")
"$RUNSC" "${F[@]}" run -detach --bundle "$BUN" capX; sleep 1
echo "status: $("$RUNSC" "${F[@]}" state capX 2>/dev/null | jq -r .status)"
timeout 30 "$RUNSC" "${F[@]}" exec capX sh -c \
  'dd if=/dev/urandom of=/big.bin bs=1M count=300 2>&1 | tail -2; echo "size=$(wc -c </big.bin)"'  # stops AT 256 MiB
echo "status after: $("$RUNSC" "${F[@]}" state capX 2>/dev/null | jq -r .status)"   # still running
timeout 30 "$RUNSC" "${F[@]}" checkpoint -image-path "$MEMCK" -leave-running capX; echo "checkpoint rc=$?"  # rc=0
"$RUNSC" "${F[@]}" delete -force capX 2>/dev/null
```

Run: `sudo -E bash cap_test.sh`.
