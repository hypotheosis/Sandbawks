//! Plan 06 T3 (issue #185) — pause/resume perf bench harness.
//!
//! Drives the in-cluster gateway (default `http://localhost:8080`)
//! through a repeating create → upload payload → pause → resume → kill
//! loop, recording per-phase wall-clock durations and emitting a JSON
//! report at the end. CI uses the report to gate spec §13 perf
//! targets (pause p95 < 8 s, resume p95 < 2 s on ~1 GiB).
//!
//! ## Current state: real measurement (#193 + #194 both fixed)
//!
//! - **#193** (snapshot bundle empty on gVisor): fixed by changing
//!   the node DaemonSet's `memory*` glob to `*.img` so it actually
//!   captures `runsc checkpoint`'s output files. `memory.zst` now
//!   reflects the sandbox's real state.
//! - **#194** (envd exits cleanly mid-pause after N iterations):
//!   fixed by tightening the node reconciler's idempotency floor —
//!   the original "envd cleanly exits" symptom was the second of two
//!   `pause()` calls firing against a container the first call had
//!   already checkpointed (and thus terminated). The reconciler now
//!   short-circuits in-flight phases with recent `started_at` to
//!   eliminate the kube-rs Controller stale-cache re-fire race.
//!
//! Both fixes mean `BENCH_PAYLOAD_MIB` and `BENCH_ITERATIONS` are
//! free to scale up as much as the CI runner budget allows. CI
//! today runs N=3 / 16 MiB just to keep the per-PR job under ~10
//! minutes; local `just bench` already defaults to N=20 / 1024 MiB
//! so a developer can repro at issue #185's described scale.
//!
//! ## Snapshot bundle measurement (#193 follow-up)
//!
//! Each iteration reads `SnapshotJob.status.manifest.parts[*].sizeBytes`
//! from the Pause job the controller dispatched, records `bytes_saved`
//! and per-part sizes in the JSON report, and (when resume is enabled)
//! re-fetches the same manifest after resume to record `bytes_restored`
//! as a defense-in-depth check. The node-side `runsc restore` already
//! sha256-verifies every part it downloads, so a mismatch here would
//! mean the storage manifest was mutated between pause and resume;
//! we catch that explicitly so a future regression surfaces in the
//! bench output rather than only in a node DaemonSet log.
//!
//! A workload-floor gate (`BENCH_BUNDLE_FLOOR_RATIO`, default 0.1)
//! fails any pause iteration whose bundle is below 10 percent of the
//! uploaded payload bytes. This is the gate that would have caught
//! issue #193: an 18-byte `memory.zst` against a 256 MiB workload
//! fails by ~6 orders of magnitude. The default ratio is intentionally
//! loose to tolerate gVisor's pages-image compression overhead;
//! tighten via the env var if a workload reliably produces 50%+
//! bundles.
//!
//! ## Workload
//!
//! The issue body listed two options for the "~1 GiB RSS at boot"
//! workload: (a) bake a python allocator into the template's startup
//! command, or (b) `exec` after `Running` to fill RSS. Both have
//! constraints in the current codebase:
//!
//!   - The `SandboxTemplate` CRD (`deploy/crds/sandboxtemplates...yaml`)
//!     has no `command` / `args` field — the controller's
//!     `pod_adapter` only stamps the single `envd` container, which
//!     runs the envd binary directly. Adding a "startup command" path
//!     would require a CRD + controller change well out of scope.
//!   - The `POST /sandboxes/:id/exec` route (`crates/sandbawks-api/
//!     src/routes/exec.rs`) is a WebSocket bridge. Driving it cleanly
//!     from Rust would mean pulling in `tokio-tungstenite`; we'd
//!     rather not grow the dependency footprint for a workload that
//!     can be approximated otherwise.
//!
//! Instead the harness uploads a configurable payload (default
//! `BENCH_PAYLOAD_MIB=1024`) split across `payload_mib` 1-MiB files
//! at `/tmp/payload-NNNN.bin` via the existing `/files/mint` PUT
//! route. The cumulative rootfs state is what the snapshot path has
//! to move on pause, so the spec §13 latency gate is measured against
//! representative data movement. Per-file size was forced down by
//! three hard limits on the api gateway's upload path: a 30 s global
//! request timeout, tonic's 4 MiB `WriteAll` gRPC ceiling, and axum's
//! 2 MiB default body limit — all detailed in [`UPLOAD_CHUNK_BYTES`].
//! The module-level doc on `crates/sandbawks-api/src/routes/files.rs`
//! even calls this out: the file API was designed for MVP-sized
//! payloads, and "a chunked variant lands in Plan 07/09 if benchmarks
//! push back". This bench is the pushback; we work around the limits
//! by chunking here rather than blocking on a gateway refactor.
//!
//! Bytes are filled from a per-iteration splitmix64 PRNG (see
//! [`SplitMix64`]) so the rootfs delta is incompressible and the
//! snapshot sink cannot dedupe chunks across iterations. The first
//! revision of this bench used `vec![iter_byte; chunk_size]`, which
//! PR #192 review correctly flagged as pathologically compressible
//! (a 256 MiB rootfs delta of one repeating byte zstds to a few
//! hundred bytes; iteration-to-iteration dedup against a single
//! varying leading byte does the rest). The current PRNG-fill makes
//! the bench measure what its comment claims it measures.
//!
//! ## Gating
//!
//! Resume measurement is controlled by `BENCH_INCLUDE_RESUME`. The code
//! default is `0` (opt-in), so a bare `cargo run` / `just bench` measures
//! only pause; CI sets it `1` explicitly (see
//! `.github/workflows/kind-smoke.yaml`). Plan 07 (epic #204) closed the
//! runtime-side gap from #176 — the `containerd-shim-sandbawks-v1`
//! wrapper drives a memory-only `runsc restore` against a kubelet-managed
//! container (PR #234). T11 (#229) re-enabled the full
//! `pause-resume-smoke.sh` as proof the restore path is green; the bench
//! itself does not run that script — it drives its own
//! create→pause→resume loop via the gateway — but with the path proven it
//! can enforce the resume gate. When `BENCH_INCLUDE_RESUME` is unset /
//! `0`, the harness still drives `pause` end to end but stops short of
//! `/connect`, killing the paused sandbox instead — useful for isolating
//! a pause regression without a working resume path.
//!
//! ## Cluster shape (CI)
//!
//! The bench's CI workflow (`.github/workflows/bench.yaml`) brings
//! up a kind cluster on the runsc-baked `sandbawks-kind-node` image
//! with `gvisor.enabled=true` + `node.enabled=true` + a Garage S3
//! fixture — same gVisor wiring as `kind-smoke.yaml`. Pause is a
//! runsc-checkpoint operation, so this is the minimum cluster shape
//! that makes the pause measurement meaningful at all.

use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};

/// Default workload size in MiB. Issue #185 specifies "~1 GiB" so we
/// default to 1024.
const DEFAULT_PAYLOAD_MIB: usize = 1024;
/// Default iteration count. Issue #185 calls for `N=20`; CI overrides
/// via `BENCH_ITERATIONS` to fit the bench inside the 30-minute
/// workflow timeout, but the default matches the issue.
const DEFAULT_ITERATIONS: usize = 20;
/// Spec §13 pause p95 ceiling, in ms.
const DEFAULT_PAUSE_P95_MS: u128 = 8_000;
/// Spec §13 resume p95 ceiling, in ms.
const DEFAULT_RESUME_P95_MS: u128 = 2_000;
/// How long we wait for `status.envdAddress` to populate (Pod scheduled,
/// envd reachable). Mirrors the api-smoke fixture's 60s budget.
/// Default for `BENCH_ENVD_READY_BUDGET_SECS`; see [`Config`].
const ENVD_READY_BUDGET: Duration = Duration::from_secs(60);
/// How long we wait for `status.phase=Paused` after a `/pause`. The
/// spec ceiling is 8 s; we give the bench a generous multiple so a
/// flaky-but-still-passing pause shows up as a measurement (likely
/// failing the gate) rather than a hard timeout that aborts the run.
///
/// Default for `BENCH_PAUSE_BUDGET_SECS`. This 60 s default suits small
/// payloads, but a GiB-scale pause is a real `runsc checkpoint` +
/// tar/zstd + object-store upload of a ~1 GiB bundle, which legitimately
/// exceeds 60 s on a constrained runner — so CI overrides this upward
/// (see `.github/workflows/kind-smoke.yaml`). Without the override the
/// iteration hard-times-out at `Pausing` before any results are written.
const PAUSE_PHASE_BUDGET: Duration = Duration::from_secs(60);
/// Same idea for the resume side; spec ceiling 2 s, harness budget
/// 60 s. Default for `BENCH_RESUME_BUDGET_SECS`; CI overrides it upward
/// for GiB-scale restores (stage download + lazy `runsc restore` + Pod
/// lifecycle) for the same reason as the pause budget above.
const RESUME_PHASE_BUDGET: Duration = Duration::from_secs(60);
/// Status-poll cadence. Matches the api-smoke / pause-resume-smoke
/// shell loops' 1 s sleep so the apples-to-apples bench-vs-smoke
/// comparison stays valid.
const STATUS_POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Default output path for the JSON report.
const DEFAULT_RESULTS_PATH: &str = "bench-results.json";

/// Default minimum ratio of snapshot bundle bytes to uploaded payload
/// bytes. A degenerate bundle (e.g. issue #193 — the `memory*` glob
/// matched nothing on gVisor so `memory.zst` was 18 B regardless of
/// workload) failed this check by ~6 orders of magnitude. 0.1 is a
/// deliberately loose floor: incompressible random bytes (the bench's
/// splitmix64 fill) should round-trip ≈ 1:1 through tar+zstd, so a
/// bundle ≥ 10 % of payload says the snapshot path is at least
/// engaging with the workload — even if compression overhead, dedup,
/// or per-part framing trims the upper bound. Tightening to 0.5
/// would catch subtler regressions but risk false positives if
/// gVisor ever adds page-dedup or pre-compression.
const DEFAULT_BUNDLE_FLOOR_RATIO: f64 = 0.1;

/// Phase identifiers for the JSON output. Matches the keys the CI
/// workflow's gate step greps for.
const PHASE_PAUSE: &str = "pause";
const PHASE_RESUME: &str = "resume";

/// CLI / env config resolved once at startup. Every knob has an env
/// default so the harness is one-line invokable from CI.
#[derive(Debug)]
struct Config {
    api_base: String,
    api_key: String,
    namespace: String,
    template_id: String,
    iterations: usize,
    payload_mib: usize,
    include_resume: bool,
    pause_p95_ms: u128,
    resume_p95_ms: u128,
    /// See [`DEFAULT_BUNDLE_FLOOR_RATIO`]. Disable the gate by setting
    /// to `0` — useful when intentionally exercising a degenerate
    /// workload (e.g. payload_mib=0) without tripping the snapshot-
    /// emptiness regression check.
    bundle_floor_ratio: f64,
    /// Hard timeouts (NOT perf gates — those are `*_p95_ms`) for how long
    /// the harness waits for each phase to reach its target status before
    /// failing the iteration. They exist so a genuinely-wedged phase
    /// fails with a clear message instead of hanging. Defaults come from
    /// the `*_BUDGET` consts; CI raises them via `BENCH_*_BUDGET_SECS`
    /// for GiB-scale payloads, where a real checkpoint / restore takes
    /// longer than the 60 s small-payload default. Each should stay
    /// larger than its matching `*_p95_ms` gate so a slow-but-completing
    /// phase trips the perf gate (clean "p95 exceeds threshold") rather
    /// than this timeout.
    envd_ready_budget: Duration,
    pause_phase_budget: Duration,
    resume_phase_budget: Duration,
    results_path: PathBuf,
}

impl Config {
    fn from_env() -> Result<Self> {
        let api_base =
            env::var("SANDBAWKS_API_BASE").unwrap_or_else(|_| "http://localhost:8080".into());
        let api_key = env::var("SANDBAWKS_API_KEY")
            // The bench reuses the api-smoke fixture's well-known
            // key — `cluster-up` (or the kind-smoke workflow's
            // `API smoke` step) has already applied a matching
            // `TeamAPIKey` CR keyed to this hash. Falling back to it
            // means a stock cluster needs no extra setup.
            .unwrap_or_else(|_| "sbk_smoke_smoke_smoke_smoke_smoke".into());
        let namespace = env::var("SANDBAWKS_API_NAMESPACE").unwrap_or_else(|_| "default".into());
        // Same default as the api-smoke fixture so a stock cluster
        // has the template already applied.
        let template_id =
            env::var("BENCH_TEMPLATE_ID").unwrap_or_else(|_| "api-smoke-template".into());
        let iterations = parse_env_usize("BENCH_ITERATIONS", DEFAULT_ITERATIONS)?;
        if iterations == 0 {
            bail!("BENCH_ITERATIONS must be > 0");
        }
        let payload_mib = parse_env_usize("BENCH_PAYLOAD_MIB", DEFAULT_PAYLOAD_MIB)?;
        let include_resume = parse_env_bool("BENCH_INCLUDE_RESUME", false)?;
        let pause_p95_ms = parse_env_u128("BENCH_PAUSE_P95_MS", DEFAULT_PAUSE_P95_MS)?;
        let resume_p95_ms = parse_env_u128("BENCH_RESUME_P95_MS", DEFAULT_RESUME_P95_MS)?;
        let bundle_floor_ratio =
            parse_env_f64("BENCH_BUNDLE_FLOOR_RATIO", DEFAULT_BUNDLE_FLOOR_RATIO)?;
        if !(0.0..=1.0).contains(&bundle_floor_ratio) {
            bail!("BENCH_BUNDLE_FLOOR_RATIO must be in [0, 1], got {bundle_floor_ratio}");
        }
        let envd_ready_budget =
            parse_env_duration_secs("BENCH_ENVD_READY_BUDGET_SECS", ENVD_READY_BUDGET)?;
        let pause_phase_budget =
            parse_env_duration_secs("BENCH_PAUSE_BUDGET_SECS", PAUSE_PHASE_BUDGET)?;
        let resume_phase_budget =
            parse_env_duration_secs("BENCH_RESUME_BUDGET_SECS", RESUME_PHASE_BUDGET)?;
        let results_path = env::var("BENCH_RESULTS_PATH")
            .unwrap_or_else(|_| DEFAULT_RESULTS_PATH.into())
            .into();
        Ok(Self {
            api_base,
            api_key,
            namespace,
            template_id,
            iterations,
            payload_mib,
            include_resume,
            pause_p95_ms,
            resume_p95_ms,
            bundle_floor_ratio,
            envd_ready_budget,
            pause_phase_budget,
            resume_phase_budget,
            results_path,
        })
    }
}

fn parse_env_usize(key: &str, default: usize) -> Result<usize> {
    match env::var(key) {
        Ok(v) => v
            .parse::<usize>()
            .with_context(|| format!("{key} must be a non-negative integer (got {v:?})")),
        Err(_) => Ok(default),
    }
}

fn parse_env_u128(key: &str, default: u128) -> Result<u128> {
    match env::var(key) {
        Ok(v) => v
            .parse::<u128>()
            .with_context(|| format!("{key} must be a non-negative integer (got {v:?})")),
        Err(_) => Ok(default),
    }
}

fn parse_env_f64(key: &str, default: f64) -> Result<f64> {
    match env::var(key) {
        Ok(v) => v
            .parse::<f64>()
            .with_context(|| format!("{key} must be a floating-point value (got {v:?})")),
        Err(_) => Ok(default),
    }
}

/// Parse an integer number of seconds from `key` into a [`Duration`],
/// falling back to `default`. Used for the per-phase wait budgets so CI
/// can extend them for GiB-scale payloads without a code change.
fn parse_env_duration_secs(key: &str, default: Duration) -> Result<Duration> {
    match env::var(key) {
        Ok(v) => {
            let secs = v.parse::<u64>().with_context(|| {
                format!("{key} must be a non-negative integer number of seconds (got {v:?})")
            })?;
            Ok(Duration::from_secs(secs))
        }
        Err(_) => Ok(default),
    }
}

fn parse_env_bool(key: &str, default: bool) -> Result<bool> {
    match env::var(key) {
        Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" | "" => Ok(false),
            other => bail!("{key} must be a boolean-ish value (got {other:?})"),
        },
        Err(_) => Ok(default),
    }
}

/// Per-iteration sample. Captured into the JSON report so a regression
/// hunt can see the raw measurements rather than just the percentiles.
///
/// Issue #193 motivated `bytes_bundle` + `parts`: the snapshot path
/// can pass a duration-only gate while producing a degenerate bundle
/// (the original symptom — 18 B `memory.zst` vs a 256 MiB workload —
/// trivially fit inside the spec §13 latency target). Surfacing
/// per-iteration bundle bytes in the JSON makes that class of bug
/// visible at a glance, and the workload-floor gate (see
/// [`DEFAULT_BUNDLE_FLOOR_RATIO`]) makes it fail CI.
#[derive(Debug, Clone, Serialize)]
struct Sample {
    iteration: usize,
    duration_ms: u128,
    /// Sum of `SnapshotJob.status.manifest.parts[*].sizeBytes` for the
    /// pause this iteration produced. For resume samples, the same
    /// value re-fetched from the same manifest — see
    /// [`fetch_pause_bundle_manifest`] for the read path, and
    /// [`run_iteration`] for the resume-side cross-check.
    ///
    /// `None` is reserved for cases the bench cannot measure (e.g. the
    /// SnapshotJob is gone before we can read it), not a code path
    /// taken on the happy path.
    #[serde(skip_serializing_if = "Option::is_none")]
    bytes_bundle: Option<u64>,
    /// Per-part breakdown of the snapshot bundle for diagnostic visibility.
    /// Populated for pause samples; resume samples re-use the pause
    /// manifest so we omit the duplicated per-part map there to keep
    /// the JSON tidy.
    #[serde(skip_serializing_if = "Option::is_none")]
    parts: Option<BTreeMap<String, u64>>,
}

/// Aggregated stats for one phase across `iterations` samples.
#[derive(Debug, Serialize)]
struct PhaseStats {
    phase: String,
    p50_ms: u128,
    p95_ms: u128,
    max_ms: u128,
    samples: Vec<Sample>,
}

impl PhaseStats {
    fn from_samples(phase: &str, samples: Vec<Sample>) -> Result<Self> {
        if samples.is_empty() {
            bail!("phase '{phase}' has no samples; cannot compute percentiles");
        }
        let mut sorted: Vec<u128> = samples.iter().map(|s| s.duration_ms).collect();
        sorted.sort_unstable();
        let p50 = percentile(&sorted, 0.50);
        let p95 = percentile(&sorted, 0.95);
        // `sorted` is non-empty per the early bail above.
        let max = *sorted.last().expect("sorted samples non-empty");
        Ok(Self {
            phase: phase.into(),
            p50_ms: p50,
            p95_ms: p95,
            max_ms: max,
            samples,
        })
    }
}

/// Linear-interpolated percentile over a sorted `&[u128]`. `p` is in
/// [0, 1]. Inline rather than pulling a stats crate to keep the bench's
/// dependency surface as small as the issue body asked for.
fn percentile(sorted: &[u128], p: f64) -> u128 {
    debug_assert!(!sorted.is_empty(), "caller checked non-empty");
    debug_assert!((0.0..=1.0).contains(&p), "p must be in [0, 1]");
    if sorted.len() == 1 {
        return sorted[0];
    }
    // `idx` is a fractional rank into `sorted`. We linearly
    // interpolate between the two nearest samples — the textbook
    // "type 7" percentile (R / numpy default).
    let idx = p * ((sorted.len() - 1) as f64);
    let lo = idx.floor() as usize;
    let hi = idx.ceil() as usize;
    if lo == hi {
        return sorted[lo];
    }
    let frac = idx - lo as f64;
    let lo_v = sorted[lo] as f64;
    let hi_v = sorted[hi] as f64;
    (lo_v + frac * (hi_v - lo_v)).round() as u128
}

/// Issue #193 follow-up: compare each pause iteration's recorded
/// snapshot bundle bytes against a floor derived from the uploaded
/// payload, returning a breach string per under-floor iteration.
///
/// The floor is `payload_mib * UPLOAD_CHUNK_BYTES * floor_ratio`. A
/// ratio of `0` or `payload_mib == 0` disables the gate (returning an
/// empty Vec): a zero-byte workload trivially can't fail an
/// emptiness check, and an operator-disabled gate should leave the
/// JSON report intact without spurious breaches.
///
/// Phases other than `PHASE_PAUSE` are ignored — resume samples re-use
/// the pause bundle, so they'd report the same bytes and double-count
/// any breach.
fn compute_bundle_breaches(
    payload_mib: usize,
    floor_ratio: f64,
    phases: &[PhaseStats],
) -> Vec<String> {
    if payload_mib == 0 || floor_ratio <= 0.0 {
        return Vec::new();
    }
    let payload_bytes = (payload_mib as u64).saturating_mul(UPLOAD_CHUNK_BYTES as u64);
    // f64 round-trip is fine here: payload_bytes <= ~10^12 and
    // ratio <= 1.0, so the product fits in f64's 53-bit mantissa
    // without loss for any plausible bench input.
    let floor_bytes = (payload_bytes as f64 * floor_ratio) as u64;
    let mut breaches: Vec<String> = Vec::new();
    for phase in phases {
        if phase.phase != PHASE_PAUSE {
            continue;
        }
        for sample in &phase.samples {
            let Some(bytes) = sample.bytes_bundle else {
                continue;
            };
            if bytes < floor_bytes {
                breaches.push(format!(
                    "pause iter {} bundle bytes {bytes} < floor {floor_bytes} \
                     (payload {payload_bytes}, ratio {floor_ratio:.2})",
                    sample.iteration,
                ));
            }
        }
    }
    breaches
}

/// Top-level JSON report layout. Schema-stable enough that the CI gate
/// step and any future trend dashboard can both parse it.
#[derive(Debug, Serialize)]
struct BenchReport {
    /// ISO 8601 wall-clock timestamp at the start of the run. Useful
    /// for trend analysis when multiple JSON artifacts get stitched
    /// together.
    started_at: String,
    iterations: usize,
    payload_mib: usize,
    include_resume: bool,
    pause_p95_threshold_ms: u128,
    resume_p95_threshold_ms: u128,
    /// Echoed back into the report so a downstream dashboard can show
    /// the gate's configured floor alongside the observed ratios.
    bundle_floor_ratio: f64,
    phases: Vec<PhaseStats>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Config::from_env().context("resolving bench configuration from env vars")?;
    eprintln!(
        "bench: api_base={} namespace={} template_id={} iterations={} payload_mib={} include_resume={}",
        cfg.api_base,
        cfg.namespace,
        cfg.template_id,
        cfg.iterations,
        cfg.payload_mib,
        cfg.include_resume,
    );
    eprintln!(
        "bench: budgets envd_ready={}s pause={}s resume={}s | gates pause_p95={}ms resume_p95={}ms",
        cfg.envd_ready_budget.as_secs(),
        cfg.pause_phase_budget.as_secs(),
        cfg.resume_phase_budget.as_secs(),
        cfg.pause_p95_ms,
        cfg.resume_p95_ms,
    );

    let client = Client::builder()
        // Per-operation timeouts rather than a whole-request ceiling.
        // The first revision used `.timeout(300s)` for the entire
        // request, but `.timeout()` covers the streaming body too —
        // and PR #192 review observed that a 1 GiB upload through
        // `kubectl port-forward` on a slow runner can come close to
        // (or exceed) that. Splitting into:
        //
        //   - connect_timeout(10s): bound the initial TCP handshake.
        //     A wedged apiserver / port-forward fails fast here.
        //   - read_timeout(120s): bound any single read from the
        //     server. The pause path can hold the response open for
        //     many seconds on a real 1 GiB workload; 120s gives that
        //     comfortable headroom without letting a stuck connection
        //     wedge the bench forever.
        //
        // No whole-request `timeout()`: the bench already has
        // per-phase budgets (`PAUSE_PHASE_BUDGET` / `RESUME_PHASE_
        // BUDGET` / `ENVD_READY_BUDGET`) that wrap each HTTP call
        // pair, so a stuck phase surfaces there with a clearer
        // message than a generic "request timeout".
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(120))
        .build()
        .context("building reqwest client")?;

    wait_for_readyz(&client, &cfg.api_base).await?;

    let started_at = Utc::now().to_rfc3339();
    let mut pause_samples: Vec<Sample> = Vec::with_capacity(cfg.iterations);
    let mut resume_samples: Vec<Sample> = Vec::with_capacity(cfg.iterations);

    for iter in 1..=cfg.iterations {
        eprintln!("bench: iteration {iter}/{}", cfg.iterations);
        let measurement = run_iteration(&client, &cfg, iter).await?;
        pause_samples.push(Sample {
            iteration: iter,
            duration_ms: measurement.pause_ms,
            bytes_bundle: Some(measurement.pause_bytes),
            parts: Some(measurement.pause_parts.clone()),
        });
        if let Some(resume_ms) = measurement.resume_ms {
            resume_samples.push(Sample {
                iteration: iter,
                duration_ms: resume_ms,
                // Resume re-uses the Pause manifest — the per-part map
                // would just duplicate what's already under the pause
                // sample, so we elide it for the resume sample to keep
                // the JSON tidy.
                bytes_bundle: measurement.resume_bytes,
                parts: None,
            });
        }
        eprintln!(
            "bench: iteration {iter} pause={}ms bytes={} resume={}",
            measurement.pause_ms,
            measurement.pause_bytes,
            measurement
                .resume_ms
                .map(|v| format!("{v}ms"))
                .unwrap_or_else(|| "skipped".into()),
        );
    }

    let mut phases = vec![PhaseStats::from_samples(PHASE_PAUSE, pause_samples)?];
    if cfg.include_resume {
        phases.push(PhaseStats::from_samples(PHASE_RESUME, resume_samples)?);
    }

    let report = BenchReport {
        started_at,
        iterations: cfg.iterations,
        payload_mib: cfg.payload_mib,
        include_resume: cfg.include_resume,
        pause_p95_threshold_ms: cfg.pause_p95_ms,
        resume_p95_threshold_ms: cfg.resume_p95_ms,
        bundle_floor_ratio: cfg.bundle_floor_ratio,
        phases,
    };

    let json = serde_json::to_string_pretty(&report).context("serializing bench report to JSON")?;
    tokio::fs::write(&cfg.results_path, &json)
        .await
        .with_context(|| format!("writing bench results to {}", cfg.results_path.display()))?;
    eprintln!("bench: wrote {}", cfg.results_path.display());
    println!("{json}");

    let mut breaches: Vec<String> = Vec::new();
    for phase in &report.phases {
        let threshold = if phase.phase == PHASE_PAUSE {
            cfg.pause_p95_ms
        } else if phase.phase == PHASE_RESUME {
            cfg.resume_p95_ms
        } else {
            // Future-proof: an unknown phase is informational only.
            continue;
        };
        if phase.p95_ms > threshold {
            breaches.push(format!(
                "{} p95={}ms exceeds threshold {}ms",
                phase.phase, phase.p95_ms, threshold,
            ));
        }
    }

    // Bundle-emptiness gate (issue #193 follow-up). Extracted into a
    // pure helper so the floor math is unit-testable without standing
    // up a cluster.
    breaches.extend(compute_bundle_breaches(
        cfg.payload_mib,
        cfg.bundle_floor_ratio,
        &report.phases,
    ));

    if !breaches.is_empty() {
        eprintln!("bench: PERF GATE FAILED");
        for b in &breaches {
            eprintln!("bench:   {b}");
        }
        bail!("perf gate regression: {}", breaches.join("; "));
    }
    eprintln!("bench: perf gate PASSED");
    Ok(())
}

/// Wait for the gateway's `/readyz` to return 200. The api-smoke /
/// pause-resume-smoke fixtures both do this; we mirror the cadence
/// (1 s polls, 30 attempts) so a slightly-late port-forward attach
/// doesn't blow up the bench.
async fn wait_for_readyz(client: &Client, api_base: &str) -> Result<()> {
    for attempt in 1..=30 {
        match client.get(format!("{api_base}/readyz")).send().await {
            Ok(resp) if resp.status().is_success() => {
                eprintln!("bench: gateway /readyz OK (attempt {attempt})");
                return Ok(());
            }
            Ok(resp) => eprintln!(
                "bench: /readyz attempt {attempt} returned {}",
                resp.status()
            ),
            Err(e) => eprintln!("bench: /readyz attempt {attempt}: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    bail!("gateway never reached /readyz after 30s")
}

/// Per-iteration timings + snapshot bundle measurements.
struct IterationMeasurement {
    pause_ms: u128,
    /// Resume wall-clock. Measured whenever `BENCH_INCLUDE_RESUME=1` (the
    /// CI default; the code default is `0`). `None` when the gate is off,
    /// in which case the iteration kills the paused sandbox without
    /// exercising `/connect`.
    resume_ms: Option<u128>,
    /// Total bytes the pause snapshot wrote to storage, summed across
    /// `SnapshotJob.status.manifest.parts[*].sizeBytes`. Issue #193:
    /// the pre-fix glob produced an 18 B `memory.zst` regardless of
    /// payload size; this field plus the workload-floor gate would
    /// have surfaced that as a CI failure rather than a silent pass.
    pause_bytes: u64,
    /// Per-part breakdown of the pause bundle for diagnostic visibility.
    pause_parts: BTreeMap<String, u64>,
    /// Total bytes summed from the same Pause manifest after resume.
    /// Should equal `pause_bytes` (the controller's sha256 verification
    /// in [`sandbawks_node::gvisor`] enforces byte-identity on the
    /// resume read side; a divergence here would mean the manifest was
    /// mutated between pause and resume, which is the regression we
    /// want surfaced). `None` when resume was skipped this iteration.
    resume_bytes: Option<u64>,
}

/// Run one create → upload → pause → (resume?) → kill cycle, returning
/// the timed phases. Each iteration uses a fresh sandbox so iterations
/// are independent — a regression in one doesn't poison the next's
/// measurements (and the controller's per-sandbox state is fully
/// torn down by the trailing kill).
async fn run_iteration(client: &Client, cfg: &Config, iter: usize) -> Result<IterationMeasurement> {
    let sandbox_id = create_sandbox(client, cfg).await?;
    // Best-effort cleanup if anything below this point fails — without
    // it a mid-iteration error would orphan Sandbox CRs and pile up
    // pods across iterations.
    let cleanup = SandboxCleanup {
        client: client.clone(),
        api_base: cfg.api_base.clone(),
        api_key: cfg.api_key.clone(),
        sandbox_id: sandbox_id.clone(),
    };

    let result = async {
        wait_for_envd_ready(cfg, &sandbox_id).await?;

        if cfg.payload_mib > 0 {
            upload_payload(client, cfg, &sandbox_id, cfg.payload_mib, iter).await?;
        }

        let pause_started = Instant::now();
        pause_sandbox(client, cfg, &sandbox_id).await?;
        wait_for_phase(cfg, &sandbox_id, "Paused", cfg.pause_phase_budget).await?;
        let pause_ms = pause_started.elapsed().as_millis();

        // Once the sandbox reaches Paused, the controller has folded
        // the manifest into Sandbox.status.snapshot_ref and the
        // SnapshotJob's status.manifest is populated + immutable. Read
        // the per-part sizes for the iteration's bytes_saved metric.
        // Issue #193: this is the measurement that would have caught
        // the empty-bundle regression on the bench's first CI run.
        let (pause_bytes, pause_parts) = fetch_pause_bundle_manifest(cfg, &sandbox_id).await?;

        let (resume_ms, resume_bytes) = if cfg.include_resume {
            let resume_started = Instant::now();
            connect_sandbox(client, cfg, &sandbox_id).await?;
            wait_for_phase(cfg, &sandbox_id, "Running", cfg.resume_phase_budget).await?;
            // Verify envd has actually re-attached — the connect
            // response above already implies a populated address (the
            // handler polls before returning), but we re-read the
            // status to time "phase=Running + envd reachable" together
            // per the issue body.
            wait_for_envd_ready(cfg, &sandbox_id).await?;
            let resume_ms = resume_started.elapsed().as_millis();
            // Re-fetch the SAME Pause SnapshotJob's manifest and assert
            // its part-size sum hasn't drifted. The node-side restore
            // path (`sandbawks_node::gvisor::resume`) already verifies
            // per-part sha256 + the aggregate manifest sha256, so a
            // size drift here would mean the manifest was mutated in
            // storage between pause and resume — defense-in-depth.
            let (resume_total, _) = fetch_pause_bundle_manifest(cfg, &sandbox_id).await?;
            if resume_total != pause_bytes {
                bail!(
                    "bundle byte mismatch between pause and resume: pause={pause_bytes} resume={resume_total} \
                     (manifest mutated mid-iteration?)",
                );
            }
            (Some(resume_ms), Some(resume_total))
        } else {
            (None, None)
        };

        Ok::<_, anyhow::Error>(IterationMeasurement {
            pause_ms,
            resume_ms,
            pause_bytes,
            pause_parts,
            resume_bytes,
        })
    }
    .await;

    // Cleanup only on success. If the iteration failed we leave the
    // sandbox in place so the workflow's `Dump diagnostics on failure`
    // step can dump its envd logs / describe the pod / read the
    // SnapshotJob status. Without this skip the cleanup `POST /kill`
    // fires immediately, the controller cascade-deletes the Sandbox
    // CR, kubelet tears down the Pod, and by the time diagnostics
    // run the most informative logs (envd's tracing output) are
    // unrecoverable. CI tears the whole cluster down at the end of
    // the job anyway, so the orphaned sandbox costs us nothing.
    if result.is_ok() {
        cleanup.run().await;
    } else {
        eprintln!(
            "bench: leaving sandbox {} in place for diagnostics (iteration failed)",
            sandbox_id,
        );
    }
    result
}

/// RAII-ish cleanup helper. We can't put this in `Drop` because
/// teardown is async; instead we spell it out at the end of each
/// iteration. Errors from cleanup are logged and swallowed — the
/// measurement run we just completed is the load-bearing output.
///
/// ## Safety of kill-during-Pausing
///
/// PR #192 review flagged that if `wait_for_phase("Paused")` times
/// out, this helper posts `/kill` on a sandbox that's mid-Pausing.
/// Verified against the controller code:
///
///   - `POST /sandboxes/:id/kill` calls `sandbox_ops().delete()`
///     (`crates/sandbawks-api/src/routes/sandboxes.rs`), which does a
///     `DeleteParams::background()` DELETE on the Sandbox CR.
///   - Background propagation removes the CR immediately; owned
///     children (the in-flight `SnapshotJob`, the backing Pod) are
///     cascade-deleted by the apiserver's GC.
///   - The controller's state machine has no `(Pausing, *)` →
///     `Terminating` transition, but that doesn't matter — the
///     deletion bypasses the state machine entirely. The reconciler
///     receives a delete event and exits the per-sandbox loop.
///
/// What's *not* cleaned up: an in-flight `runsc checkpoint`
/// subprocess on the node DaemonSet may still complete and write
/// orphan objects to S3, since the node-side run is decoupled from
/// the SnapshotJob CR's lifetime. That's a known S3-GC follow-up
/// (Plan 06 T15+, per the comment in `pause-resume-smoke.sh`) and
/// not something the bench introduces.
struct SandboxCleanup {
    client: Client,
    api_base: String,
    api_key: String,
    sandbox_id: String,
}

impl SandboxCleanup {
    async fn run(self) {
        let url = format!("{}/sandboxes/{}/kill", self.api_base, self.sandbox_id);
        match self
            .client
            .post(&url)
            .header("X-API-Key", &self.api_key)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {}
            Ok(resp) => eprintln!(
                "bench: kill sandbox {} returned {} (cleanup; non-fatal)",
                self.sandbox_id,
                resp.status()
            ),
            Err(e) => eprintln!(
                "bench: kill sandbox {} errored: {e} (cleanup; non-fatal)",
                self.sandbox_id
            ),
        }
    }
}

/// Body of `POST /sandboxes`. Mirrors the api-smoke fixture's
/// payload — `templateID` is the seed template; `timeout` is in
/// seconds and overrides the template's default.
#[derive(Serialize)]
struct CreateBody<'a> {
    #[serde(rename = "templateID")]
    template_id: &'a str,
    timeout: u32,
}

/// Subset of the create response we care about. The gateway returns
/// other fields too (E2B-compat envelope) but we only need the id.
#[derive(Deserialize)]
struct CreateResp {
    #[serde(rename = "sandboxID")]
    sandbox_id: String,
}

async fn create_sandbox(client: &Client, cfg: &Config) -> Result<String> {
    let url = format!("{}/sandboxes", cfg.api_base);
    let body = CreateBody {
        template_id: &cfg.template_id,
        // Generous timeout — the bench's create→upload→pause window
        // is dominated by the upload, which on 1 GiB can run a few
        // minutes through `kubectl port-forward`. The template's own
        // default is 300, so anything ≥ that buys us headroom without
        // changing controller behavior.
        timeout: 600,
    };
    let resp = client
        .post(&url)
        .header("X-API-Key", &cfg.api_key)
        .json(&body)
        .send()
        .await
        .context("POST /sandboxes")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        bail!("POST /sandboxes returned {status}: {text}");
    }
    let parsed: CreateResp = resp
        .json()
        .await
        .context("parsing /sandboxes response body")?;
    Ok(parsed.sandbox_id)
}

/// Poll `status.envdAddress` via the gateway. We bypass kubectl by
/// using the gateway's own listing — `GET /sandboxes/:id` returns a
/// shape compatible with the SDK's `Sandbox.get_info`, which includes
/// `phase` and `envdAddress`. Mirroring the api-smoke fixture's 60 s
/// budget.
///
/// We accept either the canonical CamelCase fields or the SDK-facing
/// snake_case aliases the api gateway emits, so a future tweak to the
/// gateway's response shape doesn't silently desync the bench.
async fn wait_for_envd_ready(cfg: &Config, sandbox_id: &str) -> Result<()> {
    wait_for_envd_via_kubectl(cfg, sandbox_id, cfg.envd_ready_budget).await
}

/// `wait_for_phase` and `wait_for_envd_ready` both poll the Sandbox
/// CR via `kubectl get sandbox`. The gateway doesn't expose a "raw
/// status" endpoint, and going through the apiserver mirrors what the
/// existing smoke fixtures do — keeping the bench's polling semantics
/// observably identical to the fixtures the issue acceptance criteria
/// reference.
///
/// We shell out rather than embedding a kube client because (a) the
/// bench's dependency footprint stays tiny, (b) the bench already
/// assumes a working `KUBECONFIG` in CI (same as the smokes), and
/// (c) the polling is low-frequency.
async fn wait_for_envd_via_kubectl(cfg: &Config, sandbox_id: &str, budget: Duration) -> Result<()> {
    let deadline = Instant::now() + budget;
    loop {
        let addr = kubectl_status_field(cfg, sandbox_id, ".status.envdAddress").await?;
        if !addr.trim().is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "status.envdAddress never populated for {sandbox_id} within {:?}",
                budget
            );
        }
        tokio::time::sleep(STATUS_POLL_INTERVAL).await;
    }
}

async fn wait_for_phase(
    cfg: &Config,
    sandbox_id: &str,
    target: &str,
    budget: Duration,
) -> Result<()> {
    let deadline = Instant::now() + budget;
    loop {
        let phase = kubectl_status_field(cfg, sandbox_id, ".status.phase").await?;
        if phase.trim() == target {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "status.phase for {sandbox_id} never reached {target} within {:?} (last={phase:?})",
                budget
            );
        }
        tokio::time::sleep(STATUS_POLL_INTERVAL).await;
    }
}

/// `kubectl -n <ns> get sandbox <id> -o jsonpath='{<field>}'`. Returns
/// the raw string the apiserver emitted (empty when the field isn't
/// populated yet).
///
/// `field` must be a kubectl-jsonpath expression — kubectl's dialect
/// requires a leading dot (`.status.envdAddress`), unlike the more
/// common RFC-style "$.status.envdAddress". Calling this with
/// `"status.envdAddress"` (no dot) fails at runtime with
/// `unrecognized identifier status` rather than a missing-field empty
/// string, so the leading dot is non-optional.
async fn kubectl_status_field(cfg: &Config, sandbox_id: &str, field: &str) -> Result<String> {
    debug_assert!(
        field.starts_with('.'),
        "kubectl jsonpath expects a leading dot; got {field:?}",
    );
    // `tokio::process::Command` so the bench stays inside the runtime
    // without blocking the executor on a sync `Command::output`.
    let output = tokio::process::Command::new("kubectl")
        .args([
            "-n",
            &cfg.namespace,
            "get",
            "sandbox",
            sandbox_id,
            "-o",
            &format!("jsonpath={{{field}}}"),
        ])
        .output()
        .await
        .with_context(|| format!("kubectl get sandbox {sandbox_id} -o jsonpath={{{field}}}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Treat "NotFound" as a transient — the gateway returns 201
        // before the controller has finished applying the CR in some
        // races; the next poll will succeed.
        if stderr.contains("NotFound") {
            return Ok(String::new());
        }
        return Err(anyhow!("kubectl get sandbox {sandbox_id} failed: {stderr}"));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Fetch the manifest of the Pause SnapshotJob the controller dispatched
/// for `sandbox_id`, returning the total bundle bytes + per-part sizes.
///
/// The controller names jobs `<sandbox>-<pause|resume>-<epoch-millis>`
/// (see `crates/sandbawks-controller/src/reconcile.rs::build_snapshot_job`),
/// so multiple in-flight or completed jobs may share the same prefix.
/// We list every SnapshotJob in the namespace, filter to those with
/// `spec.sandboxName == sandbox_id` and `spec.mode == "Pause"`, then
/// pick the most recently created. Once the Sandbox reaches
/// `phase=Paused` the matching job is in a terminal phase and its
/// `status.manifest` is immutable, so the read is race-free.
///
/// Shelling out to `kubectl get -o json` mirrors the rest of the
/// bench's kubectl usage (see [`kubectl_status_field`]) — no embedded
/// kube client, no extra dependency surface.
///
/// Returns `(total_bytes, parts_by_name)`. Errors if no matching
/// SnapshotJob exists or its `status.manifest` is empty — both
/// surface as iteration failures so the bench leaves the sandbox in
/// place for diagnostic dumping (matching the existing failure
/// contract in [`run_iteration`]).
async fn fetch_pause_bundle_manifest(
    cfg: &Config,
    sandbox_id: &str,
) -> Result<(u64, BTreeMap<String, u64>)> {
    let output = tokio::process::Command::new("kubectl")
        .args(["-n", &cfg.namespace, "get", "snapshotjobs", "-o", "json"])
        .output()
        .await
        .context("kubectl get snapshotjobs -o json")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("kubectl get snapshotjobs failed: {stderr}");
    }

    // `kubectl get -o json` always emits a List wrapper with `items`,
    // even when there's a single resource. Parse into serde_json::Value
    // rather than a typed struct so the bench stays decoupled from
    // sandbawks-types' SnapshotJob shape (out-of-workspace crate).
    let list: serde_json::Value =
        serde_json::from_slice(&output.stdout).context("parse kubectl snapshotjobs JSON")?;
    let items = list
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("kubectl snapshotjobs output missing 'items' array"))?;

    // Walk every job, retain only this sandbox's Pause jobs, and pick
    // the one with the largest `metadata.creationTimestamp` (RFC 3339
    // sorts lexicographically). At most one Pause job is in flight at
    // a time, but historical completions stay until the Sandbox is
    // cleaned up, so the filter has to cope with N≥1.
    let mut latest: Option<(&str, &serde_json::Value)> = None;
    for item in items {
        let spec = match item.get("spec") {
            Some(s) => s,
            None => continue,
        };
        if spec.get("sandboxName").and_then(|v| v.as_str()) != Some(sandbox_id) {
            continue;
        }
        if spec.get("mode").and_then(|v| v.as_str()) != Some("Pause") {
            continue;
        }
        let created = item
            .get("metadata")
            .and_then(|m| m.get("creationTimestamp"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        match latest {
            None => latest = Some((created, item)),
            Some((cur, _)) if created > cur => latest = Some((created, item)),
            _ => {}
        }
    }

    let job = latest
        .ok_or_else(|| anyhow!("no Pause SnapshotJob found for sandbox {sandbox_id}"))?
        .1;

    let parts = job
        .get("status")
        .and_then(|s| s.get("manifest"))
        .and_then(|m| m.get("parts"))
        .and_then(|p| p.as_array())
        .ok_or_else(|| {
            anyhow!(
                "Pause SnapshotJob for sandbox {sandbox_id} has no status.manifest.parts \
                 (controller may not have written the manifest yet, or job is still in flight)",
            )
        })?;

    let mut by_name: BTreeMap<String, u64> = BTreeMap::new();
    let mut total: u64 = 0;
    for part in parts {
        let name = part
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("SnapshotJob part missing 'name': {part}"))?;
        let size = part
            .get("sizeBytes")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow!("SnapshotJob part '{name}' missing 'sizeBytes'"))?;
        by_name.insert(name.to_owned(), size);
        total = total.saturating_add(size);
    }
    Ok((total, by_name))
}

/// `POST /files/mint` body. `method` is the verb the gateway should
/// authorize the returned URL for — for the bench we always want PUT.
#[derive(Serialize)]
struct MintBody<'a> {
    sandbox_id: &'a str,
    path: &'a str,
    method: &'a str,
    ttl: u32,
}

#[derive(Deserialize)]
struct MintResp {
    url: String,
}

/// splitmix64 — a tiny, dependency-free PRNG used to fill the bench
/// upload buffer with high-entropy bytes.
///
/// The first revision of this bench filled each chunk with a single
/// repeating byte. PR #192 review caught that this was the worst
/// possible input for a snapshot path: zstd compresses 256 MiB of a
/// constant byte to a few hundred bytes, and a content-addressed
/// chunker dedupes iterations 2..N to almost nothing across runs that
/// vary only by their leading byte. Net effect — the bench measured
/// pause on a "trivially compressible, mostly-deduped" rootfs delta
/// rather than on a realistic 256 MiB of state.
///
/// splitmix64 produces statistically-random `u64`s; serialised
/// little-endian, the output passes zstd's entropy floor and resists
/// content-addressed dedup across iterations (each iteration is
/// seeded by its iteration index, so byte streams are independent).
///
/// We inline the algorithm rather than depending on `rand` because
/// the bench's dependency footprint is meant to stay small per the
/// issue body ("`reqwest` + `serde_json` + an inline p50/p95 calc")
/// and splitmix64 is ~10 lines.
///
/// Reference: Steele, Lea, Flood, "Fast Splittable Pseudorandom
/// Number Generators" (OOPSLA 2014). Constants from the public-
/// domain reference impl.
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Construct a generator deterministic in `seed`. The seed is
    /// mixed through a single splitmix64 step before being used so
    /// adjacent seeds (`iter = 1, 2, 3, …`) produce uncorrelated
    /// byte streams instead of barely-different ones.
    fn new(seed: u64) -> Self {
        let mut s = Self {
            state: seed.wrapping_add(0x9E3779B97F4A7C15),
        };
        // Discard one output so the first `next_u64` doesn't return
        // a tightly-coupled function of the raw seed.
        let _ = s.next_u64();
        s
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    /// Fill `buf` with high-entropy bytes. Each `next_u64` covers 8
    /// bytes; a tail shorter than 8 is filled by truncating the
    /// final output.
    fn fill_bytes(&mut self, buf: &mut [u8]) {
        for window in buf.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            let n = window.len().min(bytes.len());
            window[..n].copy_from_slice(&bytes[..n]);
        }
    }
}

/// Per-file upload size. Chosen to stay well below all three of the
/// api gateway's hard limits on the upload path:
///
///   - 30 s global request timeout (`middleware::timeout_layer(30)`
///     in `crates/sandbawks-api/src/bin/sandbawks-api.rs`) — a 256
///     MiB PUT through `kubectl port-forward` blew through this in
///     the first CI run of the bench;
///   - tonic's default `max_decoding_message_size` (4 MiB) for the
///     envd `Filesystem.WriteAll` RPC the gateway forwards to;
///   - axum's `DefaultBodyLimit` (2 MiB) — bypassed today because the
///     upload handler takes `body: Body` directly, but a future
///     handler refactor could re-enable it without notice.
///
/// 1 MiB sits comfortably under all three. The module-level docstring
/// on `crates/sandbawks-api/src/routes/files.rs` explicitly calls out
/// that the file API was designed for MVP-sized payloads ("A chunked
/// variant lands in Plan 07/09 if benchmarks push back") — this bench
/// is precisely that pushback, but we work around it by chunking
/// here rather than blocking the bench on a gateway refactor.
const UPLOAD_CHUNK_BYTES: usize = 1024 * 1024;

/// Upload `payload_mib` MiB of high-entropy bytes (see [`SplitMix64`]
/// for the why) to the sandbox by issuing `payload_mib` sequential
/// 1-MiB PUTs to `/tmp/payload-NNNN.bin`. The cumulative rootfs state
/// is what the snapshot path has to move on pause; per-file size
/// matters only to the upload transport, not to the measurement.
///
/// Earlier revisions of this function streamed one big PUT of
/// `payload_mib` MiB. That hit the api gateway's 30 s request timeout
/// the first time CI exercised it. The chunked approach keeps each
/// PUT short and lets the bench scale the rootfs state up without
/// touching the gateway.
async fn upload_payload(
    client: &Client,
    cfg: &Config,
    sandbox_id: &str,
    payload_mib: usize,
    iter: usize,
) -> Result<()> {
    // Each iteration gets its own PRNG seeded by `iter` so byte
    // streams across iterations are uncorrelated; a content-addressed
    // snapshot sink cannot dedupe one iteration's payload against
    // another's, and zstd cannot trivially compress any single
    // iteration's body.
    let mut rng = SplitMix64::new(iter as u64);
    // Buffer reused across files — saves `payload_mib` allocations
    // per iteration on big workloads.
    let mut buf = vec![0u8; UPLOAD_CHUNK_BYTES];

    for file_idx in 0..payload_mib {
        rng.fill_bytes(&mut buf);
        // Zero-pad the index so a future debug session on a 1024-file
        // run sees the files in numeric (rather than lexicographic)
        // order on `ls`.
        let path = format!("/tmp/payload-{file_idx:04}.bin");
        upload_one(client, cfg, sandbox_id, &path, &buf).await?;
    }
    Ok(())
}

/// Mint a signed upload URL for `path` and PUT `body` to it. One
/// chunk of the bench's per-iteration payload — see [`upload_payload`]
/// for the chunking rationale.
async fn upload_one(
    client: &Client,
    cfg: &Config,
    sandbox_id: &str,
    path: &str,
    body: &[u8],
) -> Result<()> {
    let mint_url = format!("{}/files/mint", cfg.api_base);
    let mint_body = MintBody {
        sandbox_id,
        path,
        method: "PUT",
        ttl: 600,
    };
    let mint_resp = client
        .post(&mint_url)
        .header("X-API-Key", &cfg.api_key)
        .json(&mint_body)
        .send()
        .await
        .context("POST /files/mint (PUT)")?;
    if !mint_resp.status().is_success() {
        let status = mint_resp.status();
        let text = mint_resp.text().await.unwrap_or_default();
        bail!("POST /files/mint returned {status}: {text}");
    }
    let mint: MintResp = mint_resp.json().await.context("parsing mint response")?;
    let upload_url = format!("{}{}", cfg.api_base, mint.url);

    // `.body(Vec<u8>)` is fine here: each chunk is ≤ UPLOAD_CHUNK_BYTES,
    // so the entire body fits in memory comfortably and reqwest can
    // set Content-Length without our help. Cloning is cheap relative
    // to the network round-trip — the alternative (a `&[u8]` body)
    // requires a static lifetime reqwest can't satisfy across an
    // await point.
    let upload_resp = client
        .put(&upload_url)
        .header("X-API-Key", &cfg.api_key)
        .body(body.to_vec())
        .send()
        .await
        .context("PUT mint-issued upload URL")?;
    if upload_resp.status() != StatusCode::NO_CONTENT {
        let status = upload_resp.status();
        let text = upload_resp.text().await.unwrap_or_default();
        bail!("PUT upload returned {status}: {text}");
    }
    Ok(())
}

async fn pause_sandbox(client: &Client, cfg: &Config, sandbox_id: &str) -> Result<()> {
    let url = format!("{}/sandboxes/{sandbox_id}/pause", cfg.api_base);
    let resp = client
        .post(&url)
        .header("X-API-Key", &cfg.api_key)
        .send()
        .await
        .context("POST /sandboxes/:id/pause")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        bail!("POST /sandboxes/{sandbox_id}/pause returned {status}: {text}");
    }
    Ok(())
}

async fn connect_sandbox(client: &Client, cfg: &Config, sandbox_id: &str) -> Result<()> {
    let url = format!("{}/sandboxes/{sandbox_id}/connect", cfg.api_base);
    let resp = client
        .post(&url)
        .header("X-API-Key", &cfg.api_key)
        .send()
        .await
        .context("POST /sandboxes/:id/connect")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        bail!("POST /sandboxes/{sandbox_id}/connect returned {status}: {text}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_single_sample() {
        assert_eq!(percentile(&[42], 0.0), 42);
        assert_eq!(percentile(&[42], 0.5), 42);
        assert_eq!(percentile(&[42], 1.0), 42);
    }

    #[test]
    fn percentile_two_samples_interpolates() {
        // p=0 -> first, p=1 -> last, p=0.5 -> midpoint.
        let s = vec![10, 20];
        assert_eq!(percentile(&s, 0.0), 10);
        assert_eq!(percentile(&s, 1.0), 20);
        assert_eq!(percentile(&s, 0.5), 15);
    }

    #[test]
    fn percentile_matches_numpy_type7() {
        // numpy's `np.percentile([10, 20, 30, 40], 95)` returns 38.5,
        // which rounds to 39 in our integer impl. The same input at
        // p50 returns 25.
        let s = vec![10, 20, 30, 40];
        assert_eq!(percentile(&s, 0.95), 39);
        assert_eq!(percentile(&s, 0.50), 25);
    }

    #[test]
    fn phase_stats_rejects_empty_samples() {
        let err = PhaseStats::from_samples("pause", vec![]).unwrap_err();
        assert!(err.to_string().contains("no samples"));
    }

    #[test]
    fn phase_stats_computes_aggregates() {
        let samples = vec![
            Sample {
                iteration: 1,
                duration_ms: 100,
                bytes_bundle: None,
                parts: None,
            },
            Sample {
                iteration: 2,
                duration_ms: 200,
                bytes_bundle: None,
                parts: None,
            },
            Sample {
                iteration: 3,
                duration_ms: 300,
                bytes_bundle: None,
                parts: None,
            },
        ];
        let stats = PhaseStats::from_samples("pause", samples).unwrap();
        assert_eq!(stats.phase, "pause");
        assert_eq!(stats.max_ms, 300);
        assert_eq!(stats.p50_ms, 200);
        // p95 over (100,200,300) with type-7 interp: index =
        // 0.95 * 2 = 1.9, so 200 + 0.9*(300-200) = 290.
        assert_eq!(stats.p95_ms, 290);
        assert_eq!(stats.samples.len(), 3);
    }

    fn mk_pause_stats(bytes_per_iter: &[Option<u64>]) -> PhaseStats {
        let samples = bytes_per_iter
            .iter()
            .enumerate()
            .map(|(i, b)| Sample {
                iteration: i + 1,
                duration_ms: 1,
                bytes_bundle: *b,
                parts: None,
            })
            .collect::<Vec<_>>();
        PhaseStats::from_samples(PHASE_PAUSE, samples).unwrap()
    }

    #[test]
    fn bundle_gate_skipped_when_payload_zero() {
        // payload_mib=0 means the bench wasn't asked to upload anything;
        // a 0-byte bundle is the expected outcome, not a regression.
        let phases = vec![mk_pause_stats(&[Some(0)])];
        let breaches = compute_bundle_breaches(0, 0.5, &phases);
        assert!(breaches.is_empty(), "got {breaches:?}");
    }

    #[test]
    fn bundle_gate_skipped_when_ratio_zero() {
        // Operator can disable the gate by setting BENCH_BUNDLE_FLOOR_
        // RATIO=0; we should not synthesize spurious breaches even if
        // every iteration reports a tiny bundle.
        let phases = vec![mk_pause_stats(&[Some(1)])];
        let breaches = compute_bundle_breaches(64, 0.0, &phases);
        assert!(breaches.is_empty(), "got {breaches:?}");
    }

    #[test]
    fn bundle_gate_passes_when_bytes_above_floor() {
        // 64 MiB payload * 0.5 ratio = 32 MiB floor. 64 MiB bundle is
        // way above, so no breach.
        let bundle = 64u64 * 1024 * 1024;
        let phases = vec![mk_pause_stats(&[Some(bundle), Some(bundle), Some(bundle)])];
        let breaches = compute_bundle_breaches(64, 0.5, &phases);
        assert!(breaches.is_empty(), "got {breaches:?}");
    }

    #[test]
    fn bundle_gate_catches_issue_193_regression() {
        // This is the exact regression #193 reported: 18-byte bundle
        // against a 256 MiB payload. With the default 0.1 floor the
        // gate must fail loudly.
        let payload_mib = 256;
        let phases = vec![mk_pause_stats(&[Some(18), Some(18), Some(18), Some(18)])];
        let breaches = compute_bundle_breaches(payload_mib, 0.1, &phases);
        assert_eq!(
            breaches.len(),
            4,
            "every iteration should breach; got {breaches:?}",
        );
        // Spot-check that the breach string carries the diagnostic
        // payload a CI operator needs (iteration #, observed bytes,
        // computed floor, ratio).
        let first = &breaches[0];
        assert!(first.contains("pause iter 1"), "{first}");
        assert!(first.contains("18"), "{first}");
        assert!(first.contains("0.10"), "{first}");
    }

    #[test]
    fn bundle_gate_ignores_resume_phase() {
        // Resume samples re-use the pause bundle bytes; gating on them
        // too would double-count any breach. The pure helper must
        // restrict itself to PHASE_PAUSE.
        let pause = mk_pause_stats(&[Some(64 * 1024 * 1024)]);
        let resume_samples = vec![Sample {
            iteration: 1,
            duration_ms: 1,
            // Imagine a tiny resume re-fetch; the gate must not flag
            // it. (In practice resume_bytes mirrors pause_bytes, but
            // even a hypothetical drift shouldn't surface here.)
            bytes_bundle: Some(1),
            parts: None,
        }];
        let resume = PhaseStats::from_samples(PHASE_RESUME, resume_samples).unwrap();
        let breaches = compute_bundle_breaches(64, 0.5, &[pause, resume]);
        assert!(breaches.is_empty(), "got {breaches:?}");
    }

    #[test]
    fn bundle_gate_ignores_samples_without_recorded_bytes() {
        // A future bench mode might skip the manifest read (e.g.
        // resume-only re-runs); `None` should be silently ignored
        // rather than spuriously failing.
        let phases = vec![mk_pause_stats(&[None, None])];
        let breaches = compute_bundle_breaches(64, 0.5, &phases);
        assert!(breaches.is_empty(), "got {breaches:?}");
    }

    #[test]
    fn sample_omits_unset_optional_fields() {
        // The schema-stable JSON keys we always emit — iteration and
        // duration_ms — must serialise even when the issue-#193 bundle
        // fields are unset. This pins the `#[serde(skip_serializing_if
        // = "Option::is_none")]` on `bytes_bundle` and `parts` so a
        // future refactor that drops the attribute surfaces here.
        let sample = Sample {
            iteration: 7,
            duration_ms: 1234,
            bytes_bundle: None,
            parts: None,
        };
        let json = serde_json::to_string(&sample).unwrap();
        assert!(json.contains("\"iteration\":7"));
        assert!(json.contains("\"duration_ms\":1234"));
        assert!(
            !json.contains("bytes_bundle"),
            "bytes_bundle:null should be elided, got {json}",
        );
        assert!(
            !json.contains("parts"),
            "parts:null should be elided, got {json}"
        );
    }

    #[test]
    fn parse_env_bool_truthy() {
        // Cargo runs tests within a binary on a shared process, so
        // mutating a fixed `BENCH_TEST_BOOL` key would race against
        // any future test that touches the same key (or against the
        // ambient env, however unlikely). Scope the key to this
        // test's name + module path so the binding is unique even
        // if the suite grows. PR #192 review flagged the original
        // shared key as future-flake risk.
        const KEY: &str = "BENCH_TEST_BOOL_PARSE_ENV_BOOL_TRUTHY";
        std::env::set_var(KEY, "1");
        assert!(parse_env_bool(KEY, false).unwrap());
        std::env::set_var(KEY, "yes");
        assert!(parse_env_bool(KEY, false).unwrap());
        std::env::set_var(KEY, "0");
        assert!(!parse_env_bool(KEY, true).unwrap());
        std::env::remove_var(KEY);
        assert!(parse_env_bool(KEY, true).unwrap());
        assert!(!parse_env_bool(KEY, false).unwrap());
    }

    #[test]
    fn splitmix64_is_deterministic() {
        let mut a = SplitMix64::new(42);
        let mut b = SplitMix64::new(42);
        for _ in 0..32 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn splitmix64_diverges_across_seeds() {
        // Two adjacent seeds must not produce correlated streams —
        // the whole point of seed-mixing in `new` is to give each
        // iteration index an independent stream.
        let mut a = SplitMix64::new(1);
        let mut b = SplitMix64::new(2);
        let mut diff = 0;
        for _ in 0..128 {
            if a.next_u64() != b.next_u64() {
                diff += 1;
            }
        }
        assert!(
            diff >= 120,
            "expected ≥120 of 128 outputs to differ; got {diff}"
        );
    }

    #[test]
    fn splitmix64_fill_bytes_is_high_entropy() {
        // A statistically-random byte stream over 64 KiB should hit
        // nearly every value in [0, 255]. A constant-byte fill (the
        // pre-PR-192-review behavior) would fail this with a
        // single-bucket histogram.
        let mut rng = SplitMix64::new(0xdeadbeef);
        let mut buf = vec![0u8; 65_536];
        rng.fill_bytes(&mut buf);
        let mut hist = [0u32; 256];
        for &b in &buf {
            hist[b as usize] += 1;
        }
        let nonzero = hist.iter().filter(|&&c| c > 0).count();
        assert!(
            nonzero >= 240,
            "expected ≥240 of 256 byte buckets populated; got {nonzero}",
        );
    }

    #[test]
    fn splitmix64_fill_bytes_handles_short_tail() {
        // splitmix64 emits 8 bytes per step; a tail shorter than 8
        // must be filled without panicking or running off the end.
        let mut rng = SplitMix64::new(7);
        let mut buf = vec![0u8; 13];
        rng.fill_bytes(&mut buf);
        // No assertion on values — coverage is enough. If
        // `fill_bytes` mis-handled the tail, this would have
        // panicked on the slice copy.
    }
}
