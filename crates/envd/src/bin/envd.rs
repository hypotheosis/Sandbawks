//! `envd` binary entrypoint.
//!
//! Boots the in-sandbox agent: parses CLI flags, initialises tracing,
//! loads the pause/resume snapshot from `--state-file` (when present),
//! mounts the `filesystem` and `process` gRPC services on `--listen`,
//! mounts the `/__state`, `/__quiesce`, and `/__rearm` HTTP routes on
//! `--http-listen`, and serves until SIGTERM/SIGINT triggers a graceful
//! drain.
//!
//! # Service surface
//!
//! Two gRPC services are wired in here — `filesystem.Filesystem` and
//! `process.Process` — plus one secondary HTTP route. PTY behaviour is
//! *not* a separate gRPC service: the upstream E2B `process.proto`
//! folds PTY allocation/resize/I-O into the existing `Process` RPCs
//! (`Start` with a `pty` field, `SendInput`/`StreamInput` with a `Pty`
//! payload variant, `Update` for resize). The [`crate::pty`] module
//! is a building block for `ProcessService`, not a server in its own
//! right.
//!
//! ## `GET /__state`
//!
//! The HTTP route returns the live [`EnvdState`] as a JSON body. It
//! exists so the node DaemonSet's `GvisorAdapter::pause` can capture
//! envd's in-process bookkeeping (open processes, allocated PTYs,
//! registered filesystem watches) just before `runsc checkpoint`
//! freezes the sandbox. The route is mounted on a second listener
//! (default `[::]:49984`) rather than multiplexed onto the gRPC
//! port — h2c content-type sniffing would work but the chart already
//! mounts ports per-container and a dedicated HTTP listener keeps
//! tonic's HTTP/2 framing isolated from a casual `curl`.
//!
//! ## `POST /__quiesce`, `POST /__rearm`
//!
//! Two telemetry-rearm routes share the same layer-free listener (issue
//! #401). The node drives `POST /__quiesce` just before `runsc checkpoint`
//! to tear the OTLP exporters down without blocking on a (frequently dead)
//! collector, and `POST /__rearm` after `runsc restore` to rebuild the
//! provider stack from the restored environment. They sit on the
//! `:49984` listener — deliberately free of the gRPC trace/metrics
//! `tower` layers — precisely so the rearm transitions keep working when
//! the gRPC surface is mid-trouble. Both are `POST`: the destructive
//! quiesce is never coupled to `GET /__state`'s side-effect-free capture.
//!
//! # Snapshot lifecycle
//!
//! - **Startup:** if `--state-file` exists, the file is parsed and the
//!   snapshot's three arrays are handed to the live services via
//!   [`ProcessService::with_seed`] / [`FilesystemService::with_seed`].
//!   The seed is held read-only on the service and is **not** surfaced
//!   by `/__state` or the shutdown snapshot writer — that decoupling is
//!   what stops the seed from growing monotonically across pause/resume
//!   cycles (issue #178). The seed remains addressable through
//!   `boot_seed_*` accessors for the future rehydration path (re-mint
//!   live entries on resume; deferred work tracked by #178).
//! - **Shutdown:** after the gRPC drain completes, the binary captures
//!   a fresh `EnvdState` directly from the live services' registries
//!   and writes it back to `--state-file`. Issue #174 retired the old
//!   `SnapshotState` indirection: capture is now a thin walk over the
//!   service handles via [`ProcessService::collect_process_snapshots`],
//!   [`ProcessService::collect_pty_snapshots`], and
//!   [`FilesystemService::collect_watch_snapshots`] — all of which
//!   report live registry state only, never the boot seed.
//!
//! # Shutdown sequencing
//!
//! The drain follows the four steps the issue spec calls out:
//!
//! 1. Receive SIGTERM or SIGINT.
//! 2. Tonic stops accepting new connections; in-flight RPCs are given
//!    up to 30 s to finish (the cap is enforced here, not inside
//!    tonic, by racing the serve task against
//!    [`tokio::time::timeout`]).
//! 3. The snapshot is flushed to `--state-file`.
//! 4. The process exits with code 0 on the happy path, non-zero on
//!    any I/O / serve failure.

use std::ffi::OsString;
use std::net::SocketAddr;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::{FromRef, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::{Parser, Subcommand};
use sandbawks_observability::RearmHandle;
use tokio::signal::unix::{signal, SignalKind};
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

use envd::filesystem::FilesystemService;
use envd::fingerprint::RootfsFingerprint;
use envd::process::ProcessService;
use envd::proto::filesystem::filesystem_server::FilesystemServer;
use envd::proto::process::process_server::ProcessServer;
use envd::state::{EnvdState, SCHEMA_VERSION};

/// Default gRPC listen address. Matches the port the controller-side
/// kubelet `containerPort` and Service definition target in
/// `deploy/helm/` so a fresh envd Pod is reachable without per-deploy
/// overrides. The IPv6 wildcard `[::]` accepts both IPv6 and (on Linux,
/// where `net.ipv6.bindv6only=0` is the default) IPv4-mapped clients, so
/// it serves dual-stack and IPv6-only clusters alike where `0.0.0.0`
/// would answer only IPv4.
const DEFAULT_LISTEN: &str = "[::]:49983";

/// Default HTTP listen address for the `/__state` route the node
/// DaemonSet's pause path probes. Held one port up from the gRPC
/// listener so the two surfaces don't compete for accept-time framing.
/// Binds the IPv6 wildcard `[::]` for the same dual-stack reach as
/// [`DEFAULT_LISTEN`].
const DEFAULT_HTTP_LISTEN: &str = "[::]:49984";

/// Default pause/resume snapshot path.
///
/// Lives under `/var/tmp` (mode `1777`) rather than `/var/lib/envd`
/// (root-owned `0755`) because the workload container runs envd as the
/// image's declared `USER`, which is routinely non-root (e.g. the
/// `compound-sandbox` image ships `USER dev`). On shutdown envd creates
/// this path's parent and writes the snapshot; a root-owned parent made
/// that `create_dir_all` fail with `EACCES`, crashing warm-pool Pods on
/// auto-pause. `/var/tmp` is a plain directory on the container's overlay
/// rootfs — captured by the `runsc checkpoint` rootfs-delta just as
/// `/var/lib/envd` was — so the restore path is unchanged; only the
/// permission constraint is lifted. What world-writability buys is narrow
/// but sufficient: any UID may create the `envd/` subdir under the sticky
/// `1777` root. The snapshot file itself lives one level deeper inside that
/// envd-owned `envd/` dir, whose mode follows the process umask (typically
/// `0755`), so `state.json` is not itself world-accessible.
const DEFAULT_STATE_FILE: &str = "/var/tmp/envd/state.json";

/// Default filesystem root for the `Filesystem` service. Production
/// uses `/`; tests override this with a tempdir.
const DEFAULT_ROOT: &str = "/";

/// Compile-time build identifier reported by `GET /__version`.
///
/// Baked into the binary's read-only code/data pages at compile time. It
/// defaults to the crate version (`CARGO_PKG_VERSION`); a build may stamp a
/// distinct value by setting `SANDBAWKS_ENVD_BUILD_ID` in the compiler's
/// environment, which `option_env!` reads at compile time (not at run time).
///
/// This is the observable seam for the capture-in-checkpoint acceptance proof
/// (issues #466 / #470): because the value lives in the binary's pages rather
/// than in an env var or a file, querying it after `runsc restore` distinguishes
/// a snapshot whose envd pages were restored from `pages.img` (reports the
/// *captured* id) from one that re-faulted a host-swapped binary (reports the
/// *host* id). A `match` is used rather than `unwrap_or` because `Option::unwrap_or`
/// is not yet usable in a `const` initializer on this toolchain.
const BUILD_ID: &str = match option_env!("SANDBAWKS_ENVD_BUILD_ID") {
    Some(id) => id,
    None => env!("CARGO_PKG_VERSION"),
};

/// Upper bound on how long we wait for in-flight RPCs to drain after
/// receiving SIGTERM/SIGINT. Past this point the serve task is dropped
/// and we proceed to the snapshot-flush step regardless.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Bound on `POST /__quiesce`'s stale-provider shutdown (issue #401).
///
/// `RearmHandle::quiesce` tears the old OTLP providers down on detached
/// threads and abandons any still running past this deadline — exactly the
/// case the pause path must survive, since the collector is frequently
/// unreachable by the time the node drives a checkpoint. Kept short so the
/// node's pre-checkpoint quiesce never stalls on a dead collector; the
/// no-op layer swap that stops *new* emission has already happened by the
/// time this deadline matters.
const QUIESCE_SHUTDOWN_CAP: Duration = Duration::from_secs(5);

/// Worker threads for the dedicated gRPC serving runtime (#560).
///
/// The gRPC server runs on its own Tokio runtime so the pre-checkpoint drain
/// can force-close every live connection by tearing that runtime down (tonic
/// spawns per-connection tasks detached, so there is no other way to close them
/// while keeping the process alive). Two workers comfortably drive a single
/// sandbox's RPC load; the footprint is deliberately small since this runtime
/// is rebuilt on each drain.
const GRPC_WORKER_THREADS: usize = 2;

/// Grace given to the dedicated gRPC runtime's [`Runtime::shutdown_timeout`]
/// when draining (#560). The drain *wants* connections gone, so this is short:
/// it bounds how long aborting the detached connection tasks may take before
/// the runtime is dropped regardless.
const GRPC_RUNTIME_SHUTDOWN_GRACE: Duration = Duration::from_millis(500);

/// Bound on `POST /__quiesce`'s pre-checkpoint gRPC connection drain (#560).
/// The handler waits this long for the serve supervisor to ack a fresh,
/// connection-free listener before returning `200` regardless, so a stuck
/// rebind can never pin the node's pause.
const QUIESCE_DRAIN_CAP: Duration = Duration::from_secs(5);

/// In-sandbox agent: filesystem, process, and PTY RPCs over gRPC.
//
// Field-level docs below are surfaced verbatim by `clap` as the
// per-flag `--help` text — keep them user-facing rather than
// implementation-oriented.
#[derive(Debug, Parser)]
#[command(name = "envd", version)]
struct Cli {
    /// Optional one-shot subcommand. When omitted, envd runs as the
    /// long-lived in-sandbox agent and the flags below take effect; when
    /// present, envd performs the subcommand and exits, ignoring the
    /// server flags (which keep their defaults). `Option<Subcommand>` is
    /// the clap idiom for "subcommand or default behaviour" — `None`
    /// preserves the historical bare-`envd` server invocation.
    #[command(subcommand)]
    command: Option<Command>,

    /// Address the gRPC server binds to. Defaults to the IPv6 wildcard
    /// `[::]`, which exposes the service on every interface of the
    /// sandbox Pod across both address families (IPv6 plus IPv4-mapped on
    /// Linux); loopback-only (`[::1]:...` / `127.0.0.1:...`) is
    /// appropriate for unit/integration tests.
    #[arg(long, default_value = DEFAULT_LISTEN)]
    listen: SocketAddr,

    /// Address the HTTP `/__state` listener binds to. Targeted by the
    /// node DaemonSet's pause path; the gRPC traffic goes to
    /// `--listen` instead.
    #[arg(long, default_value = DEFAULT_HTTP_LISTEN)]
    http_listen: SocketAddr,

    /// Path to the pause/resume snapshot file. Loaded on startup when
    /// it exists; rewritten on shutdown regardless.
    #[arg(long, default_value = DEFAULT_STATE_FILE)]
    state_file: PathBuf,

    /// Root directory the `Filesystem` service serves from. Every
    /// request path is resolved relative to this prefix and rejected
    /// if it escapes the root.
    #[arg(long, default_value = DEFAULT_ROOT)]
    root: PathBuf,

    /// Logging filter. Falls back to the `RUST_LOG` environment
    /// variable, then to `info` if neither is set. Accepts the full
    /// `tracing-subscriber` env-filter syntax (e.g.
    /// `envd=debug,tonic=info`).
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    log_level: String,
}

/// One-shot subcommands that run to completion and exit, rather than
/// booting the long-lived agent.
#[derive(Debug, Subcommand)]
enum Command {
    /// Install this running `envd` binary to `<dest>`.
    ///
    /// Copies `/proc/self/exe` — the kernel's handle on the running
    /// executable — to `<dest>`, producing a byte-identical copy with no
    /// dependency on a shell or any userland. This is the shell-free
    /// replacement for the node DaemonSet's former
    /// `cp /usr/local/bin/envd <dest>` staging step (issue #268): because
    /// envd installs *itself*, the carrier image no longer needs `cp` (or
    /// the `python:3.12-slim` base that provided it) and shrinks to a
    /// `scratch` image holding only the static binary.
    SelfInstall {
        /// Destination path the binary is installed to. Its parent
        /// directory is created if missing.
        dest: PathBuf,
    },
}

/// Capture-side handle to the live services.
///
/// Holds cheap-Clone snapshot handles to the same `ProcessService` and
/// `FilesystemService` the gRPC server is serving from. Used by the
/// axum `/__state` route and by [`write_snapshot`] on shutdown to walk
/// the live registries directly — no intermediate `RestoreResult`
/// indirection, no mutex over a duplicate copy.
#[derive(Clone)]
struct SnapshotHandle {
    process: ProcessService,
    filesystem: FilesystemService,
    /// Writable rootfs path (envd's `--root`). Fingerprinted at capture
    /// so the snapshot carries the resume divergence oracle (#236).
    root: PathBuf,
    /// envd's `--state-file`. Excluded from the fingerprint walk: the
    /// default deployment writes it *inside* `--root` and it embeds the
    /// fingerprint itself, so hashing it would be self-referential.
    state_file: PathBuf,
}

/// Shared state for the layer-free `:49984` HTTP listener.
///
/// Bundles the two cheap-`Clone` handles its routes need: the
/// [`SnapshotHandle`] behind `GET /__state`, and the [`RearmHandle`] behind
/// the `POST /__quiesce` / `POST /__rearm` telemetry-rearm routes (issue
/// #401). The [`FromRef`] impls below let each handler extract just the slice
/// it needs via `State<SnapshotHandle>` / `State<RearmHandle>`, so a route
/// never sees a handle it has no business touching.
/// Drives the pre-checkpoint gRPC connection drain (#560) from `POST /__quiesce`.
///
/// `/__quiesce` rides the layer-free `:49984` listener, which is independent of
/// the gRPC `:49983` listener being drained — so it can request a drain and
/// block for the ack without the chicken-and-egg of closing the connection it
/// is answering on. [`drain`](Self::drain) bumps an epoch the gRPC serve
/// supervisor (see `run_grpc_loop`) observes; the supervisor force-closes every
/// live gRPC connection (by tearing its runtime down) and rebinds a fresh,
/// connection-free listener, then publishes the epoch back on `done`.
#[derive(Clone)]
struct DrainHandle {
    /// Epoch bumped per drain request. `Arc` because `watch::Sender` is not
    /// `Clone` but [`AppState`] (hence this handle) must be.
    request: Arc<tokio::sync::watch::Sender<u64>>,
    /// Last epoch the serve supervisor has finished rebinding for.
    done: tokio::sync::watch::Receiver<u64>,
}

impl DrainHandle {
    /// Request a gRPC connection drain and block until the serve supervisor has
    /// rebound a fresh, connection-free listener — bounded by `cap` so a wedged
    /// supervisor can never pin the pause. Returns whether the drain was acked
    /// within `cap`.
    async fn drain(&self, cap: Duration) -> bool {
        let target = {
            let mut target = 0;
            self.request.send_modify(|epoch| {
                *epoch += 1;
                target = *epoch;
            });
            target
        };
        let mut done = self.done.clone();
        // `matches!` consumes the result in place so the `watch::Ref` borrow of
        // `done` doesn't outlive it. `Ok(Ok(_))` = acked within `cap`; an outer
        // `Err` (timeout) or inner `Err` (supervisor dropped the sender) is a
        // miss we log and proceed past.
        let acked = matches!(
            tokio::time::timeout(cap, done.wait_for(|acked| *acked >= target)).await,
            Ok(Ok(_))
        );
        if !acked {
            tracing::warn!(
                target,
                cap = ?cap,
                "pre-checkpoint gRPC connection drain not acked in time; \
                 proceeding with checkpoint regardless (#560)"
            );
        }
        acked
    }

    /// A pre-acked handle for tests that exercise the HTTP routes without a live
    /// serve supervisor: `done` already exceeds any epoch [`drain`](Self::drain)
    /// can request, so `drain` returns immediately.
    #[cfg(test)]
    fn disabled() -> Self {
        let (req, _req_rx) = tokio::sync::watch::channel(0u64);
        let (_done_tx, done) = tokio::sync::watch::channel(u64::MAX);
        Self {
            request: Arc::new(req),
            done,
        }
    }
}

#[derive(Clone)]
struct AppState {
    snapshot: SnapshotHandle,
    rearm: RearmHandle,
    drain: DrainHandle,
}

// `FromRef` is axum's sub-state extractor: with `.with_state(AppState)`, a
// handler asking for `State<SnapshotHandle>` (or `State<RearmHandle>`) gets a
// clone of that field. This is the idiomatic alternative to one monolithic
// state type every handler must destructure.
impl FromRef<AppState> for SnapshotHandle {
    fn from_ref(state: &AppState) -> Self {
        state.snapshot.clone()
    }
}

impl FromRef<AppState> for RearmHandle {
    fn from_ref(state: &AppState) -> Self {
        state.rearm.clone()
    }
}

impl FromRef<AppState> for DrainHandle {
    fn from_ref(state: &AppState) -> Self {
        state.drain.clone()
    }
}

/// Build the layer-free HTTP router served on `--http-listen` (`:49984`).
///
/// Extracted from [`run`] so tests construct the exact route table the
/// production listener serves — no drift between what is tested and what
/// ships. Deliberately carries none of the gRPC trace/metrics `tower`
/// layers, so these routes keep responding even when the gRPC surface is
/// mid-trouble (e.g. a quiesced exporter): that isolation is the whole
/// reason the node drives rearm over this listener rather than over gRPC.
fn http_router(snapshot: SnapshotHandle, rearm: RearmHandle, drain: DrainHandle) -> Router {
    Router::new()
        .route("/__state", get(state_handler))
        .route("/__version", get(version_handler))
        .route("/__quiesce", post(quiesce_handler))
        .route("/__rearm", post(rearm_handler))
        .with_state(AppState {
            snapshot,
            rearm,
            drain,
        })
}

impl SnapshotHandle {
    /// Capture an [`EnvdState`] from the underlying services.
    ///
    /// Each `collect_*_snapshots` call briefly locks its own registry;
    /// the locks are not held across one another, so a long-running
    /// `Start` on the process side will not stall a watcher
    /// registration on the filesystem side.
    ///
    /// The rootfs fingerprint (#236) is computed on a blocking thread —
    /// it streams every file under `--root` through SHA-256, which would
    /// otherwise stall the async reactor. A compute failure is logged
    /// and the fingerprint left absent rather than failing the capture:
    /// pause should still produce a snapshot, and resume treats an absent
    /// fingerprint as "no oracle available" (it only fails closed on a
    /// genuine *mismatch*, never on absence).
    async fn capture(&self) -> EnvdState {
        EnvdState {
            version: SCHEMA_VERSION,
            processes: self.process.collect_process_snapshots().await,
            ptys: self.process.collect_pty_snapshots().await,
            watches: self.filesystem.collect_watch_snapshots().await,
            rootfs_fingerprint: self.fingerprint_rootfs().await,
        }
    }

    /// Compute the writable-rootfs fingerprint on the blocking pool,
    /// returning `None` (with a warning logged) if it cannot be computed.
    /// The `--state-file` is excluded so the snapshot's own bytes never
    /// feed back into the fingerprint.
    async fn fingerprint_rootfs(&self) -> Option<RootfsFingerprint> {
        let root = self.root.clone();
        let state_file = self.state_file.clone();
        match tokio::task::spawn_blocking(move || {
            RootfsFingerprint::compute(&root, &[state_file.as_path()])
        })
        .await
        {
            Ok(Ok(fingerprint)) => {
                tracing::debug!(
                    digest = %fingerprint.digest,
                    entries = fingerprint.entries,
                    "captured rootfs fingerprint"
                );
                Some(fingerprint)
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    root = %self.root.display(),
                    error = %e,
                    "rootfs fingerprint capture failed; snapshot will carry no resume oracle"
                );
                None
            }
            Err(e) => {
                tracing::error!(error = %e, "rootfs fingerprint task panicked");
                None
            }
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // One-shot subcommands do a small synchronous job (a file copy) and exit.
    // They run *before* the Tokio runtime and `init_telemetry` are built:
    // there is no reason a `cp`-equivalent should spin up three OTLP batch
    // exporters only to tear them straight back down. No `tracing` subscriber
    // is installed on this path, so the failure diagnostic goes straight to
    // stderr via `eprintln!`, and `process::exit` sets the exit code without
    // the `Termination` impl printing the error a second time.
    if let Some(command) = cli.command {
        if let Err(err) = run_command(command) {
            eprintln!("envd command failed: {err:?}");
            std::process::exit(1);
        }
        return Ok(());
    }

    // #467: before any server work, relocate onto the checkpoint-captured
    // tmpfs and re-exec from there. Serving from a `medium: Memory` volume
    // puts envd's mmap'd code pages in the sandbox MemoryFile, so a
    // memory-only `runsc restore` rehydrates them from `pages.img`
    // independently of the (possibly upgraded) node-staged host binary —
    // the fix for snapshots being invalidated by every envd upgrade. See
    // docs/superpowers/specs/2026-06-10-capture-in-checkpoint-envd-design.md.
    // A no-op when the controller did not request relocation; the `?` fails
    // closed (non-zero exit) rather than silently serving a sandbox whose
    // snapshots could never be restored.
    relocate_onto_capture_surface()?;

    // Server path. Funnel the `--log-level` flag into the `EnvFilter` that
    // `init_telemetry` builds. The flag already defaults from `RUST_LOG`
    // (flag > `RUST_LOG` > `info`), and `init_telemetry`'s filter is sourced
    // from `RUST_LOG`, so exporting the resolved value makes the two agree
    // and preserves the documented precedence. Validate it first with
    // `try_new` so an invalid filter still fails fast — `init_telemetry`
    // would otherwise silently fall back to `info`, dropping the original
    // `init_tracing`'s rejecting behaviour.
    EnvFilter::try_new(&cli.log_level)
        .with_context(|| format!("invalid log filter {:?}", cli.log_level))?;
    std::env::set_var("RUST_LOG", &cli.log_level);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;

    // Drive the server under `block_on` so the OpenTelemetry batch exporters
    // — which `init_telemetry` spawns onto this runtime — are constructed and
    // flushed with a live runtime in scope. The `#[tokio::main]` siblings get
    // this for free; envd builds the runtime by hand, so the guard must live
    // and drop *inside* `block_on`. The guard MUST stay bound to a named
    // variable: dropping it early — or with `let _ = ...` — exports nothing.
    runtime.block_on(async move {
        // The re-arm handle hot-swaps telemetry across a gVisor pause/resume
        // (#356). It is now driven by the node over the layer-free `:49984`
        // listener: `POST /__quiesce` before `runsc checkpoint`, `POST /__rearm`
        // after restore (issue #401). `run` mounts those routes; `_telemetry`
        // stays bound so the guard flushes on drop.
        let (_telemetry, rearm) =
            sandbawks_observability::init_telemetry("envd", env!("CARGO_PKG_VERSION"))
                .context("initialising OpenTelemetry SDK")?;

        let result = run(cli, rearm).await;

        if let Err(err) = &result {
            // `tracing::error!` per the repo's CLAUDE.md rule on console
            // diagnostics; `println!` would race with the subscriber's
            // own stderr writer.
            tracing::error!(error = %err, "envd shutting down with error");
        }
        result
    })
}

/// The Linux kernel's symlink to the running executable's backing file.
/// Reading through it yields the exact bytes of the current binary
/// regardless of how (or under what `argv[0]`) it was launched, and with
/// no dependency on a shell or `/usr/bin/cp`.
const SELF_EXE: &str = "/proc/self/exe";

/// Dispatch a one-shot [`Command`] to completion.
fn run_command(command: Command) -> Result<()> {
    match command {
        Command::SelfInstall { dest } => self_install(&dest),
    }
}

/// Install the running `envd` binary to `dest` (issue #268).
///
/// Thin wrapper over [`install_from`] that fixes the source to
/// [`SELF_EXE`]; the split keeps the copy logic unit-testable without a
/// real `/proc/self/exe`.
fn self_install(dest: &Path) -> Result<()> {
    install_from(Path::new(SELF_EXE), dest)
}

/// Copy `src` to `dest` byte-for-byte, creating `dest`'s parent
/// directory if it does not exist.
///
/// The bytes are written to a temporary sibling of `dest` and then
/// atomically `rename`d into place, so a concurrent reader — a sandbox
/// Pod racing the DaemonSet's stage step (#272) — never execs a
/// half-written binary. `rename(2)` is atomic only within a single
/// filesystem, which is why the temp file is placed alongside `dest`
/// rather than in `/tmp`. [`std::fs::copy`] preserves the source's
/// permission bits, so the installed binary stays executable (the
/// `/proc/self/exe` source is mode `0755`).
fn install_from(src: &Path, dest: &Path) -> Result<()> {
    if let Some(parent) = dest.parent() {
        // `Path::parent` is `Some("")` for a bare filename; skip the
        // `create_dir_all("")` (which would error) in that case.
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating destination directory {}", parent.display()))?;
        }
    }

    // Hidden, PID-tagged temp name in `dest`'s directory. Hidden so a
    // directory listing doesn't surface a transient file. The atomic
    // `rename` below — not this name — is what guarantees a reader never
    // sees a half-written binary; concurrent *writers* are ruled out by
    // the DaemonSet scheduling invariant (one `stage-envd` Pod per node,
    // and inside the scratch carrier envd is PID 1). The PID tag is just
    // a cheap uniqueness hedge so a re-run after an aborted install
    // doesn't collide with its own stale tempfile.
    let file_name = dest
        .file_name()
        .with_context(|| format!("destination {} has no file name", dest.display()))?;
    let mut tmp_name = std::ffi::OsString::from(".");
    tmp_name.push(file_name);
    tmp_name.push(format!(".tmp.{}", std::process::id()));
    let tmp = dest.with_file_name(tmp_name);

    std::fs::copy(src, &tmp)
        .with_context(|| format!("copying {} to {}", src.display(), tmp.display()))?;

    if let Err(e) = std::fs::rename(&tmp, dest) {
        // Best-effort cleanup so a failed install doesn't strand the
        // temp file; the rename error is the one that matters.
        let _ = std::fs::remove_file(&tmp);
        return Err(anyhow::Error::new(e).context(format!(
            "renaming {} to {}",
            tmp.display(),
            dest.display()
        )));
    }

    tracing::info!(
        src = %src.display(),
        dest = %dest.display(),
        "installed envd binary"
    );
    Ok(())
}

/// Environment variable the controller sets to the absolute path envd should
/// relocate onto and serve from — a `medium: Memory` (tmpfs) volume whose
/// contents the gVisor memory checkpoint captures (issue #466). Unset on the
/// cold / legacy path, where envd serves in place from wherever it was exec'd.
const RELOCATE_TARGET_ENV: &str = "SANDBAWKS_ENVD_RELOCATE_TARGET";

/// Marker envd sets in the re-exec'd child's environment after relocating, so
/// the child short-circuits *structurally* rather than by re-deriving path
/// equality. `execve` inherits the environment, so without this the only thing
/// stopping an infinite re-exec storm is `current_exe()` canonicalizing to
/// exactly the configured target — a path-geometry assumption (`/proc/self/exe`
/// resolution, bind mounts, mount namespaces) that fails closed into a loop
/// with no diagnostic. The marker makes "already relocated" a fact the parent
/// states, not one the child has to rediscover (issue #471 review).
const RELOCATED_MARKER_ENV: &str = "SANDBAWKS_ENVD_RELOCATED";

/// Whether envd should serve from where it is or relocate first.
#[derive(Debug, PartialEq, Eq)]
enum RelocateAction {
    /// Serve from the current binary — already relocated (marker set),
    /// [`RELOCATE_TARGET_ENV`] is unset, or this process is already running
    /// from the target.
    ServeInPlace,
    /// Copy self to this path and re-exec from it before serving.
    Relocate(PathBuf),
}

/// Parse [`RELOCATE_TARGET_ENV`] into an optional target path, treating an
/// empty value the same as unset. Split from the environment read so the
/// unset / empty / path cases are unit-testable without mutating process env
/// (which is process-global and would race parallel tests).
fn parse_relocate_target(raw: Option<OsString>) -> Option<PathBuf> {
    raw.filter(|v| !v.is_empty()).map(PathBuf::from)
}

/// Pure relocation decision, kept side-effect-free for unit testing.
///
/// `already_relocated` is whether [`RELOCATED_MARKER_ENV`] is set — the
/// structural loop backstop: once set, serve unconditionally, so a re-exec can
/// never recur regardless of how the two paths resolve. `target` is the parsed
/// [`RELOCATE_TARGET_ENV`] value and `current_exe` is the path this process is
/// running from (both canonicalized by the caller). The `target == current_exe`
/// arm is now only an optimization — it skips a redundant copy + re-exec when
/// envd is cold-booted *directly* from the target — not the loop guard.
fn relocate_decision(
    already_relocated: bool,
    target: Option<PathBuf>,
    current_exe: &Path,
) -> RelocateAction {
    if already_relocated {
        return RelocateAction::ServeInPlace;
    }
    match target {
        None => RelocateAction::ServeInPlace,
        Some(t) if t.as_path() == current_exe => RelocateAction::ServeInPlace,
        Some(t) => RelocateAction::Relocate(t),
    }
}

/// Best-effort canonicalization: resolve symlinks / `..` when the path exists,
/// otherwise return it unchanged — the target legitimately does not exist on
/// the first boot, before the copy. Used to compare `current_exe()` against the
/// target on the happy path; correctness of the loop *backstop* does not depend
/// on it (that is [`RELOCATED_MARKER_ENV`]), so an asymmetric resolution here
/// costs at most one redundant relocate, never an `execve` loop.
fn canonicalize_or(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

/// Relocate envd onto the checkpoint-captured tmpfs and re-exec from there, so
/// the serving process's code pages are captured by `runsc checkpoint` and
/// survive a memory-only restore regardless of the node-staged host binary
/// (issues #467 / #466).
///
/// Returns `Ok(())` to continue serving in place — already relocated
/// ([`RELOCATED_MARKER_ENV`] set), [`RELOCATE_TARGET_ENV`] unset (cold / legacy
/// path), or this process already running from the target. On the relocate path
/// it copies the running binary to the target (reusing [`install_from`]'s
/// atomic, byte-identical copy) and replaces this process image via `execve`,
/// forwarding `argv[1..]` so every server flag carries through and preserving
/// `argv[0]` so the program name is unchanged post-relocate; that call returns
/// only on failure.
///
/// # Errors
///
/// Fails closed: a copy or `execve` failure returns `Err` — so `main` exits
/// non-zero and the Pod surfaces it — rather than silently serving from the
/// host mount, which would leave the sandbox's snapshots un-restorable (the
/// exact failure this design removes).
fn relocate_onto_capture_surface() -> Result<()> {
    let already_relocated = std::env::var_os(RELOCATED_MARKER_ENV).is_some();
    let target = parse_relocate_target(std::env::var_os(RELOCATE_TARGET_ENV));
    let current = std::env::current_exe().context("resolving current executable path")?;
    let canon_target = target.map(|t| canonicalize_or(&t));
    let canon_current = canonicalize_or(&current);

    match relocate_decision(already_relocated, canon_target, &canon_current) {
        RelocateAction::ServeInPlace => Ok(()),
        RelocateAction::Relocate(target) => {
            install_from(Path::new(SELF_EXE), &target).with_context(|| {
                format!("relocating envd onto capture surface {}", target.display())
            })?;
            // Tracing is not up yet on this path, so `install_from`'s
            // `tracing::info!` goes nowhere — emit a plain stderr line so a
            // post-mortem of an un-restorable snapshot can confirm the relocate
            // happened (issue #471 review).
            eprintln!(
                "envd: relocated onto capture surface {} for capture-in-checkpoint; re-execing",
                target.display()
            );
            // `CommandExt::exec` replaces this process image (`execve`); it
            // inherits the current environment and returns only on failure.
            // `RELOCATED_MARKER_ENV` makes the child short-circuit structurally
            // (no `execve` loop if path resolution is ever asymmetric);
            // `arg0` preserves the original `argv[0]` so the program name does
            // not flip to the tmpfs path in any post-relocate diagnostic.
            let argv0 = std::env::args_os()
                .next()
                .unwrap_or_else(|| target.clone().into_os_string());
            let err = std::process::Command::new(&target)
                .arg0(argv0)
                .args(std::env::args_os().skip(1))
                .env(RELOCATED_MARKER_ENV, "1")
                .exec();
            Err(anyhow::Error::new(err)
                .context(format!("re-execing relocated envd at {}", target.display())))
        }
    }
}

/// Async entrypoint. Split out from `main` so the runtime construction
/// stays synchronous and the `?`-propagated errors can flow up into a
/// single `tracing::error!` site.
async fn run(cli: Cli, rearm: RearmHandle) -> Result<()> {
    let Cli {
        // `command` is `None` on the server path (checked in `main`
        // before the runtime is built); ignore it here.
        command: _,
        listen,
        http_listen,
        state_file,
        root,
        log_level: _,
    } = cli;

    tracing::info!(
        %listen,
        %http_listen,
        state_file = %state_file.display(),
        root = %root.display(),
        schema_version = SCHEMA_VERSION,
        "starting envd"
    );

    let (seed_processes, seed_ptys, seed_watches) = load_boot_seed(&state_file, &root).await?;

    let filesystem = FilesystemService::with_seed(root.clone(), seed_watches)
        .with_context(|| format!("initialising filesystem service at {}", root.display()))?;
    let process = ProcessService::with_seed(seed_processes, seed_ptys);

    // Cheap clones so the HTTP `/__state` route and the shutdown
    // snapshot flush can read the same live registries the gRPC server
    // is mutating. The `filesystem` / `process` originals are then
    // moved into the tonic builder below.
    let snapshot = SnapshotHandle {
        process: process.clone(),
        filesystem: filesystem.clone(),
        root: root.clone(),
        state_file: state_file.clone(),
    };

    // Bind the HTTP `/__state` listener *before* the signal handler
    // wires up so a bind failure (port in use, EADDRINUSE on a
    // double-launch) surfaces synchronously with the rest of the boot
    // checks rather than after the gRPC server is already accepting.
    let http_listener = tokio::net::TcpListener::bind(http_listen)
        .await
        .with_context(|| format!("binding HTTP /__state listener on {http_listen}"))?;

    // Bind the gRPC listener up front too, for the same fail-fast reason and to
    // close a bind-after-release race: the supervisor (`run_grpc_loop`) adopts
    // this already-bound socket on its first iteration rather than re-binding
    // `listen` by address, so there is never a window where the port is unowned
    // between picking it and binding it. A `std` listener is used because it is
    // handed across the thread boundary and adopted inside the supervisor's own
    // dedicated runtime via `TcpListener::from_std`.
    let grpc_listener = std::net::TcpListener::bind(listen)
        .with_context(|| format!("binding gRPC listener on {listen}"))?;

    // Fan the single shutdown signal out to both servers via a
    // broadcast channel. `oneshot` won't work because we need to
    // observe the signal from two independent futures (tonic's
    // `serve_with_shutdown` and axum's `with_graceful_shutdown`).
    let (signal_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let http_signal_rx = signal_tx.subscribe();
    // Drain-cap gates: independent subscribers awaited *before* the
    // post-signal `DRAIN_TIMEOUT` caps below, so each cap bounds only the
    // drain phase and not the serving lifetime. See `join_with_drain_cap`.
    // (The gRPC server observes the final-shutdown signal inside its own
    // supervisor; `grpc_drain_gate` only paces the post-signal join cap here.)
    let grpc_drain_gate = signal_tx.subscribe();
    let http_drain_gate = signal_tx.subscribe();
    let signal_tx_for_handler = signal_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = wait_for_signal().await {
            tracing::error!(error = %e, "signal handler error; forcing shutdown");
        }
        // `send` only errors if there are no live receivers — which
        // happens when both servers have already exited. Either way,
        // nothing useful to do.
        let _ = signal_tx_for_handler.send(());
    });

    // The PID-1 orphan reaper (issue #459) is process-global and starts
    // lazily when `ProcessService` is constructed above, so there is nothing
    // to wire up here — see `process::global_reaper`.

    // #560 drain coordination: `POST /__quiesce` bumps `drain_req`; the gRPC
    // serve supervisor force-closes connections + rebinds a fresh listener,
    // then publishes the epoch on `drain_done`. Created here so the handle
    // reaches the HTTP router while the matching receiver/sender go to the
    // supervisor thread.
    let (drain_req_tx, drain_req_rx) = tokio::sync::watch::channel(0u64);
    let (drain_done_tx, drain_done_rx) = tokio::sync::watch::channel(0u64);
    let drain_handle = DrainHandle {
        request: Arc::new(drain_req_tx),
        done: drain_done_rx,
    };

    // Serve gRPC on a dedicated Tokio runtime in its own OS thread so the
    // pre-checkpoint drain can force-close every connection by tearing that
    // runtime down (#560) — tonic spawns per-connection tasks detached, so a
    // graceful shutdown can't close a held streaming RPC, and a second runtime
    // can only be driven from another thread (nesting `block_on` panics). The
    // supervisor's final serve result returns over a oneshot.
    let (grpc_result_tx, grpc_result_rx) = tokio::sync::oneshot::channel();
    {
        let signal_tx = signal_tx.clone();
        std::thread::Builder::new()
            .name("envd-grpc-sup".to_owned())
            .spawn(move || {
                let result = run_grpc_loop(
                    grpc_listener,
                    filesystem,
                    process,
                    drain_req_rx,
                    drain_done_tx,
                    signal_tx,
                );
                let _ = grpc_result_tx.send(result);
            })
            .context("spawning gRPC serve supervisor thread")?;
    }

    let http_shutdown = async move {
        let mut rx = http_signal_rx;
        let _ = rx.recv().await;
        tracing::info!("shutdown signal received; axum stops accepting new HTTP requests");
    };

    let http_app: Router = http_router(snapshot.clone(), rearm, drain_handle);
    // axum 0.7's `WithGracefulShutdown` impls `IntoFuture`, not
    // `Future` directly — wrap the await in an async block so the
    // spawned task has a concrete `Future` type.
    let http_task = tokio::spawn(async move {
        axum::serve(http_listener, http_app)
            .with_graceful_shutdown(http_shutdown)
            .await
    });

    // Await the gRPC supervisor's final serve result, capping the wait at
    // `DRAIN_TIMEOUT` *after* the shutdown signal fires. The gate is awaited
    // first so the cap covers only the post-signal drain, never the serving
    // lifetime (`tokio::time::timeout` starts its clock the instant it is
    // polled). The supervisor observes the same broadcast signal and returns
    // once its dedicated gRPC runtime has wound down.
    {
        let mut gate = grpc_drain_gate;
        let _ = gate.recv().await;
    }
    match tokio::time::timeout(DRAIN_TIMEOUT, grpc_result_rx).await {
        Ok(Ok(Ok(()))) => tracing::info!("gRPC server drained cleanly"),
        Ok(Ok(Err(e))) => {
            tracing::error!(error = %e, "gRPC server returned error");
            // Flush the snapshot anyway — losing it is worse than losing the
            // serve error, which is already logged. The HTTP task is left to
            // drop here (the broadcast hangs up on `signal_tx`'s last drop).
            let flush = write_snapshot(&snapshot, &state_file).await;
            return Err(e.context(
                flush
                    .err()
                    .map(|e| format!("(snapshot flush also failed: {e})"))
                    .unwrap_or_else(|| "snapshot flushed".into()),
            ));
        }
        Ok(Err(_recv)) => {
            tracing::error!("gRPC serve supervisor exited without reporting a result");
        }
        Err(_) => {
            tracing::warn!(
                "gRPC serve supervisor did not exit within {:?} of shutdown",
                DRAIN_TIMEOUT
            );
        }
    }

    // Drain the HTTP listener too. axum's graceful shutdown fires on
    // the broadcast signal, so by the time we're here the listener has
    // already stopped accepting; the join is just to surface any panic
    // and avoid a `JoinHandle` warning. `http_drain_gate` has already
    // fired (same signal that released the gRPC gate above), so this
    // resolves promptly; the cap only guards against a stuck in-flight
    // HTTP request pinning shutdown indefinitely.
    match join_with_drain_cap(http_task, http_drain_gate, DRAIN_TIMEOUT).await {
        Ok(Ok(Ok(()))) => tracing::info!("HTTP /__state listener drained cleanly"),
        Ok(Ok(Err(e))) => tracing::error!(error = %e, "HTTP /__state listener returned error"),
        Ok(Err(e)) => tracing::error!(error = %e, "HTTP /__state task panicked"),
        Err(_) => tracing::warn!(
            "HTTP /__state drain timed out after {:?}; in-flight HTTP requests cancelled",
            DRAIN_TIMEOUT
        ),
    }
    // `signal_tx` is dropped here, which closes the broadcast channel
    // for any subscribers that didn't fire — keeping it in scope
    // until now stops a premature drop from poisoning the receivers.
    drop(signal_tx);

    write_snapshot(&snapshot, &state_file).await?;
    tracing::info!("envd exit");
    Ok(())
}

/// Serve the gRPC API (`filesystem` + `process`) on a **dedicated** Tokio
/// runtime, restarting it on demand so a pre-checkpoint drain can force-close
/// every live connection (issue #560).
///
/// # Why a dedicated runtime
///
/// tonic spawns one detached task per accepted connection. Dropping or aborting
/// the serve future closes the *listener* but leaves those connection tasks —
/// and their sockets — alive; a graceful shutdown, meanwhile, waits for
/// in-flight RPCs, so a held streaming RPC (`process.Start`) would block it
/// indefinitely. The only reliable force-close is to tear the whole runtime
/// down. Running gRPC on its own runtime makes that possible without killing
/// the rest of envd (a second runtime must live on its own OS thread, since
/// nesting `Runtime::block_on` panics).
///
/// # Loop
///
/// The caller binds the listener (`listener`) up front and hands it in, so the
/// first iteration adopts an already-bound socket — there is no window where the
/// port is unowned between choosing it and binding it (issue #567). Each later
/// iteration re-binds the *same* address: the previous runtime's listener was
/// closed by [`Runtime::shutdown_timeout`], and a listening socket frees its
/// port immediately on close with no `TIME_WAIT`, so the rebind always succeeds.
///
/// Each iteration builds a fresh runtime + `:49983` listener and serves until
/// one of:
/// - **final shutdown** (`signal_tx` fires) → tear down and return `Ok(())`;
/// - **serve exit** (almost always a startup bind failure) → return the error;
/// - **drain** (`/__quiesce` bumped `drain_req`) → tear the runtime down
///   ([`Runtime::shutdown_timeout`] aborts the serve task *and* every
///   connection task, closing all sockets), publish the epoch on `drain_done`,
///   and loop to rebind a fresh, connection-free listener.
///
/// So a `runsc checkpoint` taken right after `/__quiesce` captures a `:49983`
/// listener with zero established connections — the state that resumes cleanly
/// (the gateway reconnects after restore).
fn run_grpc_loop(
    listener: std::net::TcpListener,
    filesystem: FilesystemService,
    process: ProcessService,
    mut drain_req_rx: tokio::sync::watch::Receiver<u64>,
    drain_done_tx: tokio::sync::watch::Sender<u64>,
    signal_tx: tokio::sync::broadcast::Sender<()>,
) -> Result<()> {
    // The address every rebind re-binds to, read once from the pre-bound socket.
    let listen = listener
        .local_addr()
        .context("reading gRPC listener local address")?;
    // Adopted by the first iteration via `from_std`; `None` thereafter, so drain
    // rebinds fall through to binding `listen` afresh.
    let mut prebound = Some(listener);
    // One final-shutdown subscriber, reused across rebinds: a signal sent while
    // a drain is rebuilding the runtime is buffered (broadcast capacity 1) and
    // observed on the next loop, so final shutdown is never missed.
    let mut final_rx = signal_tx.subscribe();

    // The drain epoch *this* iteration's fresh listener satisfies. Boot acks 0;
    // a drain captures its requested epoch when observed (below) and only the
    // *next* iteration — which runs strictly after the force-close — acks it. So
    // the ack can never outrun the drain it claims to satisfy. (The old code
    // re-borrowed `drain_req` at bind time, which let the boot bind ack a drain
    // racing in just before that borrow without ever closing a connection —
    // issue #567's actual flake: the test's held connection stayed open.)
    let mut ack_epoch = 0u64;

    /// What ended a serve iteration.
    enum Outcome {
        /// Final shutdown signal — tear down and exit the loop.
        Final,
        /// `/__quiesce` requested a drain (carrying the requested epoch) — tear
        /// the runtime down (force-closing every connection) and rebind a fresh
        /// listener that acks this epoch on the next iteration.
        Drain(u64),
        /// The serve future resolved on its own (bind failure / serve error).
        Exited(Result<()>),
    }

    loop {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(GRPC_WORKER_THREADS)
            .thread_name("envd-grpc")
            .enable_all()
            .build()
            .context("building the dedicated gRPC runtime")?;

        let outcome = runtime.block_on(async {
            // First iteration: adopt the pre-bound listener handed in by the
            // caller (no unbound window — issue #567). Drain iterations: bind a
            // fresh listener on the same address so the drain is acked only AFTER
            // a fresh listener is accepting, and `:49983` is never unbound when
            // the node checkpoints. (Re-binding the same port succeeds: the
            // previous runtime's listener was closed by `shutdown_timeout` before
            // this iteration; a listening socket frees its port immediately on
            // close, with no `TIME_WAIT`.)
            let listener = match prebound.take() {
                // `from_std` must run inside a runtime (it registers the socket
                // with the reactor) and requires the socket be non-blocking.
                Some(std_listener) => {
                    let adopted = std_listener
                        .set_nonblocking(true)
                        .and_then(|()| tokio::net::TcpListener::from_std(std_listener));
                    match adopted {
                        Ok(listener) => listener,
                        Err(e) => {
                            return Outcome::Exited(Err(anyhow::Error::new(e).context(format!(
                                "adopting pre-bound gRPC listener on {listen}"
                            ))));
                        }
                    }
                }
                None => match tokio::net::TcpListener::bind(listen).await {
                    Ok(listener) => listener,
                    Err(e) => {
                        return Outcome::Exited(Err(anyhow::Error::new(e)
                            .context(format!("binding gRPC listener on {listen}"))));
                    }
                },
            };
            // Ack the epoch this (re)bind satisfies: a fresh, connection-free
            // listener is up, so the waiting `/__quiesce` may return 200 and the
            // node may proceed to checkpoint. `ack_epoch` was captured *after*
            // the previous iteration's force-close, so this ack never precedes
            // the drain it claims to satisfy.
            let _ = drain_done_tx.send(ack_epoch);

            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            // Rebuilt each iteration: services are cheap `Arc`-backed clones, and
            // the trace/metrics tower layers (#298/#300) are re-applied so a
            // rebound listener keeps full observability.
            let server = Server::builder()
                .layer(envd::trace::GrpcTraceLayer)
                .layer(envd::grpc_metrics::GrpcMetricsLayer)
                .add_service(FilesystemServer::new(filesystem.clone()))
                .add_service(ProcessServer::new(process.clone()))
                .serve_with_incoming(incoming);
            // Detached on this runtime so tearing the runtime down aborts it.
            let mut serve = tokio::spawn(server);
            tokio::select! {
                joined = &mut serve => Outcome::Exited(match joined {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(anyhow::Error::new(e).context("serving gRPC")),
                    // The serve task was aborted by our own runtime shutdown
                    // below — not an error.
                    Err(_join) => Ok(()),
                }),
                _ = final_rx.recv() => Outcome::Final,
                // Capture (and mark seen) the requested epoch now, so the next
                // iteration acks exactly what was requested and `changed()` does
                // not re-fire on this same value.
                _ = drain_req_rx.changed() => Outcome::Drain(*drain_req_rx.borrow_and_update()),
            }
        });

        // Force-close: aborts the (detached) serve task and every per-connection
        // task tonic spawned, closing the listener and all sockets. Bounded so a
        // stuck task can't pin shutdown; the drain *wants* connections gone.
        runtime.shutdown_timeout(GRPC_RUNTIME_SHUTDOWN_GRACE);

        match outcome {
            Outcome::Final => {
                tracing::info!("shutdown signal received; gRPC server stopped");
                return Ok(());
            }
            Outcome::Exited(result) => return result,
            Outcome::Drain(epoch) => {
                // Carry the requested epoch to the next iteration's ack, which
                // runs after the force-close above — never before the fresh
                // listener is up.
                ack_epoch = epoch;
                tracing::info!(
                    epoch,
                    "drained gRPC connections before checkpoint; rebinding a fresh listener (#560)"
                );
            }
        }
    }
}

/// `GET /__state` — return the live [`EnvdState`] as JSON.
///
/// The body is the live capture of the underlying services'
/// registries, version-stamped with [`SCHEMA_VERSION`]. The same shape
/// is what envd writes to `--state-file` on shutdown, so a paused
/// snapshot bundle's `envd-state.json` is byte-comparable (modulo
/// JSON whitespace) with the file the next-boot envd reads back.
///
/// The node injects a W3C `traceparent` into this probe (issue #298); the
/// handler adopts it as its span's parent so the `/__state` work continues the
/// node's pause trace rather than starting a disconnected one. `skip_all` keeps
/// the `HeaderMap` and snapshot handle out of span attributes.
#[tracing::instrument(skip_all, name = "handler:envd::state")]
async fn state_handler(
    headers: axum::http::HeaderMap,
    State(snapshot): State<SnapshotHandle>,
) -> Json<EnvdState> {
    sandbawks_observability::continue_current(&sandbawks_observability::HttpHeaderExtractor(
        &headers,
    ));
    Json(snapshot.capture().await)
}

/// `GET /__version` — return envd's compile-time build identifier ([`BUILD_ID`])
/// as a `text/plain` body.
///
/// Load-bearing for the capture-in-checkpoint acceptance proof (issues #466 /
/// #470). The id is baked into the running binary's pages at compile time, so
/// the value this handler returns is whatever code is actually executing — not
/// anything read from disk or the environment at request time. Observing it
/// after `runsc restore` is therefore a direct test of provenance: the
/// *captured* id means the restored process is serving from `pages.img`, while
/// the *host* id would mean it re-faulted a host-side binary that was swapped
/// between checkpoint and restore (the exact failure the epic exists to rule
/// out).
///
/// Mounted on the layer-free `:49984` listener alongside `/__state`, so it keeps
/// answering even while the gRPC surface is mid-trouble (e.g. a quiesced
/// exporter). The handler takes no state — the value is a `'static` constant —
/// and so has no failure path.
#[tracing::instrument(name = "handler:envd::version")]
async fn version_handler() -> &'static str {
    BUILD_ID
}

/// `POST /__quiesce` — stop emitting telemetry and tear the stale OTLP
/// providers down without blocking on a dead collector (issue #401).
///
/// The node drives this just before `runsc checkpoint` so the frozen
/// sandbox carries no half-flushing exporter into the restore. Mounted on
/// the layer-free `:49984` listener so the destructive transition never
/// rides the gRPC trace/metrics layers it is tearing down, and kept a
/// `POST` so it is never coupled to `GET /__state`'s capture-and-retry.
///
/// [`RearmHandle::quiesce`] is infallible and panic-free (a hung exporter is
/// *abandoned* after [`QUIESCE_SHUTDOWN_CAP`], never joined), so this route
/// has no 500 path: it always returns `200 OK`.
#[tracing::instrument(skip_all, name = "handler:envd::quiesce")]
async fn quiesce_handler(
    State(rearm): State<RearmHandle>,
    State(drain): State<DrainHandle>,
) -> StatusCode {
    rearm.quiesce(QUIESCE_SHUTDOWN_CAP);
    // #560: drop every live gRPC client connection before the node checkpoints.
    // A connection captured ESTABLISHED in the checkpoint comes back from
    // `runsc restore` as a dead half-open socket whose external peer is gone,
    // and that wedges envd's serving runtime (it accepts new connections but
    // answers nothing). Draining here makes the frozen sandbox carry a fresh,
    // connection-free `:49983` listener; the gateway reconnects after resume.
    drain.drain(QUIESCE_DRAIN_CAP).await;
    StatusCode::OK
}

/// `POST /__rearm` — rebuild the OTLP provider stack from the current
/// environment and swap the live telemetry layers back onto it (issue #401).
///
/// The node drives this after `runsc restore` so the resumed process honours
/// whatever collector endpoint/config its restored environment now carries.
/// Returns `200 OK` on success; on failure (unsupported protocol, exporter
/// build error, poisoned reload lock) the handle is left safely quiesced and
/// the route returns `500` so the caller can retry a clean rearm.
///
/// On success this also re-arms the background gRPC metrics recorder (issue
/// #402): `rearm.rearm()` calls `set_meter_provider(new)` internally, so the
/// recorder's cached instruments must be rebuilt off the new provider *after*
/// that swap, or post-resume RPC metrics keep flowing into the dead one.
#[tracing::instrument(skip_all, name = "handler:envd::rearm")]
async fn rearm_handler(State(rearm): State<RearmHandle>) -> StatusCode {
    match rearm.rearm() {
        Ok(()) => {
            // Ordering matters: this runs only after `rearm.rearm()` has already
            // committed `set_meter_provider(new)`, so the recorder's rebuild
            // reads the fresh provider (issue #402).
            envd::grpc_metrics::rearm();
            StatusCode::OK
        }
        Err(error) => {
            tracing::error!(error = %error, "re-arm telemetry failed; handle left quiesced");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Type alias for the three seed arrays a service set is constructed
/// with. Pulled out so the [`load_boot_seed`] return type fits on one
/// line and so the binary's import surface doesn't grow.
type BootSeed = (
    Vec<envd::state::ProcessSnapshot>,
    Vec<envd::state::PtySnapshot>,
    Vec<envd::state::WatchSnapshot>,
);

/// Read the snapshot file from disk and surface its three arrays as
/// seed input for the live services.
///
/// Falls back to three empty vectors in two cases:
///
/// 1. The file does not exist — first boot of a fresh sandbox Pod.
/// 2. The file's parent directory does not exist — same scenario,
///    just one level higher (e.g. `/var/tmp/envd/` not yet created).
///
/// Any other I/O or parse error is propagated so a corrupt snapshot
/// fails the boot loudly rather than silently dropping pre-pause
/// state on the floor.
///
/// When the snapshot carries a rootfs fingerprint (#236), it is
/// re-verified against `root` — the restored filesystem — *before* the
/// seed is handed back. This is the envd-side resume divergence oracle:
/// a mismatch means the restored rootfs is not the one captured at
/// pause, so the boot fails closed rather than running a workload on a
/// subtly-wrong filesystem. A snapshot without a fingerprint (written by
/// an envd predating #236) skips the check.
async fn load_boot_seed(path: &Path, root: &Path) -> Result<BootSeed> {
    if !path.exists() {
        tracing::info!(path = %path.display(), "no state file found; starting fresh");
        return Ok((Vec::new(), Vec::new(), Vec::new()));
    }
    let state = EnvdState::read_from(path)
        .with_context(|| format!("reading snapshot from {}", path.display()))?;
    // Exclude the state file itself from the re-verification walk — it
    // must match the exclusion the capture side applied (it lives inside
    // `--root` in the default deployment).
    verify_rootfs_fingerprint(&state, root, path).await?;
    let restored = state
        .restore()
        .with_context(|| format!("restoring snapshot from {}", path.display()))?;
    let processes = restored.processes.into_inner();
    let ptys = restored.ptys.into_inner();
    let watches = restored.watches.into_inner();
    tracing::info!(
        processes = processes.len(),
        ptys = ptys.len(),
        watches = watches.len(),
        "loaded pause/resume seed into live services"
    );
    Ok((processes, ptys, watches))
}

/// Re-verify the snapshot's rootfs fingerprint against the restored
/// filesystem at `root` (#236).
///
/// No-op when the snapshot carries no fingerprint. The recompute runs on
/// the blocking pool (it streams every file under `root` through
/// SHA-256). A [`FingerprintError::Mismatch`] is the fail-closed signal
/// and is surfaced as an error so the boot aborts.
///
/// [`FingerprintError::Mismatch`]: envd::fingerprint::FingerprintError::Mismatch
async fn verify_rootfs_fingerprint(
    state: &EnvdState,
    root: &Path,
    exclude_state_file: &Path,
) -> Result<()> {
    let Some(expected) = state.rootfs_fingerprint.clone() else {
        tracing::info!("snapshot carries no rootfs fingerprint; skipping resume re-verification");
        return Ok(());
    };
    let root = root.to_path_buf();
    let exclude = exclude_state_file.to_path_buf();
    tokio::task::spawn_blocking(move || expected.verify(&root, &[exclude.as_path()]))
        .await
        .map_err(|e| anyhow::anyhow!("rootfs fingerprint verify task join error: {e}"))?
        .context("rootfs fingerprint re-verification failed; restored filesystem diverged")?;
    tracing::info!("rootfs fingerprint re-verified; restored filesystem matches the pause capture");
    Ok(())
}

/// Flush the live services' registries to `path`. Creates the parent
/// directory if it does not exist so a fresh Pod with no
/// `/var/tmp/envd` directory still writes a usable snapshot.
async fn write_snapshot(snapshot: &SnapshotHandle, path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!("creating snapshot parent directory {}", parent.display())
            })?;
        }
    }
    let state = snapshot.capture().await;
    // `write_to` runs the atomic temp-file + rename dance; spawn it
    // on the blocking pool so the std::fs calls inside don't stall
    // the runtime in the (admittedly unlikely) case of a slow
    // filesystem on shutdown.
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || state.write_to(&path))
        .await
        .map_err(|e| anyhow::anyhow!("snapshot writer task join error: {e}"))?
        .map_err(|e| anyhow::anyhow!("writing snapshot: {e}"))?;
    tracing::info!("snapshot flushed");
    Ok(())
}

/// Wait for the first of SIGTERM or SIGINT. Returns immediately on
/// either signal; the caller is responsible for fanning that into a
/// tonic shutdown future.
///
/// SIGTERM is what Kubernetes sends when terminating a Pod; SIGINT is
/// what a developer running `cargo run -- ...` sees on Ctrl-C. We
/// honour both equally — there's no operational reason to drain
/// differently between the two.
async fn wait_for_signal() -> Result<()> {
    let mut sigterm = signal(SignalKind::terminate()).context("installing SIGTERM handler")?;
    let mut sigint = signal(SignalKind::interrupt()).context("installing SIGINT handler")?;
    tokio::select! {
        _ = sigterm.recv() => tracing::info!("received SIGTERM"),
        _ = sigint.recv() => tracing::info!("received SIGINT"),
    }
    Ok(())
}

/// Wait for a shutdown signal on `gate`, then join `task` under a `cap`
/// drain timeout.
///
/// The gate ordering is the whole point. [`tokio::time::timeout`] starts
/// counting the moment it is first polled, so applying it directly to a
/// task that serves until a shutdown signal would cap the *serving
/// lifetime* rather than the drain: envd would self-terminate `cap` after
/// boot (and the HTTP listener a further `cap` later) with no signal ever
/// sent. Awaiting `gate` — a [`broadcast::Receiver`] subscribed to the
/// same shutdown channel as the servers' own graceful-shutdown futures —
/// blocks until shutdown is actually requested, so `cap` bounds only the
/// post-signal drain.
///
/// A `recv()` error (lagged or channel closed) is treated as "shutdown
/// requested": the only sender drops at end of `run`, and a closed gate
/// can only mean the process is already tearing down, so proceeding to
/// the capped join is correct.
///
/// # Arguments
///
/// * `task` - the spawned serve task to join once draining begins
/// * `gate` - a receiver on the shutdown broadcast channel
/// * `cap` - upper bound on the post-signal drain
///
/// # Returns
///
/// The [`tokio::time::timeout`] result of joining `task`: `Ok` holds the
/// task's [`JoinHandle`] output, `Err(Elapsed)` means the drain ran past
/// `cap` and the caller should proceed to snapshot-and-exit regardless.
async fn join_with_drain_cap<T>(
    task: tokio::task::JoinHandle<T>,
    mut gate: tokio::sync::broadcast::Receiver<()>,
    cap: Duration,
) -> Result<Result<T, tokio::task::JoinError>, tokio::time::error::Elapsed> {
    let _ = gate.recv().await;
    tokio::time::timeout(cap, task).await
}

#[cfg(test)]
mod tests {
    use super::{
        canonicalize_or, http_router, install_from, join_with_drain_cap, parse_relocate_target,
        relocate_decision, run_grpc_loop, DrainHandle, RelocateAction, SnapshotHandle,
        DEFAULT_STATE_FILE,
    };
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use envd::filesystem::FilesystemService;
    use envd::process::ProcessService;
    use sandbawks_observability::RearmHandle;
    use std::ffi::OsString;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::time::Duration;
    use tower::ServiceExt;

    /// Build the production HTTP router wired to a no-op (disabled-SDK)
    /// [`RearmHandle`], so the route table under test is byte-for-byte the
    /// one [`super::run`] mounts. A disabled handle keeps the test free of
    /// any global-subscriber mutation: `quiesce` is a no-op and `rearm`
    /// returns `Ok(())`, which is exactly what the wiring assertions need.
    /// The exporter-state *flip* itself is proven against an in-memory
    /// exporter in `sandbawks-observability`'s `rearm` tests (#400); this
    /// suite owns the HTTP surface (#401).
    fn router_with_disabled_rearm(root: &std::path::Path) -> axum::Router {
        let filesystem = FilesystemService::with_seed(root.to_path_buf(), Vec::new())
            .expect("filesystem service init");
        let process = ProcessService::with_seed(Vec::new(), Vec::new());
        let snapshot = SnapshotHandle {
            process,
            filesystem,
            root: root.to_path_buf(),
            state_file: root.join("state.json"),
        };
        http_router(snapshot, RearmHandle::disabled(), DrainHandle::disabled())
    }

    /// `POST /__quiesce` is wired and answers `200 OK`. `quiesce` is
    /// infallible, so the route has no error path — a 200 is the contract
    /// the node's pre-checkpoint step depends on (#401).
    #[tokio::test]
    async fn quiesce_route_returns_200() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let app = router_with_disabled_rearm(dir.path());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/__quiesce")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("router responds");

        assert_eq!(response.status(), StatusCode::OK);
    }

    /// `POST /__rearm` is wired and answers `200 OK` on success. A disabled
    /// handle's `rearm` returns `Ok(())`, exercising the success arm of the
    /// route's status mapping (#401).
    #[tokio::test]
    async fn rearm_route_returns_200_on_success() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let app = router_with_disabled_rearm(dir.path());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/__rearm")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("router responds");

        assert_eq!(response.status(), StatusCode::OK);
    }

    /// `GET /__version` is wired and answers `200 OK` with the compile-time
    /// build id as its `text/plain` body. With no `SANDBAWKS_ENVD_BUILD_ID`
    /// stamped into this test build, `BUILD_ID` falls back to the crate
    /// version, so the body must equal `CARGO_PKG_VERSION` — proving the
    /// fallback arm and that the route returns the running binary's own id
    /// (the capture-in-checkpoint provenance seam, #466 / #470).
    #[tokio::test]
    async fn version_route_returns_build_id() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let app = router_with_disabled_rearm(dir.path());

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/__version")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("router responds");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        assert_eq!(body.as_ref(), env!("CARGO_PKG_VERSION").as_bytes());
    }

    /// The rearm routes are `POST`-only: a `GET /__quiesce` must be rejected
    /// with `405 Method Not Allowed`, proving the destructive transition is
    /// never reachable by the side-effect-free verb `GET /__state` uses
    /// (#401). One route stands in for both — they share a mount idiom.
    #[tokio::test]
    async fn quiesce_route_rejects_get() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let app = router_with_disabled_rearm(dir.path());

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/__quiesce")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("router responds");

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    /// Regression for the 60 s self-exit bug: until a shutdown signal is
    /// broadcast, [`join_with_drain_cap`] must stay blocked on its gate and
    /// must NOT let the `cap` fire. If the cap leaked into the serving
    /// phase the combinator would resolve early — the exact failure that
    /// made every sandbox `envd` exit `2 * DRAIN_TIMEOUT` after boot with no
    /// signal. We prove the gate holds by racing a far-longer outer timeout
    /// against a tiny `cap`: gated correctly, only the outer timeout can
    /// fire. `start_paused` auto-advances virtual time, so this is instant
    /// and deterministic.
    #[tokio::test(start_paused = true)]
    async fn drain_cap_does_not_start_until_shutdown_fires() {
        let (tx, gate) = tokio::sync::broadcast::channel::<()>(1);
        // Keep the sender alive (no `send`) so the gate blocks on "no
        // signal" rather than returning `Closed`.
        let _sender = tx;
        // A task that never completes on its own — stands in for a server
        // that serves until told to stop.
        let task = tokio::spawn(std::future::pending::<()>());

        let leaked = tokio::time::timeout(
            Duration::from_secs(3600),
            join_with_drain_cap(task, gate, Duration::from_millis(10)),
        )
        .await;

        assert!(
            leaked.is_err(),
            "drain cap must not start until a shutdown signal fires"
        );
    }

    /// Once a shutdown signal is broadcast, the `cap` must bound the drain:
    /// a task that refuses to finish is abandoned after `cap` elapses
    /// (`Err(Elapsed)`), so the caller proceeds to snapshot-and-exit.
    #[tokio::test(start_paused = true)]
    async fn drain_cap_applies_after_shutdown_fires() {
        let (tx, gate) = tokio::sync::broadcast::channel::<()>(1);
        tx.send(()).expect("receiver is live");
        let task = tokio::spawn(std::future::pending::<()>());

        let res = join_with_drain_cap(task, gate, Duration::from_millis(10)).await;

        assert!(
            res.is_err(),
            "after shutdown the drain cap must elapse and abandon the task"
        );
    }

    /// Regression: the warm-pool auto-pause path runs envd as the workload
    /// image's declared `USER`, which is routinely non-root (e.g. the
    /// `compound-sandbox` image ships `USER dev`). On SIGTERM envd flushes
    /// its snapshot to the default `--state-file`; if that path's parent is
    /// owned by root (the old `/var/lib/envd`, mode `0755`), the shutdown
    /// `create_dir_all` fails with `EACCES`, envd exits non-zero, and the
    /// warm `Sandbox` lands in `Failed` — starving the pool. The default
    /// must nest under a world-writable, sticky temp root (`/var/tmp`, mode
    /// `1777`) so *any* UID can create its snapshot dir. `/var/tmp` is a
    /// plain directory on the container's overlay rootfs — captured by the
    /// `runsc checkpoint` rootfs-delta exactly as `/var/lib/envd` was — so
    /// the restore path is unchanged.
    ///
    /// The assertion pins `/var/tmp` specifically rather than any temp root:
    /// `/tmp` is deliberately rejected because some runtimes back it with an
    /// internal tmpfs that the `runsc checkpoint` rootfs-delta would *not*
    /// capture, reintroducing a resume breakage. Accepting `/tmp` here would
    /// let such a regression pass silently.
    #[test]
    fn default_state_file_nests_under_var_tmp() {
        assert!(
            DEFAULT_STATE_FILE.starts_with("/var/tmp/"),
            "default --state-file must nest under /var/tmp (sticky 1777, captured by \
             the runsc rootfs-delta) so a non-root envd can persist its snapshot on \
             auto-pause; got {DEFAULT_STATE_FILE}"
        );
    }

    /// `install_from` copies the source byte-for-byte and leaves the
    /// destination executable — the `self-install` contract (#268), minus
    /// the `/proc/self/exe` source that only resolves in a real process.
    #[test]
    fn install_from_produces_byte_identical_executable_copy() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let src = dir.path().join("src-bin");
        // A non-trivial payload including a NUL byte, so the copy is
        // exercised on binary (not just text) content.
        let payload: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        fs::write(&src, &payload).expect("write source");
        fs::set_permissions(&src, fs::Permissions::from_mode(0o755)).expect("chmod source");

        let dest = dir.path().join("dest-bin");
        install_from(&src, &dest).expect("install_from succeeds");

        assert_eq!(
            fs::read(&dest).expect("read dest"),
            payload,
            "installed copy must be byte-identical to the source"
        );
        let mode = fs::metadata(&dest).expect("stat dest").permissions().mode();
        assert_eq!(
            mode & 0o111,
            0o111,
            "installed copy must keep the source's executable bits (mode {mode:o})"
        );
    }

    /// The destination's parent directory is created when absent, so the
    /// DaemonSet's `DirectoryOrCreate` hostPath staging never fails on a
    /// missing intermediate dir.
    #[test]
    fn install_from_creates_missing_parent_directory() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let src = dir.path().join("src-bin");
        fs::write(&src, b"envd").expect("write source");

        // `nested/` does not exist yet.
        let dest = dir.path().join("nested").join("envd");
        install_from(&src, &dest).expect("install_from creates parent dir");

        assert_eq!(fs::read(&dest).expect("read dest"), b"envd");
    }

    /// Installing over an existing `dest` replaces it byte-for-byte and
    /// permission-for-permission — the path every chart upgrade hits when
    /// the DaemonSet restages a new envd over the previous one. The atomic
    /// `rename` swaps the directory entry to a fresh inode, so a reader
    /// holding an fd opened *before* the install keeps reading the old
    /// bytes (no in-place truncation that could tear a running Pod's
    /// mmap'd binary).
    #[test]
    fn install_from_overwrites_existing_dest_via_inode_swap() {
        use std::io::Read;

        let dir = tempfile::tempdir().expect("create tempdir");

        // Pre-existing destination: old contents, group/other-writable so
        // a stale perm bit can't masquerade as the new one.
        let dest = dir.path().join("envd");
        fs::write(&dest, b"OLD-binary-bytes").expect("write old dest");
        fs::set_permissions(&dest, fs::Permissions::from_mode(0o666)).expect("chmod old dest");

        // Hold an fd against the old inode before the swap.
        let mut old_fd = fs::File::open(&dest).expect("open old dest");

        // New source: different bytes, executable.
        let src = dir.path().join("src-bin");
        fs::write(&src, b"NEW-binary-bytes-and-then-some").expect("write source");
        fs::set_permissions(&src, fs::Permissions::from_mode(0o755)).expect("chmod source");

        install_from(&src, &dest).expect("install_from overwrites dest");

        // The directory entry now resolves to the new bytes + perms.
        assert_eq!(
            fs::read(&dest).expect("read dest"),
            b"NEW-binary-bytes-and-then-some",
            "install must replace the existing destination's bytes"
        );
        let mode = fs::metadata(&dest).expect("stat dest").permissions().mode();
        assert_eq!(
            mode & 0o777,
            0o755,
            "install must replace the existing destination's perms (mode {mode:o})"
        );

        // The fd opened before the swap still reads the OLD inode: `rename`
        // replaced the name, it did not truncate the old file in place.
        let mut old_contents = Vec::new();
        old_fd
            .read_to_end(&mut old_contents)
            .expect("read held-open old fd");
        assert_eq!(
            old_contents, b"OLD-binary-bytes",
            "rename must swap inodes, not truncate in place — a Pod with the \
             old binary mmap'd keeps reading the old bytes"
        );
    }

    /// `parse_relocate_target` collapses an unset *and* an empty
    /// `SANDBAWKS_ENVD_RELOCATE_TARGET` to "serve in place", and otherwise
    /// yields the configured path. The empty case matters: a chart that sets
    /// the env to "" must not send envd relocating onto `/` (its parent).
    #[test]
    fn parse_relocate_target_treats_unset_and_empty_as_none() {
        assert_eq!(parse_relocate_target(None), None);
        assert_eq!(parse_relocate_target(Some(OsString::new())), None);
        assert_eq!(
            parse_relocate_target(Some(OsString::from("/var/tmp/envd/run/envd"))),
            Some(PathBuf::from("/var/tmp/envd/run/envd")),
        );
    }

    /// No target configured (cold / legacy path): serve from wherever envd was
    /// exec'd, never relocate.
    #[test]
    fn relocate_decision_serves_in_place_when_target_unset() {
        assert_eq!(
            relocate_decision(false, None, Path::new("/sandbawks/bin/envd")),
            RelocateAction::ServeInPlace,
        );
    }

    /// The re-exec'd process where `current_exe` already equals the target
    /// serves rather than copying + re-execing again — the cold-boot-at-target
    /// optimization (the loop *backstop* is the marker, covered below).
    #[test]
    fn relocate_decision_serves_in_place_when_already_at_target() {
        assert_eq!(
            relocate_decision(
                false,
                Some(PathBuf::from("/var/tmp/envd/run/envd")),
                Path::new("/var/tmp/envd/run/envd"),
            ),
            RelocateAction::ServeInPlace,
        );
    }

    /// First boot: envd is exec'd from the read-only hostPath bind
    /// (`/sandbawks/bin/envd`) but the target is the captured rootfs overlay
    /// (`/var/tmp/envd/run/envd`), so it must relocate before serving.
    #[test]
    fn relocate_decision_relocates_when_running_from_host_mount() {
        assert_eq!(
            relocate_decision(
                false,
                Some(PathBuf::from("/var/tmp/envd/run/envd")),
                Path::new("/sandbawks/bin/envd"),
            ),
            RelocateAction::Relocate(PathBuf::from("/var/tmp/envd/run/envd")),
        );
    }

    /// Structural loop backstop (issue #471 review): once the relocated marker
    /// is set, serve unconditionally — even with a target configured and the
    /// paths *disagreeing*, so a path-resolution asymmetry can never produce a
    /// second `execve`.
    #[test]
    fn relocate_decision_serves_in_place_when_marker_set_despite_path_mismatch() {
        assert_eq!(
            relocate_decision(
                true,
                Some(PathBuf::from("/var/tmp/envd/run/envd")),
                Path::new("/sandbawks/bin/envd"),
            ),
            RelocateAction::ServeInPlace,
        );
    }

    /// `canonicalize_or` returns a non-existent path unchanged — the target
    /// legitimately does not exist on the first boot, before the copy.
    #[test]
    fn canonicalize_or_returns_input_for_nonexistent_path() {
        let missing = Path::new("/nonexistent/var/tmp/envd/run/envd");
        assert_eq!(canonicalize_or(missing), missing.to_path_buf());
    }

    /// `canonicalize_or` resolves an existing path to its canonical form, so
    /// the happy-path "already at target" comparison sees matching paths.
    #[test]
    fn canonicalize_or_resolves_existing_path() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let file = dir.path().join("envd");
        fs::write(&file, b"envd").expect("write file");
        assert_eq!(
            canonicalize_or(&file),
            file.canonicalize().expect("canonicalize existing file"),
        );
    }

    /// #560 drain mechanism: a [`DrainHandle::drain`] (as `POST /__quiesce`
    /// issues) force-closes every live gRPC connection and the supervisor
    /// rebinds a fresh listener, acking only once that fresh listener is up.
    /// Drives [`run_grpc_loop`] on its own thread exactly as production does and
    /// asserts at the TCP layer (no gRPC client needed): the held connection is
    /// closed, and a new connection still succeeds.
    ///
    /// This validates the in-process mechanism; the end-to-end gVisor
    /// checkpoint/restore efficacy is validated separately (the A/B harness on a
    /// deployed image — see issue #560), since `runsc` C/R can't run under
    /// `cargo test`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn quiesce_drains_grpc_connections_and_rebinds() {
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpStream;

        // Bind a free port and hand the listener straight to the supervisor —
        // no release-then-rebind gap for another listener to win (issue #567).
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a free port");
        let addr = listener.local_addr().expect("local addr");

        let dir = tempfile::tempdir().expect("create tempdir");
        let filesystem = FilesystemService::with_seed(dir.path().to_path_buf(), Vec::new())
            .expect("filesystem service init");
        let process = ProcessService::with_seed(Vec::new(), Vec::new());

        let (req_tx, req_rx) = tokio::sync::watch::channel(0u64);
        let (done_tx, done_rx) = tokio::sync::watch::channel(0u64);
        let (signal_tx, _) = tokio::sync::broadcast::channel::<()>(1);
        let drain = DrainHandle {
            request: std::sync::Arc::new(req_tx),
            done: done_rx,
        };

        let signal_for_thread = signal_tx.clone();
        let grpc = std::thread::spawn(move || {
            let _ = run_grpc_loop(
                listener,
                filesystem,
                process,
                req_rx,
                done_tx,
                signal_for_thread,
            );
        });

        // Wait for the listener, then hold a connection open across the drain.
        let mut held = None;
        for _ in 0..100 {
            if let Ok(stream) = TcpStream::connect(addr).await {
                held = Some(stream);
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let mut held = held.expect("gRPC listener came up");

        // Drain: force-closes the held connection and rebinds. Acked only once
        // the fresh listener is accepting.
        assert!(
            drain.drain(Duration::from_secs(5)).await,
            "drain should be acked once the fresh listener is up"
        );

        // The held connection is now closed. It may first deliver the server's
        // unprompted HTTP/2 SETTINGS frame (tonic writes it on accept) that was
        // buffered before the force-close, so consume any such in-flight bytes
        // and require the stream to *reach* EOF or a reset within the cap. That
        // — not "zero bytes ever readable" — is what "the connection was closed"
        // means; asserting on the first read alone races the SETTINGS frame and
        // is the source of this test's flake under full-suite load (issue #567).
        let closed = tokio::time::timeout(Duration::from_secs(2), async {
            let mut buf = [0u8; 256];
            // `read` returning `Ok(0)` is a clean EOF; `Err(_)` is a reset; any
            // `Ok(n > 0)` is a buffered server frame — keep draining toward EOF.
            while let Ok(n) = held.read(&mut buf).await {
                if n == 0 {
                    break;
                }
            }
        })
        .await;
        assert!(
            closed.is_ok(),
            "held gRPC connection neither closed nor reset after drain"
        );

        // The rebound listener accepts new connections.
        let fresh = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr)).await;
        assert!(
            matches!(fresh, Ok(Ok(_))),
            "supervisor must rebind a fresh listener after draining"
        );

        // Final shutdown, then join the supervisor thread off the reactor.
        let _ = signal_tx.send(());
        let _ = tokio::task::spawn_blocking(move || grpc.join()).await;
    }
}
