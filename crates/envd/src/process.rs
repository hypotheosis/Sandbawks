//! `ProcessService` implementation.
//!
//! Implements the upstream E2B `process` gRPC service for non-PTY exec:
//! `Start`, `Connect`, `SendInput`, `SendSignal`, `List`, `Update`,
//! plus `StreamInput` and `CloseStdin` to satisfy the full proto
//! surface. PTY allocation is handled by the sibling [`crate::pty`]
//! module (Plan 03 T5).
//!
//! # Registry
//!
//! Each running (or recently-exited) child has an entry in the
//! `entries` map keyed by OS pid. The entry owns:
//!
//! - A [`tokio::sync::broadcast::Sender<ProcessEvent>`] used to fan-out
//!   stdout/stderr chunks and the terminating `EndEvent` to all live
//!   `Connect` / `Start` subscribers. Buffer size is 256 events; late
//!   subscribers may drop history per `broadcast`'s lossy semantics,
//!   which is acceptable for streaming output.
//! - An optional [`tokio::sync::mpsc::Sender<Vec<u8>>`] for stdin
//!   forwarding, populated only when the client opted into stdin at
//!   start. `CloseStdin` drops this sender, which propagates to the
//!   forwarder task and closes the child's stdin pipe.
//! - A [`tokio::sync::Mutex<Option<ProcessEvent>>`] cache of the
//!   terminating `EndEvent` after exit. Late subscribers — those that
//!   join *after* the broadcast already published the end — read this
//!   cache and replay it, so they still observe the exit even though
//!   broadcast is lossy.
//!
//! # Retention
//!
//! After a process exits, its entry remains in the registry for
//! `POST_EXIT_RETENTION` so `List` can report recently-exited
//! processes and late `Connect` calls can still pull the cached
//! `EndEvent`. A detached task sleeps for the retention window and
//! then removes the entry.
//!
//! # Race-freedom argument for late `Connect`
//!
//! `build_event_stream` subscribes to the entry's broadcast channel
//! **before** snapshotting the exit cache. Combined with the wait
//! task's order of (1) cache the end event under the `exit` mutex,
//! (2) publish on broadcast, every interleaving yields the correct
//! stream — see the comment inside `build_event_stream`.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::Stream;
use nix::sys::signal::{kill, Signal as NixSignal};
use nix::unistd::Pid;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{async_trait, Request, Response, Status, Streaming};

use crate::state::{ProcessSnapshot, PtySnapshot};

use crate::proto::process::process_event::data_event::Output as DataOutput;
use crate::proto::process::process_event::{
    DataEvent, EndEvent, Event as ProcessEventKind, StartEvent,
};
use crate::proto::process::process_input::Input as ProcessInputKind;
use crate::proto::process::process_selector::Selector;
use crate::proto::process::process_server::Process;
use crate::proto::process::stream_input_request::Event as StreamInputEventKind;
use crate::proto::process::{
    CloseStdinRequest, CloseStdinResponse, ConnectRequest, ConnectResponse, ListRequest,
    ListResponse, ProcessConfig, ProcessEvent, ProcessInfo, ProcessSelector, SendInputRequest,
    SendInputResponse, SendSignalRequest, SendSignalResponse, Signal as ProtoSignal, StartRequest,
    StartResponse, StreamInputRequest, StreamInputResponse, UpdateRequest, UpdateResponse,
};
use crate::pty::PtyHandle;

/// Broadcast buffer size. Picked at 256 events so a briefly-stalled
/// subscriber has slack to catch up before `broadcast` starts dropping
/// the head of the channel.
const BROADCAST_BUFFER: usize = 256;

/// Stdin mpsc buffer size. Stdin payloads are typically small command
/// fragments; 32 in-flight chunks is plenty without unbounded growth.
const STDIN_BUFFER: usize = 32;

/// Per-call mpsc buffer size for the response stream. Sized so a brief
/// pause in the network reader doesn't stall the broadcast forwarder.
const OUTGOING_BUFFER: usize = 64;

/// Retention window for an exited process. `List` keeps reporting the
/// process and late `Connect` calls can still pull the cached
/// `EndEvent` during this window.
const POST_EXIT_RETENTION: Duration = Duration::from_secs(60);

/// Variant data held by [`ProcessEntry`]. Splits the entry into the
/// two ways a child can be wired:
///
/// - [`ProcessKind::Pipe`] — three separate fds for stdin/stdout/stderr.
///   No variant-specific data; the entry's shared `stdin_tx` and
///   `events` fields cover everything.
/// - [`ProcessKind::Pty`] — a single bidirectional master fd plus a
///   minted PTY id and last-known geometry. All four are populated
///   together at spawn-time and stay in sync for the entry's life.
///   Bundling them in the variant gives the type system the
///   "pty-backed iff has id/rows/cols" invariant for free; downstream
///   call sites (`update` resize, `collect_pty_snapshots`) pattern-match
///   on the variant instead of juggling `Option`s.
enum ProcessKind {
    Pipe,
    Pty {
        /// PTY master fd. Used by `Update` for `TIOCSWINSZ` resize and
        /// by `SendInput`/`StreamInput` to drive the `Pty` input
        /// variant.
        master: Arc<PtyHandle>,
        /// Opaque PTY id assigned at allocation. Carried through to
        /// [`PtySnapshot::id`] so client-visible PTY handles survive
        /// pause/resume.
        id: u64,
        /// Last-known PTY rows. Mirrors the kernel state set by the
        /// most recent `Update`/`TIOCSWINSZ`; read by snapshot capture.
        /// Atomic rather than mutex-guarded because resize is a single
        /// u16 store with no companion state.
        rows: AtomicU16,
        /// Last-known PTY cols. See `rows` for the rationale.
        cols: AtomicU16,
    },
}

/// Per-process state held by [`ProcessService`].
///
/// Fields are private and accessed only via the methods in this
/// module: the wait task writes `exit`, the stdin forwarder reads
/// `stdin_tx`, and `Connect` / `Start` snapshot both.
struct ProcessEntry {
    /// OS pid of the spawned child.
    pid: u32,
    /// The `ProcessConfig` the client sent in `StartRequest`. Echoed
    /// back via `List`.
    config: ProcessConfig,
    /// Optional client-supplied tag, used by `ProcessSelector::Tag`.
    tag: Option<String>,
    /// Fan-out channel. `Connect` and `Start` both subscribe; the
    /// stdout/stderr reader tasks and the wait task publish.
    events: broadcast::Sender<ProcessEvent>,
    /// Input forwarder. For pipe-backed processes this is the child's
    /// stdin; for PTY-backed processes the bytes are written to the
    /// PTY master (the kernel's line discipline delivers them to the
    /// child). `Mutex<Option<_>>` so `CloseStdin` can drop the sender,
    /// propagating EOF to the child for pipe-backed processes.
    stdin_tx: Mutex<Option<mpsc::Sender<Vec<u8>>>>,
    /// Cached `EndEvent` after the child exits. Late subscribers read
    /// this and replay it, since `broadcast` is lossy for past
    /// messages.
    exit: Mutex<Option<ProcessEvent>>,
    /// Unix epoch seconds at which the process was spawned. Captured
    /// once at spawn-time so the snapshot's `started_at` survives
    /// pause/resume with the same wall-clock age.
    started_at: i64,
    /// Discriminates between pipe-backed and PTY-backed entries, with
    /// the PTY-specific bookkeeping bundled into the [`ProcessKind::Pty`]
    /// variant.
    kind: ProcessKind,
}

/// gRPC `Process` service implementing the upstream E2B schema.
///
/// `Clone` is cheap — every field is either `Arc`-wrapped or a small
/// owned `Vec` only populated by [`Self::with_seed`]. Cloning gives the
/// binary's HTTP `/__state` handler a snapshot-capture handle to the
/// same live registry the gRPC server is mutating.
#[derive(Clone)]
pub struct ProcessService {
    entries: Arc<Mutex<HashMap<u32, Arc<ProcessEntry>>>>,
    /// Monotonic source for the `id` field of [`ProcessKind::Pty`].
    /// Behind `Arc` so clones share the same counter — two service
    /// handles must not re-issue the same id.
    next_pty_id: Arc<AtomicU64>,
    /// Boot-time process snapshots restored from `envd-state.json`.
    ///
    /// Read-only for the life of the service. Exposed via
    /// [`Self::boot_seed_processes`] for the future rehydration path
    /// (gVisor-adapter resume; see issue #178). Deliberately **not**
    /// blended into [`Self::collect_process_snapshots`] — if it were,
    /// each pause/resume cycle would persist `live + seed` back out,
    /// and the seed would grow monotonically across cycles (issue #178).
    seeded_processes: Arc<Vec<ProcessSnapshot>>,
    /// Boot-time PTY snapshots, sibling of [`Self::seeded_processes`].
    /// See that field for the lifecycle policy.
    seeded_ptys: Arc<Vec<PtySnapshot>>,
    /// Handle to the process-global orphan reaper (issue #459).
    ///
    /// envd runs as PID 1 in the sandbox, so the kernel reparents every
    /// orphaned grandchild (e.g. the `rustc`/`cc`/`tail` workers forked by
    /// `cargo` or `just`) onto it. The reaper drives the sole `waitpid(-1)`
    /// loop in the process and routes each direct child's exit status back to
    /// its wait task. Obtained from [`global_reaper`], so every service —
    /// and the reaper's `waitpid` loop — share the one registry. See
    /// [`ChildReaper`].
    reaper: ChildReaper,
}

impl Default for ProcessService {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessService {
    /// Construct an empty service. No child processes are spawned until
    /// the first `Start` RPC.
    pub fn new() -> Self {
        Self::with_seed(Vec::new(), Vec::new())
    }

    /// Construct a service pre-seeded with boot-time snapshots.
    ///
    /// Used by the binary entrypoint when `--state-file` exists on
    /// boot: the snapshot's process / PTY arrays land here and are
    /// held read-only for the life of the service. Future rehydration
    /// code reads them via [`Self::boot_seed_processes`] /
    /// [`Self::boot_seed_ptys`] to re-mint live registry entries on
    /// resume (gated on issue #178's follow-up work).
    ///
    /// The seed is **not** blended into [`Self::collect_process_snapshots`]
    /// or [`Self::collect_pty_snapshots`] — that would persist
    /// `live + seed` on shutdown and grow the snapshot monotonically
    /// across pause/resume cycles. See issue #178 for the lifecycle
    /// rationale.
    pub fn with_seed(
        seeded_processes: Vec<ProcessSnapshot>,
        seeded_ptys: Vec<PtySnapshot>,
    ) -> Self {
        // Start the pty-id counter past the largest seeded id so a
        // fresh `Start(pty)` can never collide with a snapshot entry
        // the previous envd already minted. `+1` over `max` (defaulting
        // to 0 when the seed is empty) keeps the first fresh id at 1.
        // The collision concern survives even though the seed no longer
        // appears in `collect_*_snapshots`: when the rehydration path
        // lands, it will re-mint live entries under the seeded ids, and
        // a concurrent fresh `Start(pty)` must not race onto the same id.
        let next_pty_id = seeded_ptys.iter().map(|s| s.id).max().unwrap_or(0) + 1;
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
            next_pty_id: Arc::new(AtomicU64::new(next_pty_id)),
            seeded_processes: Arc::new(seeded_processes),
            seeded_ptys: Arc::new(seeded_ptys),
            // Obtain (and, on first call in this process, start) the global
            // orphan reaper so the service is self-contained: a `Start` RPC
            // works without any caller having to wire up reaping. envd is
            // PID 1 in the sandbox, so the kernel reparents orphaned
            // grandchildren onto it; the reaper collects them and is also the
            // sole `waitpid` owner that delivers direct children's statuses.
            reaper: global_reaper(),
        }
    }

    /// Read-only view of the boot-time process seed.
    ///
    /// Returns the snapshots originally passed to [`Self::with_seed`],
    /// untouched by any live registry churn. Empty for services built
    /// via [`Self::new`].
    ///
    /// Future rehydration code (gVisor-adapter resume path) reads this
    /// to synthesize live registry entries from the pre-pause record.
    /// All other call sites — `/__state`, the shutdown snapshot writer
    /// — must use [`Self::collect_process_snapshots`] instead, which
    /// reports live state only.
    pub fn boot_seed_processes(&self) -> &[ProcessSnapshot] {
        &self.seeded_processes
    }

    /// Read-only view of the boot-time PTY seed. Sibling of
    /// [`Self::boot_seed_processes`].
    pub fn boot_seed_ptys(&self) -> &[PtySnapshot] {
        &self.seeded_ptys
    }

    /// Walk the registry of pipe-backed processes and return a fresh
    /// vector of [`ProcessSnapshot`] entries.
    ///
    /// PTY-backed entries are *excluded* — they belong in
    /// [`Self::collect_pty_snapshots`] per the on-disk schema.
    ///
    /// Boot-time seed entries are deliberately **not** included here:
    /// blending them in would mean every shutdown writer persists
    /// `live + seed`, and the seed would grow monotonically across
    /// pause/resume cycles (issue #178). Callers that need the
    /// pre-pause record — currently only the future rehydration path —
    /// should read [`Self::boot_seed_processes`] directly.
    ///
    /// Ordering is deterministic: `(started_at, pid)` ascending. That
    /// keeps the JSON encoding byte-comparable across probes of the
    /// same registry state — useful for the round-trip property test.
    pub async fn collect_process_snapshots(&self) -> Vec<ProcessSnapshot> {
        let entries = self.entries.lock().await;
        let mut snapshots: Vec<ProcessSnapshot> = entries
            .values()
            .filter(|e| matches!(e.kind, ProcessKind::Pipe))
            .map(|e| ProcessSnapshot {
                pid: e.pid,
                cmd: e.config.cmd.clone(),
                args: e.config.args.clone(),
                env: e
                    .config
                    .envs
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<BTreeMap<_, _>>(),
                cwd: e.config.cwd.clone().map(PathBuf::from).unwrap_or_default(),
                started_at: e.started_at,
            })
            .collect();
        drop(entries);
        snapshots.sort_by(|a, b| a.started_at.cmp(&b.started_at).then(a.pid.cmp(&b.pid)));
        snapshots
    }

    /// Walk the registry of PTY-backed processes and return a fresh
    /// vector of [`PtySnapshot`] entries. Same seed-exclusion policy
    /// as [`Self::collect_process_snapshots`] — see that method for
    /// the rationale.
    ///
    /// Ordering: `(id, pid)` ascending — id alone is unique post-mint
    /// but `pid` is the secondary sort key for stability against future
    /// id-reuse policies.
    pub async fn collect_pty_snapshots(&self) -> Vec<PtySnapshot> {
        let entries = self.entries.lock().await;
        let mut snapshots: Vec<PtySnapshot> = entries
            .values()
            .filter_map(|e| match &e.kind {
                ProcessKind::Pty { id, rows, cols, .. } => Some(PtySnapshot {
                    id: *id,
                    pid: e.pid,
                    rows: rows.load(Ordering::Relaxed),
                    cols: cols.load(Ordering::Relaxed),
                    cmd: e.config.cmd.clone(),
                    args: e.config.args.clone(),
                }),
                ProcessKind::Pipe => None,
            })
            .collect();
        drop(entries);
        snapshots.sort_by(|a, b| a.id.cmp(&b.id).then(a.pid.cmp(&b.pid)));
        snapshots
    }

    /// Resolve a `ProcessSelector` to a pid.
    ///
    /// `Selector::Pid` is direct; `Selector::Tag` walks the registry.
    /// Missing or empty selectors return `InvalidArgument`; unmatched
    /// tags return `NotFound`.
    // `tonic::Status` error; see `classify_input` for why it isn't boxed.
    #[allow(clippy::result_large_err)]
    async fn resolve_selector(&self, selector: Option<ProcessSelector>) -> Result<u32, Status> {
        let sel =
            selector.ok_or_else(|| Status::invalid_argument("process selector is required"))?;
        let inner = sel
            .selector
            .ok_or_else(|| Status::invalid_argument("process selector is empty"))?;
        match inner {
            Selector::Pid(pid) => Ok(pid),
            Selector::Tag(tag) => {
                let entries = self.entries.lock().await;
                entries
                    .values()
                    .find(|e| e.tag.as_deref() == Some(tag.as_str()))
                    .map(|e| e.pid)
                    .ok_or_else(|| Status::not_found(format!("no process with tag {tag}")))
            }
        }
    }

    /// Fetch an entry by pid, cloning the `Arc` so the caller can drop
    /// the map lock immediately and the entry can outlive the lookup.
    // `tonic::Status` error; see `classify_input` for why it isn't boxed.
    #[allow(clippy::result_large_err)]
    async fn entry_for(&self, pid: u32) -> Result<Arc<ProcessEntry>, Status> {
        let entries = self.entries.lock().await;
        entries
            .get(&pid)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("no process with pid {pid}")))
    }
}

/// Build the outgoing event stream for `Start` / `Connect`.
///
/// Subscribes to the entry's broadcast channel *before* snapshotting
/// `exit`. With the wait task's invariant of (1) cache the end event
/// under `exit`, then (2) publish on broadcast, every interleaving
/// yields a correct stream:
///
/// 1. Child still running → forward broadcast messages until the
///    `EndEvent`.
/// 2. Child exits after we subscribed but before we snapshot `exit`
///    → broadcast publishes the `EndEvent` to our subscriber; we don't
///    replay.
/// 3. Child exited before we subscribed → `exit` is `Some` and we
///    replay the cached `EndEvent`, discarding the broadcast receiver.
///
/// `prefix_start_event` is `true` for `Start` (which must announce the
/// pid up front) and `false` for `Connect` (the caller already knows
/// the pid it asked for).
async fn build_event_stream(
    entry: Arc<ProcessEntry>,
    prefix_start_event: bool,
) -> ReceiverStream<Result<ProcessEvent, Status>> {
    let (tx, rx) = mpsc::channel::<Result<ProcessEvent, Status>>(OUTGOING_BUFFER);

    if prefix_start_event {
        // Fresh channel with cap OUTGOING_BUFFER → this send never blocks.
        let _ = tx
            .send(Ok(ProcessEvent {
                event: Some(ProcessEventKind::Start(StartEvent { pid: entry.pid })),
            }))
            .await;
    }

    // Subscribe first, snapshot second — the order is load-bearing
    // for the race-freedom argument above.
    let mut bcast_rx = entry.events.subscribe();
    let exit_snapshot = entry.exit.lock().await.clone();

    if let Some(end_event) = exit_snapshot {
        let _ = tx.send(Ok(end_event)).await;
        // Drop bcast_rx so the broadcaster doesn't keep buffering
        // messages for a subscriber that will never read them.
        drop(bcast_rx);
    } else {
        tokio::spawn(async move {
            loop {
                match bcast_rx.recv().await {
                    Ok(ev) => {
                        let is_end = matches!(ev.event, Some(ProcessEventKind::End(_)));
                        if tx.send(Ok(ev)).await.is_err() {
                            return;
                        }
                        if is_end {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Subscriber fell behind. The contract is
                        // best-effort streaming; skip lagged messages
                        // rather than tearing down the stream.
                        continue;
                    }
                }
            }
        });
    }

    ReceiverStream::new(rx)
}

/// Map of direct-child pid → the channel that delivers its exit status.
///
/// One entry per live [`spawn_pipe_child`] / [`spawn_pty_child`] whose wait
/// task is awaiting its child's exit. Guarded by a [`std::sync::Mutex`]
/// (never a tokio mutex): every holder — the reaper's drain loop and the
/// registration in `spawn_*` — only runs synchronous syscalls / map ops
/// while holding it, never an `.await`.
type ReaperMap = HashMap<Pid, oneshot::Sender<std::process::ExitStatus>>;

/// Handle to the process-global orphan reaper (issue #459).
///
/// envd is PID 1 in the sandbox, so the kernel reparents every orphaned
/// grandchild onto it. The reaper owns the **only** `waitpid(-1)` loop in
/// the process — see [`ChildReaper::drain`] — and a registry of the direct
/// children whose exit status that loop must hand back rather than discard.
/// The handle is a cheap [`Arc`] clone shared by every [`ProcessService`];
/// the loop itself runs once per process on a dedicated thread started by
/// [`global_reaper`].
///
/// # Why a custom reaper at all
///
/// tokio reaps the children it spawns by `waitpid`-ing each specific pid
/// (via a pidfd readiness wake on Linux 5.3+, or its internal `SIGCHLD`
/// path otherwise) when you poll [`tokio::process::Child::wait`]. But a
/// grandchild forked by `cargo`/`just` is unknown to tokio; nothing waits
/// on it, so it lingers as a zombie. A naive `waitpid(-1)` reaper would fix
/// that but **steal** tokio's children too: whoever calls `waitpid(-1)`
/// first collects the status, and tokio's later `waitpid(pid)` then fails
/// with `ECHILD`, corrupting the `Process.Start` exit-code contract.
///
/// The resolution: this reaper is the *sole* `waitpid` caller. Direct
/// children register a [`oneshot`] sender here keyed by pid; `drain` routes
/// their status back over it. Their wait tasks never call `Child::wait`
/// (so tokio never races us) and hold the [`tokio::process::Child`] alive
/// (so tokio never orphan-reaps it on drop) until the status arrives.
#[derive(Clone, Default)]
struct ChildReaper {
    owned: Arc<StdMutex<ReaperMap>>,
}

impl ChildReaper {
    /// Construct an empty reaper. The `waitpid` loop is started by
    /// [`global_reaper`] on first use.
    fn new() -> Self {
        Self::default()
    }

    /// Lock the registry for registration.
    ///
    /// `spawn_*` hold the returned guard across the `Command::spawn` that
    /// creates the child **and** the subsequent insert of its sender. Since
    /// [`Self::route`] also takes this lock before deciding a reaped pid is an
    /// orphan, the spawn/register race is closed: a child that exits before
    /// its sender is inserted cannot be discarded as an orphan, because
    /// `route` blocks on this guard until registration completes (the pid
    /// does not even exist until the fork that happens under the guard).
    fn registry(&self) -> MutexGuard<'_, ReaperMap> {
        // A poisoned mutex means a holder panicked mid-critical-section.
        // None of the critical sections (a `waitpid` call, a map op) can
        // unwind in practice, so treat poisoning as the unrecoverable
        // invariant violation it is.
        self.owned.lock().expect("reaper registry mutex poisoned")
    }

    /// Run the reaper loop. Called once per process on a dedicated thread,
    /// driving the `runtime` and `sigchld` listener built by [`global_reaper`].
    ///
    /// The private current-thread runtime keeps the reaper's `SIGCHLD`
    /// listener independent of the main serving runtime — and, crucially, of
    /// each `#[tokio::test]`'s short-lived runtime, so the loop survives across
    /// tests in a shared test binary. The runtime and listener are built on
    /// the *caller's* thread (see [`global_reaper`]) so an init failure panics
    /// there rather than silently killing this thread.
    ///
    /// The loop drains, then waits for the next `SIGCHLD`; `recv` coalesces
    /// signals, so one wakeup may stand in for several near-simultaneous child
    /// exits.
    fn run(&self, runtime: tokio::runtime::Runtime, mut sigchld: tokio::signal::unix::Signal) {
        runtime.block_on(async {
            loop {
                // Drain first so zombies that exited before the listener
                // armed — or were coalesced into a single wakeup — are all
                // collected.
                self.drain();
                if sigchld.recv().await.is_none() {
                    // The signal driver is gone, i.e. the process is exiting.
                    break;
                }
            }
        });
    }

    /// Reap every currently-available zombie in one burst.
    ///
    /// Loops `waitpid(-1, WNOHANG)` until no child is ready, routing each
    /// reaped direct child's status to its wait task and discarding orphans.
    fn drain(&self) {
        use nix::errno::Errno;
        use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};

        loop {
            match waitpid(None, Some(WaitPidFlag::WNOHANG)) {
                Ok(WaitStatus::Exited(pid, code)) => self.route(pid, exit_status_from_code(code)),
                Ok(WaitStatus::Signaled(pid, sig, core_dumped)) => {
                    self.route(pid, exit_status_from_signal(sig, core_dumped));
                }
                // No more children are waitable right now.
                Ok(WaitStatus::StillAlive) => break,
                // We request neither `WUNTRACED`/`WCONTINUED` nor ptrace, so
                // the kernel never reports a Stopped/Continued/Ptrace* state
                // here. Break defensively anyway: a `continue` on a sticky
                // non-terminal state (one `waitpid` keeps re-reporting) would
                // busy-loop forever.
                Ok(_) => break,
                // `ECHILD` (no children remain) and `EINTR` (interrupted) are
                // the expected, benign ways a burst ends.
                Err(Errno::ECHILD | Errno::EINTR) => break,
                // Anything else is unexpected (e.g. `EINVAL` from a future
                // kernel feature, or an OS bug). End the burst as before, but
                // surface it — this is the only hint a real sandbox would give
                // that the reaper is misbehaving.
                Err(e) => {
                    tracing::warn!(errno = ?e, "orphan reaper: unexpected waitpid error; ending drain burst");
                    break;
                }
            }
        }
    }

    /// Deliver `status` to the wait task of direct child `pid`, if any.
    ///
    /// Takes the registry lock per reaped pid (the only `.await`-free section
    /// here) — see [`Self::registry`] for how that closes the spawn/register
    /// race. A reaped pid that is **not** in the registry is an adopted
    /// orphan grandchild: reaping it was the entire job and nothing awaits
    /// its status, so it is dropped. A send error means the wait task was
    /// already dropped (handler cancelled / shutdown); the zombie is reaped
    /// regardless, so there is nothing more to do.
    fn route(&self, pid: Pid, status: std::process::ExitStatus) {
        if let Some(tx) = self.registry().remove(&pid) {
            let _ = tx.send(status);
        }
    }

    /// Spawn `cmd` and register it with the reaper as one atomic step,
    /// returning the [`Child`](tokio::process::Child), its pid, and the
    /// [`oneshot::Receiver`] its wait task awaits for the routed exit status.
    ///
    /// The single home for the spawn/register invariant, shared by
    /// [`spawn_pipe_child`] and [`spawn_pty_child`] — the race-closing
    /// semantics are subtle enough that two copies would invite drift.
    ///
    /// The registry lock is held across `cmd.spawn()` and the sender insert so
    /// a child cannot be reaped-and-discarded before its sender is in the map
    /// (the pid does not even exist until the fork under the guard). The cost
    /// is that this lock **globally serializes every `Command::spawn` in the
    /// process**: concurrent `Start` RPCs across all [`ProcessService`] clones
    /// contend on it for the duration of each fork. That is fine for envd's
    /// workload — do not parallelize spawning without preserving the
    /// race-closing invariant. (The standard alternative is to reserve a
    /// registry slot under a per-spawn ticket and patch in the real pid
    /// post-spawn under a short lock — strictly more code than is warranted
    /// today.)
    fn spawn_registered(
        &self,
        cmd: &mut Command,
    ) -> std::io::Result<(
        tokio::process::Child,
        u32,
        oneshot::Receiver<std::process::ExitStatus>,
    )> {
        let (exit_tx, exit_rx) = oneshot::channel();
        let mut registry = self.registry();
        let child = cmd.spawn()?;
        let pid = child
            .id()
            .ok_or_else(|| std::io::Error::other("spawned child has no pid"))?;
        registry.insert(Pid::from_raw(pid as i32), exit_tx);
        Ok((child, pid, exit_rx))
    }
}

/// The process-global reaper handle, started on first access.
static REAPER: OnceLock<ChildReaper> = OnceLock::new();

/// Return the process-global [`ChildReaper`], starting its `waitpid` loop on
/// a dedicated thread the first time it is called.
///
/// A single reaper per process is mandatory, not merely an optimisation:
/// `waitpid(-1)` reaps *any* child, so two reapers in one process would each
/// collect (and, finding it unregistered, discard) the other's direct
/// children, hanging their wait tasks. Every [`ProcessService`] therefore
/// shares this one handle. Idempotent via [`OnceLock`]; the thread runs for
/// the life of the process.
fn global_reaper() -> ChildReaper {
    REAPER
        .get_or_init(|| {
            let reaper = ChildReaper::new();
            let reaper_for_thread = reaper.clone();

            // Build the runtime and install the `SIGCHLD` listener here, on the
            // caller's thread, rather than inside the reaper thread. A failure
            // is an unrecoverable boot-time invariant violation (ENOMEM-class):
            // panicking here propagates out through `ProcessService::with_seed`
            // and aborts startup. Doing it on the reaper thread instead would
            // only unwind that thread, leaving a silently-dead reaper behind
            // which the `OnceLock` would hand out forever — every later `Start`
            // would register a sender nothing routes and hang on `exit_rx`.
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build orphan-reaper runtime");
            let sigchld = {
                // `signal` must be called inside a runtime context; the listener
                // binds to `runtime`'s signal driver and so moves with it to the
                // reaper thread.
                let _enter = runtime.enter();
                signal(SignalKind::child()).expect("install SIGCHLD listener for orphan reaper")
            };

            std::thread::Builder::new()
                .name("envd-orphan-reaper".to_string())
                .spawn(move || reaper_for_thread.run(runtime, sigchld))
                .expect("spawn orphan-reaper thread");
            reaper
        })
        .clone()
}

/// Reconstruct a [`std::process::ExitStatus`] for a normally-exited child.
///
/// [`std::os::unix::process::ExitStatusExt::from_raw`] expects the raw
/// `wait(2)` status word, in which a normal exit encodes the code in bits
/// 8..16 with the low 7 bits clear (so `WIFEXITED` holds). nix already
/// decoded the code for us, so we re-encode it into that word; the wait
/// task then reads it back with `.code()` exactly as it did the status
/// from `Child::wait`.
fn exit_status_from_code(code: i32) -> std::process::ExitStatus {
    use std::os::unix::process::ExitStatusExt;
    std::process::ExitStatus::from_raw((code & 0xff) << 8)
}

/// Reconstruct a [`std::process::ExitStatus`] for a signal-terminated child.
///
/// In the raw `wait(2)` status word the terminating signal occupies the low
/// 7 bits and bit 7 (`0x80`) flags a core dump (`WIFSIGNALED` / `WCOREDUMP`),
/// which is what `.signal()` decodes.
fn exit_status_from_signal(sig: NixSignal, core_dumped: bool) -> std::process::ExitStatus {
    use std::os::unix::process::ExitStatusExt;
    let raw = sig as i32 | if core_dumped { 0x80 } else { 0 };
    std::process::ExitStatus::from_raw(raw)
}

/// Spawn the child described by `req`, register it, and wire up the
/// reader + wait tasks. Returns the entry Arc once it is in the
/// registry.
///
/// Dispatches on whether the client asked for a PTY: pipe-backed
/// processes go through [`spawn_pipe_child`] (three separate fds for
/// stdin / stdout / stderr); PTY-backed processes go through
/// [`spawn_pty_child`] (a single bidirectional master fd, with the
/// slave wired as the child's controlling terminal).
// `tonic::Status` error; see `classify_input` for why it isn't boxed.
#[allow(clippy::result_large_err)]
async fn spawn_child(
    service: &ProcessService,
    req: StartRequest,
) -> Result<Arc<ProcessEntry>, Status> {
    let cfg = req
        .process
        .ok_or_else(|| Status::invalid_argument("process config is required"))?;

    if let Some(pty_cfg) = req.pty {
        let size = pty_cfg
            .size
            .ok_or_else(|| Status::invalid_argument("pty.size is required when pty is set"))?;
        spawn_pty_child(service, cfg, req.tag, size.rows, size.cols).await
    } else {
        // Proto field is `optional bool stdin` with the default-true
        // contract documented in the schema for backwards-compat with
        // older SDKs that omit it.
        let want_stdin = req.stdin.unwrap_or(true);
        spawn_pipe_child(service, cfg, req.tag, want_stdin).await
    }
}

/// Spawn a non-PTY child with separate stdin/stdout/stderr pipes.
// `tonic::Status` error; see `classify_input` for why it isn't boxed.
#[allow(clippy::result_large_err)]
async fn spawn_pipe_child(
    service: &ProcessService,
    cfg: ProcessConfig,
    tag: Option<String>,
    want_stdin: bool,
) -> Result<Arc<ProcessEntry>, Status> {
    let mut cmd = Command::new(&cfg.cmd);
    cmd.args(&cfg.args)
        .envs(cfg.envs.iter())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(if want_stdin {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        // If the wait task is dropped without delivering the exit status
        // (handler panic / shutdown), dropping the held `Child` SIGKILLs the
        // process; the orphan reaper then collects the resulting zombie and
        // clears its registry entry (the routed status is discarded since
        // nothing awaits it). Without this the killed child would linger.
        .kill_on_drop(true);
    if let Some(cwd) = &cfg.cwd {
        cmd.current_dir(cwd);
    }

    // Capture the GenAI tool identity + (gated, redacted) argument string
    // before `cfg` is moved into the registry entry below.
    let tool_name = crate::gen_ai::command_tool_name(&cfg.cmd).to_owned();
    let tool_args = crate::gen_ai::capture_content()
        .then(|| crate::gen_ai::redact_command(&cfg.cmd, &cfg.args));

    // Fork the child and register it with the orphan reaper atomically (issue
    // #459); see [`ChildReaper::spawn_registered`] for the race-closing
    // invariant this relies on.
    let (mut child, pid, exit_rx) = service
        .reaper
        .spawn_registered(&mut cmd)
        .map_err(|e| Status::internal(format!("spawn failed: {e}")))?;

    // `_` receiver on the channel is unused; subscribers will create
    // their own via `events_tx.subscribe()` in `build_event_stream`.
    let (events_tx, _) = broadcast::channel::<ProcessEvent>(BROADCAST_BUFFER);

    let child_stdin = child.stdin.take();
    let stdin_tx = if let Some(mut pipe) = child_stdin {
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(STDIN_BUFFER);
        tokio::spawn(async move {
            while let Some(chunk) = rx.recv().await {
                if pipe.write_all(&chunk).await.is_err() {
                    return;
                }
                if pipe.flush().await.is_err() {
                    return;
                }
            }
            // Sender dropped (CloseStdin or service shutdown) →
            // explicitly shutdown the pipe so the child sees EOF.
            let _ = pipe.shutdown().await;
        });
        Some(tx)
    } else {
        None
    };

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let entry = Arc::new(ProcessEntry {
        pid,
        config: cfg,
        tag,
        events: events_tx.clone(),
        stdin_tx: Mutex::new(stdin_tx),
        exit: Mutex::new(None),
        started_at: now_unix_seconds(),
        kind: ProcessKind::Pipe,
    });

    service.entries.lock().await.insert(pid, Arc::clone(&entry));

    let stdout_handle = stdout.map(|out| spawn_reader_task(out, events_tx.clone(), false));
    let stderr_handle = stderr.map(|err| spawn_reader_task(err, events_tx.clone(), true));

    // GenAI `execute_tool` span: opened here (so it nests under the
    // in-flight `Start` handler span) and closed by the wait task once
    // the child exits, so its duration tracks the command's lifetime.
    //
    // Created *after* the reader tasks above on purpose: the readers emit
    // no `tracing` events, so they have no span to parent. Their events
    // (if any are ever added) intentionally nest at the handler span, not
    // this tool span — if that changes, hoist this above the reader
    // spawns and `.instrument()` each reader future.
    let tool_span = crate::gen_ai::tool_span(&tool_name, pid as u64);
    if let Some(args) = &tool_args {
        tool_span.record(crate::gen_ai::GEN_AI_TOOL_CALL_ARGUMENTS, args.as_str());
    }

    let entries_for_wait = Arc::clone(&service.entries);
    let entry_for_wait = Arc::clone(&entry);
    tokio::spawn(async move {
        // Hold the `Child` for the lifetime of this task so tokio neither
        // races our reaper (we never call `child.wait()`) nor orphan-reaps
        // the pid on drop. The orphan reaper (issue #459) is the sole
        // `waitpid` caller; it routes this child's status to us over
        // `exit_rx`. See [`ChildReaper`].
        let child_guard = child;
        let status = match exit_rx.await {
            Ok(s) => Ok(s),
            // The sender is dropped only if the reaper reaped this pid but
            // its registry entry was already gone — not reachable on the
            // normal path. Record a wait failure rather than hang.
            Err(_) => Err(std::io::Error::other(
                "reaper dropped exit-status sender before delivery",
            )),
        };

        // Drain stdout/stderr readers BEFORE publishing the end event,
        // so any final buffered output reaches subscribers before they
        // see the exit. The readers naturally finish when the child's
        // pipe ends EOF (shortly after `wait` returns).
        if let Some(h) = stdout_handle {
            let _ = h.await;
        }
        if let Some(h) = stderr_handle {
            let _ = h.await;
        }

        let end_event = build_end_event(&status);
        record_tool_exit(&tool_span, &end_event);
        // INVARIANT: this explicit `drop` is what flushes the OTel span
        // via the `tracing-opentelemetry` bridge (export fires on span
        // close). It is NOT redundant with the end-of-scope drop: without
        // it the span would stay alive until the task ends after
        // `POST_EXIT_RETENTION`, inflating the span's duration by ~60s.
        drop(tool_span);

        // Cache BEFORE publish so a late subscriber that snapshots
        // `exit` post-publish observes a consistent `Some(end)`
        // instead of a transient `None`.
        {
            let mut guard = entry_for_wait.exit.lock().await;
            *guard = Some(end_event.clone());
        }
        let _ = entry_for_wait.events.send(end_event);

        // Exit recorded; release the held `Child` now. Our reaper already
        // reaped the pid (that is what resolved `exit_rx`), so tokio's
        // `kill_on_drop` reap is a harmless no-op (`ESRCH`/`ECHILD` ignored).
        // Dropping here, rather than at end of scope, frees the pidfd fd
        // instead of pinning it across the retention sleep below.
        drop(child_guard);

        // Schedule removal after the retention window. `List` keeps
        // reporting the process during this window per the issue's
        // contract.
        tokio::time::sleep(POST_EXIT_RETENTION).await;
        entries_for_wait.lock().await.remove(&pid);
    });

    Ok(entry)
}

/// Spawn a PTY-backed child.
///
/// Allocates a master/slave pair with the requested initial window
/// size, hands three clones of the slave to the child as stdin /
/// stdout / stderr (`Stdio::from(OwnedFd)` consumes each clone and
/// dup2's it into place in the child), and installs the
/// [`crate::pty::install_controlling_tty`] `pre_exec` callback so the
/// child becomes a session leader and acquires the slave as its
/// controlling terminal before `execvp`.
///
/// In the parent, the slave clones are closed by the spawn machinery
/// (or fall out of scope here); only the master remains, wrapped in
/// an [`PtyHandle`] for non-blocking async I/O.
///
/// The PTY-specific I/O wiring differs from the pipe path:
///
/// - **Output reader**: a single task reads from the master and emits
///   `DataOutput::Pty(bytes)` events. The PTY merges what the child
///   wrote to stdout *and* stderr (since both are the slave), plus the
///   line-discipline echo of bytes the client wrote to the master.
/// - **Input forwarder**: the entry's `stdin_tx` mpsc channel feeds a
///   task that writes to the master. The PTY's cooked-mode line
///   discipline handles delivery to the child.
// `tonic::Status` error; see `classify_input` for why it isn't boxed.
#[allow(clippy::result_large_err)]
async fn spawn_pty_child(
    service: &ProcessService,
    cfg: ProcessConfig,
    tag: Option<String>,
    rows: u32,
    cols: u32,
) -> Result<Arc<ProcessEntry>, Status> {
    // Capture the GenAI tool identity + (gated, redacted) argument string
    // before `cfg` is moved into the registry entry below.
    let tool_name = crate::gen_ai::command_tool_name(&cfg.cmd).to_owned();
    let tool_args = crate::gen_ai::capture_content()
        .then(|| crate::gen_ai::redact_command(&cfg.cmd, &cfg.args));

    let (master, slave) =
        crate::pty::open(rows, cols).map_err(|e| Status::internal(format!("openpty: {e}")))?;
    // `try_clone` dup's the kernel fd; each clone is consumed by
    // `Stdio::from(_)` below, which dup2's it onto fd 0/1/2 in the
    // child and then closes the dup'd fd in the parent.
    let slave_stdin = slave
        .try_clone()
        .map_err(|e| Status::internal(format!("dup pty slave (stdin): {e}")))?;
    let slave_stdout = slave
        .try_clone()
        .map_err(|e| Status::internal(format!("dup pty slave (stdout): {e}")))?;
    let slave_stderr = slave; // moved last, consumes the original

    let mut cmd = Command::new(&cfg.cmd);
    cmd.args(&cfg.args)
        .envs(cfg.envs.iter())
        .stdin(Stdio::from(slave_stdin))
        .stdout(Stdio::from(slave_stdout))
        .stderr(Stdio::from(slave_stderr))
        .kill_on_drop(true);
    if let Some(cwd) = &cfg.cwd {
        cmd.current_dir(cwd);
    }
    // SAFETY: the callback only calls `setsid` and `ioctl`, both
    // async-signal-safe. It does not allocate, lock, or read parent
    // memory observably — the contract `Command::pre_exec` demands
    // on a multi-threaded parent.
    unsafe {
        cmd.pre_exec(crate::pty::install_controlling_tty);
    }

    // Fork + register atomically; see [`ChildReaper::spawn_registered`].
    let (child, pid, exit_rx) = service
        .reaper
        .spawn_registered(&mut cmd)
        .map_err(|e| Status::internal(format!("spawn pty child: {e}")))?;

    let master_handle =
        PtyHandle::new(master).map_err(|e| Status::internal(format!("wrap pty master: {e}")))?;
    let master_handle = Arc::new(master_handle);

    let (events_tx, _) = broadcast::channel::<ProcessEvent>(BROADCAST_BUFFER);

    // Input forwarder: mpsc → master fd.
    let (stdin_tx, mut stdin_rx) = mpsc::channel::<Vec<u8>>(STDIN_BUFFER);
    {
        let master_for_writer = Arc::clone(&master_handle);
        tokio::spawn(async move {
            while let Some(chunk) = stdin_rx.recv().await {
                if master_for_writer.write_all(&chunk).await.is_err() {
                    return;
                }
            }
        });
    }

    // Output reader: master fd → broadcast as `DataOutput::Pty`.
    let reader_handle = {
        let master_for_reader = Arc::clone(&master_handle);
        let events_tx_for_reader = events_tx.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            loop {
                match master_for_reader.read(&mut buf).await {
                    Ok(0) => return,
                    Ok(n) => {
                        let chunk = buf[..n].to_vec();
                        let event = ProcessEvent {
                            event: Some(ProcessEventKind::Data(DataEvent {
                                output: Some(DataOutput::Pty(chunk)),
                            })),
                        };
                        // Lossy fan-out: `Err` just means no live
                        // subscribers — keep draining so the child's
                        // tty doesn't fill up.
                        let _ = events_tx_for_reader.send(event);
                    }
                    Err(_) => return,
                }
            }
        })
    };

    let pty_id = service.next_pty_id.fetch_add(1, Ordering::Relaxed);
    let entry = Arc::new(ProcessEntry {
        pid,
        config: cfg,
        tag,
        events: events_tx.clone(),
        stdin_tx: Mutex::new(Some(stdin_tx)),
        exit: Mutex::new(None),
        started_at: now_unix_seconds(),
        kind: ProcessKind::Pty {
            master: Arc::clone(&master_handle),
            id: pty_id,
            rows: AtomicU16::new(rows.min(u16::MAX as u32) as u16),
            cols: AtomicU16::new(cols.min(u16::MAX as u32) as u16),
        },
    });

    service.entries.lock().await.insert(pid, Arc::clone(&entry));

    // GenAI `execute_tool` span: see the pipe-backed path for the
    // lifetime rationale. PTY and pipe spawns are traced identically.
    let tool_span = crate::gen_ai::tool_span(&tool_name, pid as u64);
    if let Some(args) = &tool_args {
        tool_span.record(crate::gen_ai::GEN_AI_TOOL_CALL_ARGUMENTS, args.as_str());
    }

    let entries_for_wait = Arc::clone(&service.entries);
    let entry_for_wait = Arc::clone(&entry);
    tokio::spawn(async move {
        // Hold the `Child` and await the reaper-routed status instead of
        // calling `child.wait()`; see the pipe-backed path and
        // [`ChildReaper`] for the rationale (issue #459).
        let child_guard = child;
        let status = match exit_rx.await {
            Ok(s) => Ok(s),
            Err(_) => Err(std::io::Error::other(
                "reaper dropped exit-status sender before delivery",
            )),
        };

        // Drain the reader before publishing `EndEvent`, so any final
        // tty output reaches subscribers before the exit signal.
        // The reader exits naturally once all slave fds close (which
        // happens automatically when the child terminates).
        let _ = reader_handle.await;

        let end_event = build_end_event(&status);
        record_tool_exit(&tool_span, &end_event);
        drop(tool_span);
        {
            let mut guard = entry_for_wait.exit.lock().await;
            *guard = Some(end_event.clone());
        }
        let _ = entry_for_wait.events.send(end_event);

        // See the pipe-backed path: release the held `Child` once the exit
        // is recorded so its pidfd fd is not pinned across the retention
        // sleep; the reaper already collected the pid.
        drop(child_guard);

        tokio::time::sleep(POST_EXIT_RETENTION).await;
        entries_for_wait.lock().await.remove(&pid);
    });

    Ok(entry)
}

/// Validate that an incoming `ProcessInput` matches the registered
/// process's I/O backing and return its byte payload.
///
/// Pipe-backed processes accept only `Stdin`; PTY-backed processes
/// accept only `Pty`. Mixing the two would silently drop data — a
/// `Pty` payload sent to a pipe-backed process would land in the
/// child's stdin pipe with no terminal semantics, and vice versa.
// `clippy::result_large_err`: `tonic::Status` is ~176 B, so any
// `Result<_, Status>` trips this lint. Boxing the error would force
// callers (the gRPC handler bodies) to unbox before propagating with
// `?` into the tonic-generated return type. Same pattern as
// `process::send_input` and the filesystem helpers.
#[allow(clippy::result_large_err)]
fn classify_input(entry: &ProcessEntry, input: ProcessInputKind) -> Result<Vec<u8>, Status> {
    let is_pty = matches!(entry.kind, ProcessKind::Pty { .. });
    match (is_pty, input) {
        (false, ProcessInputKind::Stdin(b)) => Ok(b),
        (true, ProcessInputKind::Pty(b)) => Ok(b),
        (false, ProcessInputKind::Pty(_)) => Err(Status::failed_precondition(
            "process is pipe-backed; use stdin input variant",
        )),
        (true, ProcessInputKind::Stdin(_)) => Err(Status::failed_precondition(
            "process is PTY-backed; use pty input variant",
        )),
    }
}

/// Wall-clock at child spawn, encoded as unix epoch seconds.
///
/// Returns `0` on the (impossible-in-practice) case where the system
/// clock is set before the epoch — better to emit a non-fatal bogus
/// timestamp than to panic mid-spawn. Snapshot consumers tolerate the
/// `0` sentinel because the lifecycle bridge already used it.
fn now_unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Construct the `EndEvent` describing how a child terminated.
///
/// Maps `ExitStatus::code()` for normal exits, `ExitStatus::signal()`
/// (via `std::os::unix::process::ExitStatusExt`) for signal-terminated
/// children, and the wait error for the I/O-error case. `exit_code`
/// for signal-terminated processes defaults to `-1` since the proto
/// has no separate "killed by signal" field.
fn build_end_event(status: &std::io::Result<std::process::ExitStatus>) -> ProcessEvent {
    use std::os::unix::process::ExitStatusExt;

    let (exit_code, exited, status_str, error) = match status {
        Ok(s) => {
            let exited = s.code().is_some();
            let code = s.code().unwrap_or(-1);
            let status_str = if let Some(c) = s.code() {
                format!("exited with code {c}")
            } else if let Some(sig) = s.signal() {
                format!("terminated by signal {sig}")
            } else {
                "exited".to_string()
            };
            (code, exited, status_str, None)
        }
        Err(e) => (-1, false, "wait failed".to_string(), Some(e.to_string())),
    };

    ProcessEvent {
        event: Some(ProcessEventKind::End(EndEvent {
            exit_code,
            exited,
            status: status_str,
            error,
        })),
    }
}

/// Record the exit outcome of a child onto its GenAI `execute_tool` span.
///
/// Reads the already-built [`EndEvent`] (the single source of truth for
/// exit code / status) and forwards the primitives to
/// [`crate::gen_ai::record_process_exit`]. A non-`End` event is
/// unreachable here — the wait task only ever builds an `EndEvent` — so
/// it is a silent no-op rather than an error path.
fn record_tool_exit(span: &tracing::Span, end_event: &ProcessEvent) {
    if let Some(ProcessEventKind::End(end)) = &end_event.event {
        crate::gen_ai::record_process_exit(span, end.exit_code, end.exited, end.error.is_some());
    }
}

/// Spawn the per-stream reader task.
///
/// Reads in fixed-size byte chunks rather than line-by-line so that
/// non-UTF8 payloads and partial-line writes (e.g., `printf "hi"`
/// without a trailing newline) still surface. The proto's `bytes`
/// typing on stdout/stderr makes this the byte-faithful
/// representation; clients that want lines can split on `\n`.
fn spawn_reader_task<R>(
    mut reader: R,
    tx: broadcast::Sender<ProcessEvent>,
    is_stderr: bool,
) -> JoinHandle<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut buf = vec![0u8; 4096];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => return,
                Ok(n) => {
                    let chunk = buf[..n].to_vec();
                    let output = if is_stderr {
                        DataOutput::Stderr(chunk)
                    } else {
                        DataOutput::Stdout(chunk)
                    };
                    let event = ProcessEvent {
                        event: Some(ProcessEventKind::Data(DataEvent {
                            output: Some(output),
                        })),
                    };
                    // `broadcast::send` returning `Err(SendError)` just
                    // means no live subscribers — keep reading so the
                    // child's pipe doesn't block, and so any
                    // post-attach subscribers see fresh data.
                    let _ = tx.send(event);
                }
                Err(_) => return,
            }
        }
    })
}

#[async_trait]
impl Process for ProcessService {
    type StartStream = Pin<Box<dyn Stream<Item = Result<StartResponse, Status>> + Send>>;
    type ConnectStream = Pin<Box<dyn Stream<Item = Result<ConnectResponse, Status>> + Send>>;

    #[tracing::instrument(skip_all, name = "handler:process::list")]
    async fn list(&self, _request: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        let entries = self.entries.lock().await;
        let processes = entries
            .values()
            .map(|e| ProcessInfo {
                config: Some(e.config.clone()),
                pid: e.pid,
                tag: e.tag.clone(),
            })
            .collect();
        Ok(Response::new(ListResponse { processes }))
    }

    // `clippy::result_large_err` fires on the inner closure: every
    // stream item is `Result<_, tonic::Status>` and `Status` is ~176 B.
    // Boxing the error would diverge from the gRPC trait the tonic
    // codegen requires, so we allow it on the handler itself — same
    // pattern `filesystem::resolve` already uses.
    #[allow(clippy::result_large_err)]
    #[tracing::instrument(skip_all, name = "handler:process::start")]
    async fn start(
        &self,
        request: Request<StartRequest>,
    ) -> Result<Response<Self::StartStream>, Status> {
        let entry = spawn_child(self, request.into_inner()).await?;
        let event_stream = build_event_stream(entry, true).await;
        let response_stream =
            event_stream.map(|res| res.map(|event| StartResponse { event: Some(event) }));
        Ok(Response::new(Box::pin(response_stream) as Self::StartStream))
    }

    // Same `result_large_err` reasoning as `start`.
    #[allow(clippy::result_large_err)]
    #[tracing::instrument(skip_all, name = "handler:process::connect")]
    async fn connect(
        &self,
        request: Request<ConnectRequest>,
    ) -> Result<Response<Self::ConnectStream>, Status> {
        let req = request.into_inner();
        let pid = self.resolve_selector(req.process).await?;
        let entry = self.entry_for(pid).await?;
        let event_stream = build_event_stream(entry, false).await;
        let response_stream =
            event_stream.map(|res| res.map(|event| ConnectResponse { event: Some(event) }));
        Ok(Response::new(
            Box::pin(response_stream) as Self::ConnectStream
        ))
    }

    #[tracing::instrument(skip_all, name = "handler:process::update")]
    async fn update(
        &self,
        request: Request<UpdateRequest>,
    ) -> Result<Response<UpdateResponse>, Status> {
        let req = request.into_inner();
        let pid = self.resolve_selector(req.process).await?;
        let entry = self.entry_for(pid).await?;

        // The only field of `UpdateRequest` we currently honor is the
        // PTY window-size resize. Future cgroup / resource hooks slot
        // in next to this match arm.
        if let Some(pty) = req.pty {
            let size = pty
                .size
                .ok_or_else(|| Status::invalid_argument("update: pty.size is required"))?;
            let ProcessKind::Pty {
                master, rows, cols, ..
            } = &entry.kind
            else {
                return Err(Status::failed_precondition(
                    "update: process is not PTY-backed",
                ));
            };
            master
                .resize(size.rows, size.cols)
                .map_err(|e| Status::internal(format!("pty resize: {e}")))?;
            // Mirror the kernel-side resize into our bookkeeping so the
            // next snapshot reports the new geometry. `min` clamps the
            // proto's u32 down to the snapshot's u16 — kernel rows/cols
            // are already u16 internally.
            rows.store(size.rows.min(u16::MAX as u32) as u16, Ordering::Relaxed);
            cols.store(size.cols.min(u16::MAX as u32) as u16, Ordering::Relaxed);
        }

        Ok(Response::new(UpdateResponse {}))
    }

    #[tracing::instrument(skip_all, name = "handler:process::send_input")]
    async fn send_input(
        &self,
        request: Request<SendInputRequest>,
    ) -> Result<Response<SendInputResponse>, Status> {
        let req = request.into_inner();
        let pid = self.resolve_selector(req.process).await?;
        let entry = self.entry_for(pid).await?;

        let input = req
            .input
            .and_then(|i| i.input)
            .ok_or_else(|| Status::invalid_argument("input payload is empty"))?;
        let bytes = classify_input(&entry, input)?;

        let stdin_guard = entry.stdin_tx.lock().await;
        let tx = stdin_guard
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("process was started without stdin"))?;
        tx.send(bytes)
            .await
            .map_err(|_| Status::unavailable("input channel closed"))?;

        Ok(Response::new(SendInputResponse {}))
    }

    #[tracing::instrument(skip_all, name = "handler:process::stream_input")]
    async fn stream_input(
        &self,
        request: Request<Streaming<StreamInputRequest>>,
    ) -> Result<Response<StreamInputResponse>, Status> {
        let mut stream = request.into_inner();
        let mut current_pid: Option<u32> = None;

        while let Some(msg) = stream.next().await {
            let msg = msg?;
            let event = msg.event.ok_or_else(|| {
                Status::invalid_argument("stream_input message had no event field")
            })?;
            match event {
                StreamInputEventKind::Start(start) => {
                    current_pid = Some(self.resolve_selector(start.process).await?);
                }
                StreamInputEventKind::Data(data) => {
                    let pid = current_pid.ok_or_else(|| {
                        Status::failed_precondition("stream_input data before start")
                    })?;
                    let entry = self.entry_for(pid).await?;
                    let chunk = data
                        .input
                        .and_then(|i| i.input)
                        .ok_or_else(|| Status::invalid_argument("data input is empty"))?;
                    let bytes = classify_input(&entry, chunk)?;
                    let stdin_guard = entry.stdin_tx.lock().await;
                    let tx = stdin_guard.as_ref().ok_or_else(|| {
                        Status::failed_precondition("process was started without stdin")
                    })?;
                    tx.send(bytes)
                        .await
                        .map_err(|_| Status::unavailable("input channel closed"))?;
                }
                StreamInputEventKind::Keepalive(_) => {
                    // Keepalives just hold the HTTP/2 stream open;
                    // nothing to forward to the child.
                }
            }
        }

        Ok(Response::new(StreamInputResponse {}))
    }

    #[tracing::instrument(skip_all, name = "handler:process::send_signal")]
    async fn send_signal(
        &self,
        request: Request<SendSignalRequest>,
    ) -> Result<Response<SendSignalResponse>, Status> {
        let req = request.into_inner();
        let pid = self.resolve_selector(req.process).await?;
        // Confirm the pid corresponds to a process we own; signalling
        // an arbitrary host pid would be a privilege-escalation
        // dressed as a typo.
        let _entry = self.entry_for(pid).await?;

        let proto_sig = ProtoSignal::try_from(req.signal).unwrap_or(ProtoSignal::Unspecified);
        let nix_sig = match proto_sig {
            ProtoSignal::Sigterm => NixSignal::SIGTERM,
            ProtoSignal::Sigkill => NixSignal::SIGKILL,
            ProtoSignal::Unspecified => {
                return Err(Status::invalid_argument("signal is unspecified"));
            }
        };
        kill(Pid::from_raw(pid as i32), nix_sig)
            .map_err(|e| Status::internal(format!("kill({pid}): {e}")))?;

        Ok(Response::new(SendSignalResponse {}))
    }

    #[tracing::instrument(skip_all, name = "handler:process::close_stdin")]
    async fn close_stdin(
        &self,
        request: Request<CloseStdinRequest>,
    ) -> Result<Response<CloseStdinResponse>, Status> {
        let req = request.into_inner();
        let pid = self.resolve_selector(req.process).await?;
        let entry = self.entry_for(pid).await?;
        let mut guard = entry.stdin_tx.lock().await;
        // Drop the mpsc sender; the forwarder task observes `None`
        // from `rx.recv()` and shuts down the child's stdin pipe.
        *guard = None;
        Ok(Response::new(CloseStdinResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use super::{exit_status_from_code, exit_status_from_signal};
    use nix::sys::signal::Signal;
    use std::os::unix::process::ExitStatusExt;

    /// A normal exit code must survive the raw-`wait`-word reconstruction the
    /// reaper performs before handing the status to a wait task, and must not
    /// be misread as a signal termination.
    #[test]
    fn exit_status_from_code_round_trips() {
        for code in [0, 1, 7, 42, 255] {
            let status = exit_status_from_code(code);
            assert_eq!(status.code(), Some(code), "code {code} should round-trip");
            assert!(
                status.signal().is_none(),
                "code {code} must not look like a signal exit",
            );
        }
    }

    /// A signal termination (no core dump) must reconstruct to the same
    /// signal, with no exit code and the core-dump flag clear.
    #[test]
    fn exit_status_from_signal_round_trips() {
        let status = exit_status_from_signal(Signal::SIGKILL, false);
        assert_eq!(status.signal(), Some(Signal::SIGKILL as i32));
        assert!(status.code().is_none(), "a signal exit has no exit code");
        assert!(!status.core_dumped(), "core-dump flag must be clear");
    }

    /// The core-dump bit (`0x80`) must round-trip independently of the signal.
    #[test]
    fn exit_status_from_signal_flags_core_dump() {
        let status = exit_status_from_signal(Signal::SIGSEGV, true);
        assert_eq!(status.signal(), Some(Signal::SIGSEGV as i32));
        assert!(status.core_dumped(), "core-dump flag must be set");
    }
}
