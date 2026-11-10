//! Task-span emission tests for the node-side `SnapshotJob` reconciler (issue #375).
//!
//! These live in their **own** integration-test binary — separate from the
//! behavioural tests in `reconcile.rs` — on purpose (issue #391).
//!
//! `tracing`'s callsite-interest cache is process-global, and these tests
//! install their recording subscriber with the *thread-local*
//! [`tracing::subscriber::with_default`]. If a sibling `#[tokio::test]` running
//! `reconcile(...)` under the global `NoSubscriber` registers the
//! `reconcile:node::snapshot_job` callsite first, its interest caches as
//! `Interest::never()` and the `#[tracing::instrument]` macro short-circuits —
//! so the recorder never observes the span and the assertion flakes with
//! `opened: []`. A thread-local subscriber cannot invalidate that global cache.
//!
//! Isolating these three tests in a dedicated binary means the only
//! `reconcile(...)` calls in this process run under the recording subscriber,
//! so the callsite is registered correctly the first (and only) time it is hit.
//!
//! The `reconcile:node::snapshot_job` span renders as a "Task" in the APM. The
//! idempotency floors (terminal / in-flight-and-fresh) do no work and open no
//! child spans, so they must NOT create that span — otherwise every lingering
//! terminal/in-flight job re-reconciling on each 30s requeue floods the Task
//! list with childless Tasks. These tests pin: a skip emits no Task span, a real
//! dispatch emits exactly the one Task span.

use std::sync::{Arc, Mutex};

use sandbawks_node::reconcile::{reconcile, ReconcileContext};
use sandbawks_types::{SnapshotJob, SnapshotJobMode, SnapshotJobPhase, SnapshotJobStatus};

mod common;
use common::*;

/// Minimal [`tracing::Subscriber`] that records the *name* of every span
/// created, so a test can assert whether a given span was (or wasn't) opened.
///
/// Only `new_span` is meaningful — every other hook is a no-op. `new_span`
/// must return a non-zero [`tracing::span::Id`], so a monotonic counter (+1)
/// supplies one. Cloneable (`Arc`-backed) so the test keeps a handle to read
/// the captured names after `with_default` hands ownership to the subscriber.
#[derive(Clone, Default)]
struct SpanNameRecorder {
    names: Arc<Mutex<Vec<String>>>,
    next_id: Arc<std::sync::atomic::AtomicU64>,
}

impl SpanNameRecorder {
    fn names(&self) -> Vec<String> {
        self.names.lock().expect("poisoned").clone()
    }

    fn opened(&self, name: &str) -> bool {
        self.names().iter().any(|n| n == name)
    }
}

impl tracing::Subscriber for SpanNameRecorder {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        self.names
            .lock()
            .expect("poisoned")
            .push(span.metadata().name().to_owned());
        // `Id::from_u64` panics on 0, so start the counter at 1.
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        tracing::span::Id::from_u64(id)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

/// Drive `reconcile(&job, &ctx)` to completion under `recorder` as the active
/// subscriber, returning the recorder so the caller can inspect span names.
///
/// A `current_thread` runtime keeps the whole future on this thread, so the
/// thread-local default subscriber stays in effect across every `.await`.
fn reconcile_under_recorder(job: &SnapshotJob, ctx: &Arc<ReconcileContext>) -> SpanNameRecorder {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread runtime");
    let recorder = SpanNameRecorder::default();
    let handle = recorder.clone();
    tracing::subscriber::with_default(recorder, || {
        rt.block_on(async { reconcile(job, ctx).await })
            .expect("reconcile under recorder");
    });
    handle
}

#[test]
fn terminal_reconcile_emits_no_task_span() {
    // A job already `Complete` hits the terminal idempotency floor and returns
    // without dispatching — so it must open no `reconcile:node::snapshot_job`
    // Task span at all (#375).
    let adapter = Arc::new(FakeRuntimeAdapter::new());
    let (ctx, status_store) = make_ctx(adapter.clone());
    let job = make_job(
        "sbx-terminal-nospan",
        SnapshotJobMode::Pause,
        None,
        Some(SnapshotJobStatus {
            phase: SnapshotJobPhase::Complete,
            manifest: Some(default_manifest(SANDBOX_NAME)),
            message: None,
            started_at: Some("2026-05-25T12:00:00+00:00".into()),
            completed_at: Some("2026-05-25T12:01:00+00:00".into()),
            local_path: None,
        }),
    );

    let recorder = reconcile_under_recorder(&job, &ctx);

    assert!(
        !recorder.opened("reconcile:node::snapshot_job"),
        "a terminal SnapshotJob must not open the reconcile:node::snapshot_job Task \
         span (would flood the APM with childless Tasks); opened: {:?}",
        recorder.names()
    );
    // And it did no work, mirroring `terminal_phase_is_idempotent`.
    assert!(adapter.pause_calls().is_empty());
    assert!(status_store.recorded().is_empty());
}

#[test]
fn in_flight_recent_reconcile_emits_no_task_span() {
    // In-flight + fresh `started_at` hits the #194 in-flight floor and skips —
    // likewise no Task span (#375).
    let recent = chrono::Utc::now().to_rfc3339();
    let adapter = Arc::new(FakeRuntimeAdapter::new());
    let (ctx, status_store) = make_ctx(adapter.clone());
    let job = make_job(
        "sbx-inflight-nospan",
        SnapshotJobMode::Pause,
        None,
        Some(SnapshotJobStatus {
            phase: SnapshotJobPhase::Checkpointing,
            manifest: None,
            message: None,
            started_at: Some(recent),
            completed_at: None,
            local_path: None,
        }),
    );

    let recorder = reconcile_under_recorder(&job, &ctx);

    assert!(
        !recorder.opened("reconcile:node::snapshot_job"),
        "an in-flight (fresh) SnapshotJob must not open the Task span; opened: {:?}",
        recorder.names()
    );
    assert!(adapter.pause_calls().is_empty());
    assert!(status_store.recorded().is_empty());
}

#[test]
fn dispatching_reconcile_emits_the_task_span() {
    // A fresh `Pending` job dispatches real pause work, so it *must* open the
    // one `reconcile:node::snapshot_job` Task span — the non-empty Tasks the
    // operator actually wants to see.
    let adapter = Arc::new(FakeRuntimeAdapter::new());
    let (ctx, _status_store) = make_ctx(adapter.clone());
    let job = make_job("sbx-fresh-span", SnapshotJobMode::Pause, None, None);

    let recorder = reconcile_under_recorder(&job, &ctx);

    assert!(
        recorder.opened("reconcile:node::snapshot_job"),
        "a dispatching SnapshotJob must open the Task span; opened: {:?}",
        recorder.names()
    );
    // Sanity: the dispatch actually ran.
    assert_eq!(adapter.pause_calls(), vec![CONTAINER_ID.to_owned()]);
}
