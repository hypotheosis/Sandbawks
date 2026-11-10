use std::sync::Arc;

use async_trait::async_trait;
use sandbawks_types::runtime::{
    noop_sink, noop_source, RuntimeAdapter, RuntimeError, SnapshotManifest,
};
use sandbawks_types::snapshot::{SnapshotSink, SnapshotSource};

struct NullAdapter;

#[async_trait]
impl RuntimeAdapter for NullAdapter {
    async fn pause(
        &self,
        _pod_uid: &str,
        _sink: &dyn SnapshotSink,
    ) -> Result<SnapshotManifest, RuntimeError> {
        Ok(SnapshotManifest {
            envd_sha256: String::new(),
            runsc_version: String::new(),
            envd_captured: false,
            sandbox_id: "test".into(),
            runtime_class: "null".into(),
            sha256: "0".repeat(64),
            created_at: "1970-01-01T00:00:00Z".into(),
            parts: Vec::new(),
            snapshot_ref: String::new(),
            template_ref: String::new(),
            template_image: String::new(),
        })
    }

    async fn resume(
        &self,
        _manifest: &SnapshotManifest,
        _pod_uid: &str,
        _source: &dyn SnapshotSource,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn runtime_class(&self) -> &str {
        "null"
    }
}

#[tokio::test(flavor = "current_thread")]
async fn trait_is_dyn_object_safe() {
    let adapter: Arc<dyn RuntimeAdapter> = Arc::new(NullAdapter);
    assert_eq!(adapter.runtime_class(), "null");
    let manifest = adapter.pause("pod-1", noop_sink()).await.unwrap();
    assert_eq!(manifest.sandbox_id, "test");
    adapter
        .resume(&manifest, "pod-1", noop_source())
        .await
        .unwrap();
}

/// `NullAdapter` overrides `pause`/`resume` but not `stage_snapshot`,
/// so it inherits the default impl, which must reject the call with
/// [`RuntimeError::Unsupported`] naming the method — the contract Plan
/// 07's real adapter (and the node worker's fail-closed arm) rely on.
#[tokio::test(flavor = "current_thread")]
async fn stage_snapshot_default_is_unsupported() {
    let adapter: Arc<dyn RuntimeAdapter> = Arc::new(NullAdapter);
    let manifest = adapter.pause("pod-1", noop_sink()).await.unwrap();
    let err = adapter
        .stage_snapshot("sbx-1", &manifest, noop_source(), None)
        .await
        .expect_err("default stage_snapshot must be unsupported");
    assert!(matches!(err, RuntimeError::Unsupported(detail) if detail == "stage_snapshot"));
}

#[test]
fn snapshot_manifest_serde_roundtrip() {
    let m = SnapshotManifest {
        // A recorded envd digest (issue #270) must survive the round-trip.
        envd_sha256: format!("{:0>64}", "feed"),
        // A recorded runsc version (#545) must survive the round-trip too.
        runsc_version: "release-20260525.0".into(),
        envd_captured: false,
        sandbox_id: "sbx-abc123".into(),
        runtime_class: "gvisor".into(),
        sha256: format!("{:0>64}", "deadbeef"),
        created_at: "2026-05-22T18:00:00Z".into(),
        parts: Vec::new(),
        snapshot_ref: "s3://bucket/sbx-abc123/manifest.json".into(),
        template_ref: String::new(),
        template_image: String::new(),
    };
    let json = serde_json::to_string(&m).unwrap();
    // The wire key is camelCase (`#[serde(rename_all = "camelCase")]`).
    assert!(
        json.contains("\"envdSha256\""),
        "wire key must be camelCase: {json}"
    );
    let back: SnapshotManifest = serde_json::from_str(&json).unwrap();
    assert_eq!(m, back);
}

/// Deserializing a v1 JSON document (no `parts`, no `snapshotRef`)
/// must succeed with the v2 defaults filled in, so the upgrade is
/// non-breaking for snapshots already recorded in `Sandbox.status`.
#[test]
fn snapshot_manifest_v1_compat_deserialize() {
    let v1 = r#"{
        "sandboxId": "sbx-old",
        "runtimeClass": "gvisor",
        "sha256": "deadbeef",
        "createdAt": "2026-01-01T00:00:00Z"
    }"#;
    let m: SnapshotManifest = serde_json::from_str(v1).unwrap();
    assert_eq!(m.sandbox_id, "sbx-old");
    assert!(m.parts.is_empty());
    assert_eq!(m.snapshot_ref, "");
    // A pre-#270 manifest carries no envd digest; it must default to
    // empty so the restore path falls back to per-part verification
    // rather than failing to deserialize.
    assert_eq!(m.envd_sha256, "");
    // A pre-#466 manifest predates capture-in-checkpoint; it must default
    // to `false` so the restore path keeps the fail-closed host-binary
    // comparison for legacy (non-relocated) snapshots.
    assert!(!m.envd_captured);
}

/// A capture-mode manifest (epic #466) must round-trip its `envdCaptured`
/// flag as a camelCase boolean — the restore path keys its host-binary-skip
/// decision off it, so it cannot be silently dropped on the wire.
#[test]
fn snapshot_manifest_envd_captured_round_trips() {
    let manifest = SnapshotManifest {
        sandbox_id: "sbx-captured".into(),
        runtime_class: "gvisor".into(),
        sha256: "deadbeef".into(),
        created_at: "2026-06-17T00:00:00Z".into(),
        envd_captured: true,
        ..Default::default()
    };
    let json = serde_json::to_string(&manifest).unwrap();
    assert!(json.contains("\"envdCaptured\":true"), "got: {json}");

    let parsed: SnapshotManifest = serde_json::from_str(&json).unwrap();
    assert!(parsed.envd_captured);
    assert_eq!(parsed, manifest);
}
