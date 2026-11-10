use kube::CustomResourceExt;
use sandbawks_types::{
    merge_egress_policies, NetworkPolicy, PersistenceMode, ResourceRequests, Sandbox, SandboxPhase,
    SandboxSpec, SandboxStatus, VolumeClaimSpec, VolumeClaimTemplate, VolumeClaimTemplateMetadata,
};

/// Convenience: an egress policy from a list of `&str` destinations.
fn np(dests: &[&str]) -> NetworkPolicy {
    NetworkPolicy {
        egress_allow: dests.iter().map(|d| (*d).to_owned()).collect(),
    }
}

#[test]
fn crd_metadata_matches_spec() {
    let crd = Sandbox::crd();
    assert_eq!(crd.spec.group, "sandbawks.dev");
    assert_eq!(crd.spec.names.kind, "Sandbox");
    assert_eq!(crd.spec.names.plural, "sandboxes");
    assert!(crd
        .spec
        .names
        .short_names
        .as_ref()
        .unwrap()
        .contains(&"sbx".to_owned()));
}

#[test]
fn yaml_roundtrip_sandbox() {
    let s = Sandbox::new(
        "sbx-abc123",
        SandboxSpec {
            template_ref: "python-data-sci".into(),
            runtime_class_name: Some("gvisor".into()),
            resources: ResourceRequests {
                cpu: "2".parse().unwrap(),
                memory: "2Gi".parse().unwrap(),
                ephemeral_storage: Some("10Gi".parse().unwrap()),
            },
            timeout_seconds: 3600,
            auto_pause: true,
            desired_phase: None,
            env: Default::default(),
            metadata: Default::default(),
            network_policy: None,
            snapshot_retention: None,
            pvc_retention: None,
            tolerations: None,
            node_selector: None,
            security_context: None,
            persistence_mode: None,
            volume_claim_templates: None,
            restore_from: None,
        },
    );
    let yaml = serde_yaml::to_string(&s).unwrap();
    let back: Sandbox = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(s.spec, back.spec);
}

#[test]
fn phase_serializes_pascal_case() {
    // The state machine in spec §5 uses PascalCase phase names.
    let p = SandboxPhase::Paused;
    assert_eq!(serde_json::to_string(&p).unwrap(), "\"Paused\"");
    let parsed: SandboxPhase = serde_json::from_str("\"Resuming\"").unwrap();
    assert_eq!(parsed, SandboxPhase::Resuming);
}

// ---------- issue #537: egress policy merge ----------

#[test]
fn merge_egress_both_none_is_none() {
    assert!(merge_egress_policies(None, None).is_none());
}

#[test]
fn merge_egress_single_side_passes_through() {
    let base = np(&["git.example.com"]);
    assert_eq!(
        merge_egress_policies(Some(base.clone()), None).unwrap(),
        base
    );
    assert_eq!(
        merge_egress_policies(None, Some(base.clone())).unwrap(),
        base
    );
}

#[test]
fn merge_egress_empty_allow_lists_normalize_to_none() {
    // An empty `egressAllow` is the same default-deny as `None`; the merge must
    // not materialize an empty `networkPolicy` that round-trips inconsistently.
    let empty = np(&[]);
    assert!(merge_egress_policies(Some(empty.clone()), None).is_none());
    assert!(merge_egress_policies(Some(empty.clone()), Some(np(&[]))).is_none());
}

#[test]
fn merge_egress_unions_baseline_first_and_dedups() {
    // Template baseline (option 1) ∪ per-create override (option 2): baseline
    // destinations lead, the override appends only what's new, exact-string
    // duplicates collapse.
    let template = np(&["git.example.com", "registry.example.com"]);
    let per_create = np(&["registry.example.com", "10.0.0.0/8"]);
    let merged = merge_egress_policies(Some(template), Some(per_create)).unwrap();
    assert_eq!(
        merged.egress_allow,
        vec!["git.example.com", "registry.example.com", "10.0.0.0/8"]
    );
}

#[test]
fn merge_egress_override_cannot_revoke_baseline() {
    // Union semantics: an empty override leaves the baseline fully intact.
    let template = np(&["git.example.com"]);
    let merged = merge_egress_policies(Some(template.clone()), Some(np(&[]))).unwrap();
    assert_eq!(merged, template);
}

#[test]
fn status_subresource_default() {
    // Make sure SandboxStatus has a sensible default (used when the
    // controller first observes a freshly-created Sandbox).
    let st = SandboxStatus::default();
    assert_eq!(st.phase, SandboxPhase::Creating);
    assert!(st.assigned_node.is_none());
    assert!(st.snapshot_ref.is_none());
}

// ---------- epic #603: persistence mode + volume claim templates ----------

/// A `SandboxSpec` carrying every required field at a valid placeholder, with
/// the epic-#603 fields left at their `None` default. Keeps each test focused on
/// the field under exercise.
fn base_sandbox_spec() -> SandboxSpec {
    SandboxSpec {
        template_ref: "tpl".into(),
        runtime_class_name: None,
        resources: ResourceRequests {
            cpu: "1".parse().unwrap(),
            memory: "1Gi".parse().unwrap(),
            ephemeral_storage: None,
        },
        timeout_seconds: 3600,
        auto_pause: false,
        desired_phase: None,
        env: Default::default(),
        metadata: Default::default(),
        network_policy: None,
        snapshot_retention: None,
        pvc_retention: None,
        tolerations: None,
        node_selector: None,
        security_context: None,
        persistence_mode: None,
        volume_claim_templates: None,
        restore_from: None,
    }
}

/// The effective default persistence mode is `Snapshot` — selecting it (or
/// leaving the enum unset and reading [`PersistenceMode::default`]) reproduces
/// the pre-#603 behavior, so existing CRs are unaffected.
#[test]
fn persistence_mode_defaults_to_snapshot() {
    assert_eq!(PersistenceMode::default(), PersistenceMode::Snapshot);
}

/// `PersistenceMode` serializes as PascalCase to match the state-machine /
/// phase naming convention on the wire.
#[test]
fn persistence_mode_serializes_pascal_case() {
    assert_eq!(
        serde_json::to_string(&PersistenceMode::Snapshot).unwrap(),
        "\"Snapshot\""
    );
    assert_eq!(
        serde_json::to_string(&PersistenceMode::Filesystem).unwrap(),
        "\"Filesystem\""
    );
    let parsed: PersistenceMode = serde_json::from_str("\"Filesystem\"").unwrap();
    assert_eq!(parsed, PersistenceMode::Filesystem);
}

/// The new optional fields are omitted from serialized YAML when `None`, so an
/// existing snapshot-mode sandbox stays clean (no `persistenceMode: null` /
/// `volumeClaimTemplates: null`).
#[test]
fn persistence_fields_omitted_when_none() {
    let yaml = serde_yaml::to_string(&Sandbox::new("sbx-1", base_sandbox_spec())).unwrap();
    assert!(
        !yaml.contains("persistenceMode"),
        "omitted persistence_mode should not serialize: {yaml}"
    );
    assert!(
        !yaml.contains("volumeClaimTemplates"),
        "omitted volume_claim_templates should not serialize: {yaml}"
    );
}

/// The new fields round-trip through YAML under their camelCase wire shape,
/// including the nested `volumeClaimTemplates[].spec.storage` /
/// `accessModes` / `storageClassName`.
#[test]
fn persistence_fields_roundtrip_camel_case() {
    let mut spec = base_sandbox_spec();
    spec.persistence_mode = Some(PersistenceMode::Filesystem);
    spec.volume_claim_templates = Some(vec![VolumeClaimTemplate {
        metadata: VolumeClaimTemplateMetadata {
            name: "workspace".into(),
            ..Default::default()
        },
        spec: VolumeClaimSpec {
            storage: "10Gi".parse().unwrap(),
            access_modes: vec!["ReadWriteOnce".into()],
            storage_class_name: Some("managed-csi".into()),
            volume_mode: None,
        },
    }]);

    let s = Sandbox::new("sbx-1", spec);
    let yaml = serde_yaml::to_string(&s).unwrap();
    assert!(yaml.contains("persistenceMode"), "{yaml}");
    assert!(yaml.contains("Filesystem"), "{yaml}");
    assert!(yaml.contains("volumeClaimTemplates"), "{yaml}");
    assert!(yaml.contains("accessModes"), "{yaml}");
    assert!(yaml.contains("storageClassName"), "{yaml}");
    // `volume_mode` is `None`, so it must not appear.
    assert!(!yaml.contains("volumeMode"), "{yaml}");

    let back: Sandbox = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(s.spec, back.spec);
}
