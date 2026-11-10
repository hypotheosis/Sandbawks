use std::collections::BTreeMap;

use kube::CustomResourceExt;
use sandbawks_types::{
    NetworkPolicy, ResourceRequests, SandboxTemplate, SandboxTemplateSpec, Toleration,
};

#[test]
fn crd_metadata() {
    let crd = SandboxTemplate::crd();
    assert_eq!(crd.spec.group, "sandbawks.dev");
    assert_eq!(crd.spec.names.kind, "SandboxTemplate");
    assert_eq!(crd.spec.names.plural, "sandboxtemplates");
}

/// `envdImage` is no longer consumed on cold-start/restore (issue #273),
/// so it must be omittable from the CRD schema. `image` stays required as
/// a control: this guards against the field silently flipping back to
/// required if the serde/schemars attributes are dropped.
#[test]
fn envd_image_is_optional_in_crd_schema() {
    let crd = SandboxTemplate::crd();
    // Navigate to spec.versions[0].schema.openAPIV3Schema.properties.spec.required.
    let schema = crd.spec.versions[0]
        .schema
        .as_ref()
        .and_then(|v| v.open_api_v3_schema.as_ref())
        .expect("CRD carries an OpenAPI v3 schema");
    let spec_required = schema
        .properties
        .as_ref()
        .and_then(|props| props.get("spec"))
        .and_then(|spec| spec.required.as_ref())
        .expect("spec subschema declares a required set");
    assert!(
        !spec_required.iter().any(|f| f == "envdImage"),
        "envdImage must not be required: {spec_required:?}"
    );
    assert!(
        spec_required.iter().any(|f| f == "image"),
        "image must stay required: {spec_required:?}"
    );
}

/// Issue #537: the template egress baseline must stay optional in the CRD
/// schema so existing templates (which carry no `networkPolicy`) remain valid.
/// Mirrors `envd_image_is_optional_in_crd_schema` — guards against the field
/// silently flipping to required if the serde/schemars attributes are dropped.
#[test]
fn network_policy_is_optional_in_crd_schema() {
    let crd = SandboxTemplate::crd();
    let schema = crd.spec.versions[0]
        .schema
        .as_ref()
        .and_then(|v| v.open_api_v3_schema.as_ref())
        .expect("CRD carries an OpenAPI v3 schema");
    let spec_required = schema
        .properties
        .as_ref()
        .and_then(|props| props.get("spec"))
        .and_then(|spec| spec.required.as_ref())
        .expect("spec subschema declares a required set");
    assert!(
        !spec_required.iter().any(|f| f == "networkPolicy"),
        "networkPolicy must not be required: {spec_required:?}"
    );
}

#[test]
fn yaml_roundtrip() {
    let t = SandboxTemplate::new(
        "python-data-sci",
        SandboxTemplateSpec {
            image: "ghcr.io/acme/py-data-sci:1.4".into(),
            default_runtime_class: "gvisor".into(),
            default_resources: ResourceRequests {
                cpu: "1".parse().unwrap(),
                memory: "1Gi".parse().unwrap(),
                ephemeral_storage: Some("5Gi".parse().unwrap()),
            },
            envd_image: None,
            default_timeout_seconds: 3600,
            network_policy: None,
            tolerations: None,
            node_selector: None,
            security_context: None,
            persistence_mode: None,
            volume_claim_templates: None,
            volume_claim_templates_policy: None,
        },
    );
    let yaml = serde_yaml::to_string(&t).unwrap();
    // `None` is skipped on serialize, so the field must not appear at all.
    assert!(
        !yaml.contains("envdImage"),
        "omitted envd_image should not serialize: {yaml}"
    );
    let back: SandboxTemplate = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(t.spec, back.spec);
}

/// Issue #537: the template-level egress baseline round-trips through YAML
/// under the camelCase `networkPolicy.egressAllow` wire shape, and is omitted
/// entirely when `None` (so an egress-free template stays clean).
#[test]
fn network_policy_roundtrips_and_omits_when_none() {
    let mut spec = SandboxTemplateSpec {
        image: "ghcr.io/acme/py:1".into(),
        default_runtime_class: "gvisor".into(),
        default_resources: ResourceRequests {
            cpu: "1".parse().unwrap(),
            memory: "1Gi".parse().unwrap(),
            ephemeral_storage: None,
        },
        envd_image: None,
        default_timeout_seconds: 3600,
        network_policy: None,
        tolerations: None,
        node_selector: None,
        security_context: None,
        persistence_mode: None,
        volume_claim_templates: None,
        volume_claim_templates_policy: None,
    };

    // Absent → not serialized.
    let yaml = serde_yaml::to_string(&SandboxTemplate::new("t", spec.clone())).unwrap();
    assert!(
        !yaml.contains("networkPolicy"),
        "omitted network_policy should not serialize: {yaml}"
    );

    // Present → round-trips under the camelCase wire shape.
    spec.network_policy = Some(NetworkPolicy {
        egress_allow: vec!["git.example.com".into()],
    });
    let t = SandboxTemplate::new("t", spec);
    let yaml = serde_yaml::to_string(&t).unwrap();
    assert!(yaml.contains("networkPolicy"), "{yaml}");
    assert!(yaml.contains("egressAllow"), "{yaml}");
    let back: SandboxTemplate = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(t.spec, back.spec);
}

/// Build a minimal template spec for the scheduling-hint tests, with both
/// `tolerations` and `node_selector` left at their `None` default. Keeps the
/// per-test setup focused on the field under test.
fn base_spec() -> SandboxTemplateSpec {
    SandboxTemplateSpec {
        image: "ghcr.io/acme/py:1".into(),
        default_runtime_class: "gvisor".into(),
        default_resources: ResourceRequests {
            cpu: "1".parse().unwrap(),
            memory: "1Gi".parse().unwrap(),
            ephemeral_storage: None,
        },
        envd_image: None,
        default_timeout_seconds: 3600,
        network_policy: None,
        tolerations: None,
        node_selector: None,
        security_context: None,
        persistence_mode: None,
        volume_claim_templates: None,
        volume_claim_templates_policy: None,
    }
}

/// Issue #592: the scheduling hints must stay optional in the CRD schema so
/// existing templates (which carry neither field) remain valid. Mirrors
/// `network_policy_is_optional_in_crd_schema`.
#[test]
fn scheduling_hints_are_optional_in_crd_schema() {
    let crd = SandboxTemplate::crd();
    let schema = crd.spec.versions[0]
        .schema
        .as_ref()
        .and_then(|v| v.open_api_v3_schema.as_ref())
        .expect("CRD carries an OpenAPI v3 schema");
    let spec_schema = schema
        .properties
        .as_ref()
        .and_then(|props| props.get("spec"))
        .expect("spec subschema present");
    let spec_required = spec_schema
        .required
        .as_ref()
        .expect("spec subschema declares a required set");
    for field in ["tolerations", "nodeSelector"] {
        assert!(
            !spec_required.iter().any(|f| f == field),
            "{field} must not be required: {spec_required:?}"
        );
    }
    // The fields must still *exist* in the schema (guards against the camelCase
    // rename silently dropping them, which would make a templated `nodeSelector`
    // fail CRD validation on apply).
    let props = spec_schema
        .properties
        .as_ref()
        .expect("spec subschema declares properties");
    assert!(props.contains_key("tolerations"), "tolerations in schema");
    assert!(props.contains_key("nodeSelector"), "nodeSelector in schema");
}

/// Issue #592: `tolerations` / `nodeSelector` round-trip through YAML under
/// their camelCase wire shape (including `tolerationSeconds`), and are omitted
/// entirely when `None`.
#[test]
fn scheduling_hints_roundtrip_and_omit_when_none() {
    // Absent → not serialized.
    let yaml = serde_yaml::to_string(&SandboxTemplate::new("t", base_spec())).unwrap();
    assert!(
        !yaml.contains("tolerations") && !yaml.contains("nodeSelector"),
        "omitted scheduling hints should not serialize: {yaml}"
    );

    // Present → round-trips under the camelCase wire shape.
    let mut spec = base_spec();
    spec.tolerations = Some(vec![Toleration {
        effect: Some("NoSchedule".into()),
        key: Some("kata".into()),
        operator: Some("Equal".into()),
        value: Some("true".into()),
        toleration_seconds: Some(300),
    }]);
    let mut node_selector = BTreeMap::new();
    node_selector.insert("agentpool".to_owned(), "kata".to_owned());
    spec.node_selector = Some(node_selector);

    let t = SandboxTemplate::new("t", spec);
    let yaml = serde_yaml::to_string(&t).unwrap();
    assert!(yaml.contains("tolerations"), "{yaml}");
    assert!(yaml.contains("nodeSelector"), "{yaml}");
    // camelCase mirror of the upstream `tolerationSeconds` field.
    assert!(yaml.contains("tolerationSeconds"), "{yaml}");
    let back: SandboxTemplate = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(t.spec, back.spec);
}

/// Epic #603: the persistence fields must stay optional in the CRD schema so
/// existing templates (which carry neither) remain valid, while still existing
/// as properties (guards against the camelCase rename silently dropping them,
/// which would fail CRD validation on apply). Mirrors
/// `scheduling_hints_are_optional_in_crd_schema`.
#[test]
fn persistence_fields_are_optional_in_crd_schema() {
    let crd = SandboxTemplate::crd();
    let schema = crd.spec.versions[0]
        .schema
        .as_ref()
        .and_then(|v| v.open_api_v3_schema.as_ref())
        .expect("CRD carries an OpenAPI v3 schema");
    let spec_schema = schema
        .properties
        .as_ref()
        .and_then(|props| props.get("spec"))
        .expect("spec subschema present");
    let spec_required = spec_schema
        .required
        .as_ref()
        .expect("spec subschema declares a required set");
    for field in ["persistenceMode", "volumeClaimTemplates"] {
        assert!(
            !spec_required.iter().any(|f| f == field),
            "{field} must not be required: {spec_required:?}"
        );
    }
    let props = spec_schema
        .properties
        .as_ref()
        .expect("spec subschema declares properties");
    assert!(
        props.contains_key("persistenceMode"),
        "persistenceMode in schema"
    );
    assert!(
        props.contains_key("volumeClaimTemplates"),
        "volumeClaimTemplates in schema"
    );
}
