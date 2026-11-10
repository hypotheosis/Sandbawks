use kube::CustomResourceExt;
use sandbawks_types::{TeamAPIKey, TeamApiKeySpec};

#[test]
fn crd_metadata() {
    let crd = TeamAPIKey::crd();
    assert_eq!(crd.spec.group, "sandbawks.dev");
    assert_eq!(crd.spec.names.kind, "TeamAPIKey");
    assert_eq!(crd.spec.names.plural, "teamapikeys");
}

#[test]
fn yaml_roundtrip() {
    let k = TeamAPIKey::new(
        "ci-bot",
        TeamApiKeySpec {
            key_prefix: "sbk_a1b2".into(),
            key_hash: "sha256:abcdef0123".into(),
            namespace: "team-acme".into(),
            scopes: vec!["sandboxes:*".into()],
        },
    );
    let yaml = serde_yaml::to_string(&k).unwrap();
    let back: TeamAPIKey = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(k.spec, back.spec);
    assert_eq!(back.spec.namespace, "team-acme");
}
