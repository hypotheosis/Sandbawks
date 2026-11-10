use sandbawks_types::{Error, ResourceQuantity, ResourceRequests};

#[test]
fn parse_plain_integer() {
    let q: ResourceQuantity = "2".parse().unwrap();
    assert_eq!(q.as_str(), "2");
}

#[test]
fn parse_memory_gibibytes() {
    let q: ResourceQuantity = "1Gi".parse().unwrap();
    assert_eq!(q.as_str(), "1Gi");
}

#[test]
fn parse_millicores() {
    let q: ResourceQuantity = "500m".parse().unwrap();
    assert_eq!(q.as_str(), "500m");
}

#[test]
fn parse_rejects_empty() {
    let err = "".parse::<ResourceQuantity>().unwrap_err();
    assert!(matches!(err, Error::InvalidQuantity(_)));
}

#[test]
fn parse_rejects_garbage() {
    let err = "not a quantity".parse::<ResourceQuantity>().unwrap_err();
    assert!(matches!(err, Error::InvalidQuantity(_)));
}

#[test]
fn resource_requests_yaml_roundtrip() {
    let r = ResourceRequests {
        cpu: "2".parse().unwrap(),
        memory: "2Gi".parse().unwrap(),
        ephemeral_storage: Some("10Gi".parse().unwrap()),
    };
    let yaml = serde_yaml::to_string(&r).unwrap();
    let back: ResourceRequests = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(r, back);
    // Spec §4.1 uses ephemeralStorage in camelCase
    assert!(yaml.contains("ephemeralStorage"), "yaml was: {yaml}");
}
