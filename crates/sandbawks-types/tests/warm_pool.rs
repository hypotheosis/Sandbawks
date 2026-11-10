use kube::CustomResourceExt;
use sandbawks_types::{
    SandboxWarmPool, SandboxWarmPoolSpec, CLAIMED_AT_ANNOTATION, TEMPLATE_RV_ANNOTATION,
    WARM_POOL_LABEL,
};

#[test]
fn warm_pool_label_and_annotation_consts_are_stable() {
    // These strings are an external contract (kube labels/annotations
    // survive across controller/api versions); pin them so a rename is a
    // conscious break rather than a silent one that orphans a live pool.
    assert_eq!(WARM_POOL_LABEL, "sandbawks.dev/warm-pool");
    assert_eq!(CLAIMED_AT_ANNOTATION, "sandbawks.dev/claimed-at");
    // Issue #397: the warm member's recorded template `resourceVersion`. A
    // rename would make every existing member read as version-stale on the
    // next reconcile, churning the whole pool — pin it deliberately.
    assert_eq!(
        TEMPLATE_RV_ANNOTATION,
        "sandbawks.dev/warm-pool-template-rv"
    );
}

#[test]
fn crd_metadata() {
    let crd = SandboxWarmPool::crd();
    assert_eq!(crd.spec.names.kind, "SandboxWarmPool");
    assert_eq!(crd.spec.names.plural, "sandboxwarmpools");
}

#[test]
fn yaml_roundtrip() {
    let p = SandboxWarmPool::new(
        "python-pool",
        SandboxWarmPoolSpec {
            template_ref: "python-data-sci".into(),
            min_ready: 5,
            max_ready: 20,
        },
    );
    let yaml = serde_yaml::to_string(&p).unwrap();
    let back: SandboxWarmPool = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(p.spec, back.spec);
}
