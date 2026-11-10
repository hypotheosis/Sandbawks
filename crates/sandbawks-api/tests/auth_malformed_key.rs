//! Regression test for #597: one malformed `TeamAPIKey` must not take
//! down auth for the whole fleet.
//!
//! Before the fix, `KubeKeyStore::resolve` listed every `TeamAPIKey` and
//! let kube deserialize the list as one unit. kube's typed list deser is
//! all-or-nothing, so a single un-parseable item (e.g. a stale key created
//! before `spec.namespace` became required — #583/#590) failed the whole
//! list. Every authenticated request then 502'd with a misleading
//! `upstream_unavailable`, even for valid keys.
//!
//! These tests drive a mock apiserver that returns a `TeamAPIKeyList`
//! containing a mix of valid and malformed items and assert:
//!
//! - a valid key still resolves (the malformed sibling is skipped);
//! - presenting a key whose CR fails to deserialize yields a clean `401`,
//!   never a `502`; and
//! - presenting a key whose CR *deserializes* but carries an empty
//!   `spec.namespace` yields `401 api_key_misconfigured` (the branch this
//!   PR changed from a 500 — #597).

use std::sync::Arc;

use kube::Client;
use sandbawks_api::auth::{KeyStore, KubeKeyStore};
use sandbawks_api::error::AppError;
use sandbawks_types::auth::{hash_key, KeyPrefix};

/// Raw key for the *valid* CR. Its first 8 chars are the `keyPrefix`.
const VALID_KEY: &str = "sbk_good1aaaaaaaaaaaaaaaa";
/// Raw key for the *malformed* CR (the one missing `spec.namespace`).
const BAD_KEY: &str = "sbk_bad2bbbbbbbbbbbbbbbbb";
/// Raw key for a CR that deserializes fine but has an empty
/// `spec.namespace` — exercises the `api_key_misconfigured` branch.
const EMPTY_NS_KEY: &str = "sbk_emp3cccccccccccccccc";

/// Build a `TeamAPIKeyList` JSON body mixing a valid item, an item missing
/// the now-required `spec.namespace` field (un-deserializable → skipped),
/// and an item whose `spec.namespace` is the empty string (deserializes but
/// is misconfigured).
fn list_body() -> Vec<u8> {
    let valid_hash = hash_key(VALID_KEY).0;
    let bad_hash = hash_key(BAD_KEY).0;
    let empty_ns_hash = hash_key(EMPTY_NS_KEY).0;
    let body = serde_json::json!({
        "apiVersion": "sandbawks.dev/v1alpha1",
        "kind": "TeamAPIKeyList",
        "metadata": { "resourceVersion": "1" },
        "items": [
            // Malformed: `spec.namespace` is absent. Deserializing this
            // single item into `TeamAPIKey` fails; it must be skipped.
            {
                "apiVersion": "sandbawks.dev/v1alpha1",
                "kind": "TeamAPIKey",
                "metadata": { "name": "ghlw-key", "namespace": "sandbawks" },
                "spec": {
                    "keyPrefix": KeyPrefix::from_key(BAD_KEY).as_str(),
                    "keyHash": bad_hash,
                    // no `namespace`
                    "scopes": ["sandboxes:*"]
                }
            },
            // Deserializes fine but `spec.namespace` is the empty string:
            // resolving this key hits the `api_key_misconfigured` branch.
            {
                "apiVersion": "sandbawks.dev/v1alpha1",
                "kind": "TeamAPIKey",
                "metadata": { "name": "empty-ns-key", "namespace": "sandbawks" },
                "spec": {
                    "keyPrefix": KeyPrefix::from_key(EMPTY_NS_KEY).as_str(),
                    "keyHash": empty_ns_hash,
                    "namespace": "",
                    "scopes": ["sandboxes:*"]
                }
            },
            // Valid: fully-formed, must still authenticate.
            {
                "apiVersion": "sandbawks.dev/v1alpha1",
                "kind": "TeamAPIKey",
                "metadata": { "name": "acme-key", "namespace": "sandbawks" },
                "spec": {
                    "keyPrefix": KeyPrefix::from_key(VALID_KEY).as_str(),
                    "keyHash": valid_hash,
                    "namespace": "team-acme",
                    "scopes": ["sandboxes:*"]
                }
            }
        ]
    });
    serde_json::to_vec(&body).expect("list body serializes")
}

/// A `kube::Client` whose every request returns the canned list body.
fn mock_client() -> Client {
    let service = tower::service_fn(
        move |_req: axum::http::Request<kube::client::Body>| async move {
            Ok::<_, std::convert::Infallible>(
                axum::http::Response::builder()
                    .status(200)
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(kube::client::Body::from(list_body()))
                    .expect("response is well-formed"),
            )
        },
    );
    Client::new(service, "sandbawks")
}

#[tokio::test(flavor = "current_thread")]
async fn valid_key_resolves_despite_malformed_sibling() {
    let store = Arc::new(KubeKeyStore::new(mock_client(), "sandbawks"));
    let prefix = KeyPrefix::from_key(VALID_KEY);

    let ctx = store
        .resolve(&prefix, VALID_KEY)
        .await
        .expect("valid key must resolve even though a malformed key is in the list");

    assert_eq!(ctx.namespace, "team-acme");
    assert_eq!(ctx.key_name, "acme-key");
    assert!(ctx.allows("sandboxes:create"));
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_key_is_unauthorized_not_upstream() {
    let store = Arc::new(KubeKeyStore::new(mock_client(), "sandbawks"));
    let prefix = KeyPrefix::from_key(BAD_KEY);

    let err = store
        .resolve(&prefix, BAD_KEY)
        .await
        .expect_err("presenting the malformed key must fail");

    // The malformed item is skipped, so the presented key simply isn't
    // found: a clean 401, never a 502 `upstream_unavailable` (#597).
    match err {
        AppError::Unauthorized { .. } => {}
        other => panic!("expected 401 Unauthorized for a malformed key, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn empty_namespace_key_is_misconfigured_not_internal() {
    let store = Arc::new(KubeKeyStore::new(mock_client(), "sandbawks"));
    let prefix = KeyPrefix::from_key(EMPTY_NS_KEY);

    let err = store
        .resolve(&prefix, EMPTY_NS_KEY)
        .await
        .expect_err("a key with an empty spec.namespace must fail");

    // This CR deserializes (so it is *not* skipped) but carries an empty
    // namespace. That is a caller-side misconfiguration: it must surface as
    // `401 api_key_misconfigured`, not the old `AppError::Internal` (500)
    // and never a 502 (#597).
    match err {
        AppError::Unauthorized { code, .. } => {
            assert_eq!(
                code, "api_key_misconfigured",
                "empty-namespace key must map to api_key_misconfigured"
            );
        }
        other => panic!("expected 401 api_key_misconfigured, got {other:?}"),
    }
}
