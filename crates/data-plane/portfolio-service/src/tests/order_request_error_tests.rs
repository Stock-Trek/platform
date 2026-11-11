use crate::OrderRequestError;
use http_body_util::BodyExt;
use platform_errors::BpError;
use serde_json::json;

#[test]
fn maps_signing_failures_to_public_statuses() {
    let cases = [
        (BpError::unknown_token("ghost"), OrderRequestError::NotFound),
        (
            BpError::auth_invalid_field("owner", "has no active API token"),
            OrderRequestError::NotFound,
        ),
        (
            BpError::auth_invalid_field("account", "is not owned by the owner"),
            OrderRequestError::Forbidden,
        ),
        (
            BpError::auth_missing_scope("token-a", "orders:write"),
            OrderRequestError::Forbidden,
        ),
        (
            BpError::auth_invalid_field("issued_at", "must not be in the future"),
            OrderRequestError::InvalidRequest,
        ),
        (
            BpError::auth_store("read", "vault", true, "vault unavailable"),
            OrderRequestError::ServiceUnavailable,
        ),
        (
            BpError::auth_store("read", "vault", false, "invalid response"),
            OrderRequestError::Internal,
        ),
    ];

    for (error, expected) in cases {
        assert_eq!(OrderRequestError::from_signing_error(&error), expected);
    }
}

#[test]
fn maps_authentication_failures_to_public_statuses() {
    assert_eq!(
        OrderRequestError::from_authentication_error(&BpError::auth_invalid_jwt("expired")),
        OrderRequestError::Unauthorized
    );
    assert_eq!(
        OrderRequestError::from_authentication_error(&BpError::auth_store(
            "read",
            "jwks",
            true,
            "unavailable"
        )),
        OrderRequestError::ServiceUnavailable
    );
    assert_eq!(
        OrderRequestError::from_authentication_error(&BpError::System(
            platform_errors::SystemError::SemaphoreClosed
        )),
        OrderRequestError::Internal
    );
}

#[tokio::test]
async fn public_bodies_omit_internal_error_details() {
    let error = BpError::auth_store(
        "read",
        "vault-internal-target",
        true,
        "connection refused by 10.0.0.1",
    );
    let public = OrderRequestError::from_signing_error(&error);
    let body = public
        .response()
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(
        value,
        json!({
            "error": "service_unavailable",
            "message": "the service is temporarily unavailable"
        })
    );
    let body = String::from_utf8_lossy(&body);
    assert!(!body.contains("vault-internal-target"));
    assert!(!body.contains("connection refused"));
    assert!(!body.contains("10.0.0.1"));
}
