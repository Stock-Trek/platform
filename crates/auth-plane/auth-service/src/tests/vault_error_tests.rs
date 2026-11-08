use crate::vault_error::VaultError;
use platform_errors::{AuthError, BpError, Retryability};
use reqwest::{
    StatusCode,
    header::{HeaderMap, HeaderValue, RETRY_AFTER},
};
use std::time::{Duration, SystemTime};

struct VaultErrorFixture;

impl VaultErrorFixture {
    fn response(status: StatusCode, headers: HeaderMap) -> BpError {
        VaultError::response("read", "token-a", status, &headers, "body".to_string())
    }

    fn headers_with_retry_after(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_str(value).unwrap());
        headers
    }

    fn assert_store_retryability(error: &BpError, expected: Retryability) {
        assert_eq!(error.retryability(), expected);
        assert!(matches!(error, BpError::Auth(AuthError::Store { .. })));
    }
}

#[test]
fn request_timeout_is_retryable() {
    let error = VaultErrorFixture::response(StatusCode::REQUEST_TIMEOUT, HeaderMap::new());

    VaultErrorFixture::assert_store_retryability(&error, Retryability::Retryable);
}

#[test]
fn precondition_failed_is_permanent() {
    let error = VaultErrorFixture::response(StatusCode::PRECONDITION_FAILED, HeaderMap::new());

    VaultErrorFixture::assert_store_retryability(&error, Retryability::Permanent);
}

#[test]
fn rate_limiting_uses_a_default_retry_after() {
    let error = VaultErrorFixture::response(StatusCode::TOO_MANY_REQUESTS, HeaderMap::new());

    let BpError::Auth(AuthError::Unavailable { retry_after }) = error else {
        panic!("expected an unavailable error");
    };
    assert_eq!(retry_after, Duration::from_secs(1));
}

#[test]
fn retry_after_seconds_are_preserved() {
    let headers = VaultErrorFixture::headers_with_retry_after("7");
    let error = VaultErrorFixture::response(StatusCode::TOO_MANY_REQUESTS, headers);

    let BpError::Auth(AuthError::Unavailable { retry_after }) = error else {
        panic!("expected an unavailable error");
    };
    assert_eq!(retry_after, Duration::from_secs(7));
}

#[test]
fn retry_after_http_dates_are_supported() {
    let value = httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(30));
    let headers = VaultErrorFixture::headers_with_retry_after(&value);
    let error = VaultErrorFixture::response(StatusCode::SERVICE_UNAVAILABLE, headers);

    let BpError::Auth(AuthError::Unavailable { retry_after }) = error else {
        panic!("expected an unavailable error");
    };
    assert!(retry_after > Duration::ZERO);
    assert!(retry_after <= Duration::from_secs(30));
}

#[test]
fn server_errors_without_retry_after_remain_retryable() {
    let error = VaultErrorFixture::response(StatusCode::SERVICE_UNAVAILABLE, HeaderMap::new());

    VaultErrorFixture::assert_store_retryability(&error, Retryability::Retryable);
}

#[test]
fn check_and_set_mismatches_are_detected() {
    let body = r#"{"errors":["check-and-set parameter did not match the current version"]}"#;
    let uppercase = r#"{"errors":["Check-And-Set parameter did not match the current version"]}"#;

    assert!(VaultError::cas_mismatch(StatusCode::BAD_REQUEST, body));
    assert!(VaultError::cas_mismatch(
        StatusCode::PRECONDITION_FAILED,
        body
    ));
    assert!(VaultError::cas_mismatch(StatusCode::CONFLICT, uppercase));
}

#[test]
fn other_bad_requests_are_not_cas_mismatches() {
    assert!(!VaultError::cas_mismatch(
        StatusCode::BAD_REQUEST,
        r#"{"errors":["invalid request"]}"#
    ));
}

#[test]
fn client_errors_are_permanent() {
    let error = VaultErrorFixture::response(StatusCode::BAD_REQUEST, HeaderMap::new());

    VaultErrorFixture::assert_store_retryability(&error, Retryability::Permanent);
}

#[tokio::test]
async fn url_configuration_failures_are_permanent() {
    let transport_error = reqwest::Client::new()
        .get("not a url")
        .send()
        .await
        .unwrap_err();
    let error = VaultError::transport("read", "token-a", transport_error);

    VaultErrorFixture::assert_store_retryability(&error, Retryability::Permanent);
}
