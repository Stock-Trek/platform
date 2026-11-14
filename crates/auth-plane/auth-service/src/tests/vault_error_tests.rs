use crate::vault_error::{VaultError, VaultWriteOutcome};
use platform_errors::{AuthError, PlatformError, Retryability};
use reqwest::{
    StatusCode,
    header::{HeaderMap, HeaderValue, RETRY_AFTER},
};
use std::time::{Duration, SystemTime};

struct VaultErrorFixture;

impl VaultErrorFixture {
    fn response(status: StatusCode, headers: HeaderMap) -> PlatformError {
        VaultError::response("read", "token-a", status, &headers, "body".to_string())
    }

    fn headers_with_retry_after(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_str(value).unwrap());
        headers
    }

    fn assert_store_retryability(error: &PlatformError, expected: Retryability) {
        assert_eq!(error.retryability(), expected);
        assert!(matches!(
            error,
            PlatformError::Auth(AuthError::Store { .. })
        ));
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

    let PlatformError::Auth(AuthError::Unavailable { retry_after }) = error else {
        panic!("expected an unavailable error");
    };
    assert_eq!(retry_after, Duration::from_secs(1));
}

#[test]
fn retry_after_seconds_are_preserved() {
    let headers = VaultErrorFixture::headers_with_retry_after("7");
    let error = VaultErrorFixture::response(StatusCode::TOO_MANY_REQUESTS, headers);

    let PlatformError::Auth(AuthError::Unavailable { retry_after }) = error else {
        panic!("expected an unavailable error");
    };
    assert_eq!(retry_after, Duration::from_secs(7));
}

#[test]
fn retry_after_http_dates_are_supported() {
    let value = httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(30));
    let headers = VaultErrorFixture::headers_with_retry_after(&value);
    let error = VaultErrorFixture::response(StatusCode::SERVICE_UNAVAILABLE, headers);

    let PlatformError::Auth(AuthError::Unavailable { retry_after }) = error else {
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
fn conditional_write_failure_statuses_are_cas_mismatches() {
    for status in [
        StatusCode::BAD_REQUEST,
        StatusCode::CONFLICT,
        StatusCode::PRECONDITION_FAILED,
    ] {
        assert_eq!(
            VaultWriteOutcome::from_status(status),
            VaultWriteOutcome::CasMismatch
        );
    }
}

#[test]
fn other_statuses_are_not_cas_mismatches() {
    for status in [
        StatusCode::OK,
        StatusCode::NOT_FOUND,
        StatusCode::INTERNAL_SERVER_ERROR,
    ] {
        assert_eq!(
            VaultWriteOutcome::from_status(status),
            VaultWriteOutcome::Other
        );
    }
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
