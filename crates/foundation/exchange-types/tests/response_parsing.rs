use platform_exchange_types::{
    binance::{
        response::{BinanceResponse, BinanceResponsePayload},
        time::BinanceTimeResponse,
    },
    http::HttpResponse,
    rate_limited::{RateLimit, RateLimitRestriction},
    response::{ETHttpResponse, ETResponse, ETWebsocketResponse},
    websocket_id::ETWebsocketId,
};
use platform_types::new_types::{Nanoseconds, Seconds, UsageCount};
use serde_json::json;

#[test]
fn http_response_ignores_malformed_rate_limit_headers() {
    let response = HttpResponse {
        status: 200,
        headers: vec![
            ("x-mbx-order-count-".into(), "1".into()),
            ("x-mbx-used-weight-".into(), "2".into()),
            ("x-mbx-order-count-10".into(), "3".into()),
            ("x-mbx-used-weight-xs".into(), "4".into()),
            ("x-mbx-order-count-18446744073709551615s".into(), "5".into()),
            ("x-mbx-used-weight-é".into(), "6".into()),
            ("x-mbx-order-count-1m".into(), "not-a-number".into()),
        ],
        body: br#"{"serverTime":123}"#.to_vec(),
    };
    let parsed = BinanceResponse::<BinanceTimeResponse>::try_from_http(response).unwrap();
    assert!(parsed.rate_limit_usage().unwrap().is_empty());
}

#[test]
fn http_response_records_usage_retry_after_and_success_payload() {
    let response = HttpResponse {
        status: 200,
        headers: vec![
            ("x-mbx-used-weight-1m".into(), "12".into()),
            ("x-mbx-order-count-10s".into(), "1".into()),
            ("Retry-After".into(), "7".into()),
        ],
        body: br#"{"serverTime":123}"#.to_vec(),
    };
    let parsed = BinanceResponse::<BinanceTimeResponse>::try_from_http(response).unwrap();
    assert!(matches!(parsed.payload, BinanceResponsePayload::Success(_)));
    assert_eq!(parsed.metadata.status, 200);
    assert_eq!(parsed.retry_after(), Some(Seconds(7)));
    let usage = parsed.rate_limit_usage().unwrap();
    assert_eq!(
        usage
            .get(&RateLimit {
                restriction: RateLimitRestriction::Weight,
                interval_nanos: Nanoseconds(60_000_000_000),
            })
            .and_then(|usage| usage.used),
        Some(UsageCount(12))
    );
    assert_eq!(
        usage
            .get(&RateLimit {
                restriction: RateLimitRestriction::OrderCount,
                interval_nanos: Nanoseconds(10_000_000_000),
            })
            .and_then(|usage| usage.used),
        Some(UsageCount(1))
    );
}

#[test]
fn http_response_returns_binance_error_payload() {
    let response = HttpResponse {
        status: 400,
        headers: vec![],
        body: br#"{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}"#.to_vec(),
    };
    let parsed = BinanceResponse::<BinanceTimeResponse>::try_from_http(response).unwrap();
    let BinanceResponsePayload::Failure(error) = &parsed.payload else {
        panic!("expected failure payload");
    };
    assert_eq!(error.code, -2015);
    assert_eq!(error.msg, "Invalid API-key, IP, or permissions for action.");
    assert!(error.data.is_none());
}

#[test]
fn http_response_degrades_unparseable_bodies_to_failures() {
    let invalid_json = HttpResponse {
        status: 200,
        headers: vec![],
        body: b"not json".to_vec(),
    };
    let parsed = BinanceResponse::<BinanceTimeResponse>::try_from_http(invalid_json).unwrap();
    let BinanceResponsePayload::Failure(error) = &parsed.payload else {
        panic!("expected failure payload");
    };
    assert_eq!(error.code, -1);

    let service_unavailable = HttpResponse {
        status: 503,
        headers: vec![],
        body: b"Service Unavailable".to_vec(),
    };
    let parsed =
        BinanceResponse::<BinanceTimeResponse>::try_from_http(service_unavailable).unwrap();
    let BinanceResponsePayload::Failure(error) = &parsed.payload else {
        panic!("expected failure payload");
    };
    assert_eq!(error.code, 503);
    assert_eq!(error.msg, "Service Unavailable");
}

#[test]
fn websocket_response_parses_success_result() {
    let frame = json!({"id": 1, "status": 200, "result": {"serverTime": 456}});
    let parsed = BinanceResponse::<BinanceTimeResponse>::try_from_websocket(frame).unwrap();
    assert!(matches!(parsed.payload, BinanceResponsePayload::Success(_)));
    assert_eq!(parsed.metadata.websocket_id, Some(ETWebsocketId::Int(1)));
    assert_eq!(parsed.metadata.status, 200);
    assert_eq!(parsed.retry_after(), None);
}

#[test]
fn websocket_response_computes_retry_after_from_error_data() {
    let frame = json!({
        "id": 1,
        "status": 429,
        "error": {
            "code": -1003,
            "msg": "Too many requests",
            "data": {"serverTime": 1000, "retryAfter": 3500}
        }
    });
    let parsed = BinanceResponse::<BinanceTimeResponse>::try_from_websocket(frame).unwrap();
    let BinanceResponsePayload::Failure(error) = &parsed.payload else {
        panic!("expected failure payload");
    };
    assert_eq!(error.code, -1003);
    assert_eq!(parsed.retry_after(), Some(Seconds(3)));
    assert_eq!(parsed.metadata.websocket_id, Some(ETWebsocketId::Int(1)));
}

#[test]
fn websocket_response_records_frame_rate_limits() {
    let frame = json!({
        "id": 1,
        "status": 200,
        "rateLimits": [
            {
                "rateLimitType": "REQUEST_WEIGHT",
                "interval": "MINUTE",
                "intervalNum": 1,
                "limit": 6000,
                "count": 5
            }
        ],
        "result": {"serverTime": 1}
    });
    let parsed = BinanceResponse::<BinanceTimeResponse>::try_from_websocket(frame).unwrap();
    let usage = parsed.rate_limit_usage().unwrap();
    let rate_usage = usage
        .get(&RateLimit {
            restriction: RateLimitRestriction::Weight,
            interval_nanos: Nanoseconds(60_000_000_000),
        })
        .unwrap();
    assert_eq!(rate_usage.limit, Some(UsageCount(6000)));
    assert_eq!(rate_usage.used, Some(UsageCount(5)));
}

#[test]
fn http_response_saturates_large_retry_after() {
    let response = HttpResponse {
        status: 429,
        headers: vec![("Retry-After".into(), u64::MAX.to_string())],
        body: br#"{"serverTime":123}"#.to_vec(),
    };
    let parsed = BinanceResponse::<BinanceTimeResponse>::try_from_http(response).unwrap();
    assert_eq!(parsed.retry_after(), Some(Seconds(i64::MAX)));
}

#[test]
fn websocket_response_saturates_rate_limit_values() {
    let frame = json!({
        "id": 1,
        "status": 200,
        "rateLimits": [
            {
                "rateLimitType": "REQUEST_WEIGHT",
                "interval": "DAY",
                "intervalNum": u32::MAX,
                "limit": i64::MIN,
                "count": i64::MIN
            }
        ],
        "result": {"serverTime": 1}
    });
    let parsed = BinanceResponse::<BinanceTimeResponse>::try_from_websocket(frame).unwrap();
    let usage = parsed.rate_limit_usage().unwrap();
    let rate_usage = usage
        .get(&RateLimit {
            restriction: RateLimitRestriction::Weight,
            interval_nanos: Nanoseconds(i64::MAX),
        })
        .unwrap();
    assert_eq!(rate_usage.limit, Some(UsageCount(0)));
    assert_eq!(rate_usage.used, Some(UsageCount(0)));
}

#[test]
fn websocket_response_without_result_or_error_is_a_failure() {
    let frame = json!({"id": 1, "status": 200});
    let parsed = BinanceResponse::<BinanceTimeResponse>::try_from_websocket(frame).unwrap();
    let BinanceResponsePayload::Failure(error) = &parsed.payload else {
        panic!("expected failure payload");
    };
    assert_eq!(error.code, -1);
    assert_eq!(
        error.msg,
        "Websocket response missing both error and result"
    );
}
