use platform_errors::{
    BinanceError, BinanceErrorData, BpError, ErrorType, ExchangeError, Retryability,
};
use platform_exchange_types::{
    binance::{
        klines::{BinanceKlineResponse, BinanceKlinesResponse},
        response::{BinanceMetadata, BinanceResponse, BinanceResponsePayload},
    },
    klines::KlinesResponse,
};
use platform_types::new_types::Seconds;

fn response(
    status: u16,
    retry_after: Option<Seconds>,
    error: BinanceError,
) -> BinanceResponse<BinanceKlinesResponse> {
    BinanceResponse {
        metadata: BinanceMetadata {
            retry_after,
            status,
            usage: Default::default(),
            websocket_id: None,
        },
        payload: BinanceResponsePayload::<BinanceKlinesResponse>::Failure(error),
    }
}

fn error(code: i64, data: Option<BinanceErrorData>) -> BinanceError {
    BinanceError {
        code,
        msg: "exchange failure".into(),
        data,
    }
}

#[test]
fn binance_transient_codes_are_retryable() {
    for code in [-1003, -1001, -1007] {
        let error = response(400, None, error(code, None)).klines().unwrap_err();
        assert!(
            error.retryability().is_retryable(),
            "code {code} should be retryable"
        );
        assert_eq!(error.retryability(), Retryability::Retryable);
    }
}

#[test]
fn binance_permanent_codes_are_permanent() {
    let error = response(400, None, error(-2015, None))
        .klines()
        .unwrap_err();
    assert!(!error.retryability().is_retryable());
    assert_eq!(error.retryability(), Retryability::Permanent);
}

#[test]
fn exchange_failure_preserves_code_message_status_and_retry_after() {
    let failure = response(429, Some(Seconds(7)), error(-1003, None))
        .klines()
        .unwrap_err();
    let BpError::Exchange(ExchangeError::Response {
        code,
        message,
        status,
        retry_after,
        retryability,
    }) = failure
    else {
        panic!("expected exchange response failure");
    };
    assert_eq!(code, -1003);
    assert_eq!(message, "exchange failure");
    assert_eq!(status, 429);
    assert_eq!(retry_after, Some(Seconds(7)));
    assert_eq!(retryability, Retryability::Retryable);
}

#[test]
fn exchange_failure_uses_http_status_for_unknown_codes() {
    let error = response(503, None, error(-1, None)).klines().unwrap_err();
    assert!(error.retryability().is_retryable());
}

#[test]
fn exchange_failure_computes_retry_after_from_error_data() {
    let failure = response(
        429,
        None,
        error(
            -1003,
            Some(BinanceErrorData {
                serverTime: Some(1_000),
                retryAfter: Some(3_500),
            }),
        ),
    )
    .klines()
    .unwrap_err();
    let BpError::Exchange(ExchangeError::Response { retry_after, .. }) = failure else {
        panic!("expected exchange response failure");
    };
    assert_eq!(retry_after, Some(Seconds(3)));
}

#[test]
fn binance_error_can_be_wrapped_by_bp_error() {
    let error = BpError::from(error(-1003, None));
    assert!(matches!(error, BpError::Binance(_)));
    assert_eq!(error.retryability(), Retryability::Retryable);
    assert_eq!(error.error_type(), "binance");
}

#[test]
fn binance_code_mapping_is_exposed() {
    assert_eq!(
        Retryability::from_binance_code(-1003),
        Retryability::Retryable
    );
    assert_eq!(
        Retryability::from_binance_code(-2015),
        Retryability::Permanent
    );
    assert_eq!(
        Retryability::from_binance_code_and_status(-1, 503),
        Retryability::Retryable
    );
}

#[test]
fn success_payload_still_returns_klines() {
    let response = BinanceResponse::<BinanceKlinesResponse> {
        metadata: BinanceMetadata {
            retry_after: None,
            status: 200,
            usage: Default::default(),
            websocket_id: None,
        },
        payload: BinanceResponsePayload::Success(Vec::<BinanceKlineResponse>::new()),
    };
    assert!(response.klines().unwrap().is_empty());
}
