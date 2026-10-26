use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    binance::{
        exchange::BinanceExchange,
        exchange_info::BinanceExchangeInfoRequest,
        spot_order::BinanceSpotOrderRequest,
        supporting_types::{BinanceOrderType, BinanceSide},
        time::BinanceTimeRequest,
        websocket_session_logon::BinanceSessionLogonRequest,
        websocket_session_logout::BinanceSessionLogoutRequest,
    },
    http::HttpMethod,
    new_types::TimestampMilliseconds,
    request::{ETHttpRequest, ETRequest, ETWebsocketRequest},
    signer::Signer,
    websocket_id::ETWebsocketId,
};
use rust_decimal::Decimal;
use secrecy::SecretString;
use serde_json::{Value, json};

fn signer(api_key: &str, secret: &str) -> Signer {
    BinanceExchange::signer_hmac_sha256(ApiKeyCredentials {
        api_key: api_key.into(),
        secret: SecretString::from(secret),
    })
    .unwrap()
}

#[test]
fn signed_http_request_embeds_verifiable_signature_and_api_key_header() {
    let api_key = "api-key";
    let signer = signer(api_key, "secret-key");
    let mut request = BinanceSpotOrderRequest::new(
        "BTCUSDT",
        BinanceSide::BUY,
        BinanceOrderType::LIMIT,
        Decimal::new(5, 1),
    );
    request.set_timestamp(TimestampMilliseconds(1_700_000_000_000));
    let http = request.try_into_http(&signer).unwrap();
    assert_eq!(http.method, HttpMethod::POST);
    assert_eq!(http.path, "order");
    let query = http.query.unwrap();
    let (params, signature) = query.split_once("&signature=").unwrap();
    for param in [
        "quantity=0.5",
        "side=BUY",
        "symbol=BTCUSDT",
        "timestamp=1700000000000",
        "type=LIMIT",
    ] {
        assert!(params.contains(param), "missing {param} in {params}");
    }
    assert_eq!(
        signature,
        signer.signature(params.as_bytes()).unwrap(),
        "signature does not verify against the query params"
    );
    assert_eq!(
        http.headers,
        vec![("X-MBX-APIKEY".to_string(), api_key.to_string())]
    );
    assert!(http.body.is_none());
}

#[test]
fn unsigned_http_request_has_no_headers_or_signature() {
    let http = BinanceTimeRequest::new()
        .try_into_http(&signer("", ""))
        .unwrap();
    assert_eq!(http.method, HttpMethod::GET);
    assert_eq!(http.path, "time");
    assert_eq!(http.query, None);
    assert!(http.headers.is_empty());
    assert!(http.body.is_none());
}

#[test]
fn http_request_percent_encodes_list_query_params() {
    let request = BinanceExchangeInfoRequest::Symbols {
        symbols: vec!["BTCUSDT".into(), "ETHUSDT".into()],
    };
    let http = request.try_into_http(&signer("", "")).unwrap();
    assert_eq!(http.path, "exchangeInfo");
    let query = http.query.unwrap();
    assert!(query.starts_with("symbols="));
    assert!(query.contains("symbols=%5B%22BTCUSDT%22%2C%22ETHUSDT%22%5D"));
    assert!(http.headers.is_empty());
}

#[test]
fn signed_websocket_request_includes_verifiable_signature_and_api_key() {
    let api_key = "api-key";
    let signer = signer(api_key, "secret-key");
    let mut request = BinanceSessionLogonRequest::new();
    request.set_timestamp(TimestampMilliseconds(1_234_567_890));
    let (message, matcher) = request
        .try_into_websocket(&signer, ETWebsocketId::Int(42))
        .unwrap();
    let message: Value = serde_json::from_str(&message).unwrap();
    assert_eq!(message["method"], "session.logon");
    assert_eq!(message["id"], 42);
    assert_eq!(message["params"]["apiKey"], api_key);
    assert_eq!(message["params"]["timestamp"], 1_234_567_890);
    let payload = format!("apiKey={api_key}&timestamp=1234567890");
    assert_eq!(
        message["params"]["signature"],
        signer.signature(payload.as_bytes()).unwrap()
    );
    assert!(matcher.as_ref()(&json!({"id": 42})));
    assert!(!matcher.as_ref()(&json!({"id": 43})));
    assert!(!matcher.as_ref()(&json!({"id": "42"})));
    assert!(!matcher.as_ref()(&json!({})));
}

#[test]
fn unsigned_websocket_request_omits_params_and_matches_string_id() {
    let (message, matcher) = BinanceSessionLogoutRequest::new()
        .try_into_websocket(&signer("", ""), ETWebsocketId::Str("session-1".into()))
        .unwrap();
    let message: Value = serde_json::from_str(&message).unwrap();
    assert_eq!(message["method"], "session.logout");
    assert_eq!(message["id"], "session-1");
    assert!(message.get("params").is_none());
    assert!(matcher.as_ref()(&json!({"id": "session-1"})));
    assert!(!matcher.as_ref()(&json!({"id": "session-2"})));
    assert!(!matcher.as_ref()(&json!({"id": 1})));
}
