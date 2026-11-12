use super::listen_key::{BinanceListenKeyRequest, BinanceListenKeyResponse};
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials, binance::exchange::BinanceExchange, http::HttpMethod,
    request::ETHttpRequest,
};
use secrecy::SecretString;
use serde_json::json;

fn signer() -> platform_exchange_types::signer::Signer {
    BinanceExchange::signer_hmac_sha256(ApiKeyCredentials {
        api_key: "api-key".into(),
        secret: SecretString::from("secret"),
    })
    .unwrap()
}

#[test]
fn creates_a_listen_key_without_a_query() {
    let request = BinanceListenKeyRequest::create()
        .try_into_http(&signer())
        .unwrap();
    assert_eq!(request.method, HttpMethod::POST);
    assert_eq!(request.path, "userDataStream");
    assert_eq!(request.query, None);
    assert_eq!(
        request.headers,
        vec![("X-MBX-APIKEY".to_string(), "api-key".to_string())]
    );
}

#[test]
fn keeps_a_listen_key_alive_with_a_percent_encoded_query() {
    let request = BinanceListenKeyRequest::keepalive("key/with space")
        .try_into_http(&signer())
        .unwrap();
    assert_eq!(request.method, HttpMethod::PUT);
    assert_eq!(
        request.query.as_deref(),
        Some("listenKey=key%2Fwith%20space")
    );
}

#[test]
fn closes_a_listen_key() {
    let request = BinanceListenKeyRequest::close("listen-key")
        .try_into_http(&signer())
        .unwrap();
    assert_eq!(request.method, HttpMethod::DELETE);
    assert_eq!(request.query.as_deref(), Some("listenKey=listen-key"));
}

#[test]
fn requires_a_non_empty_listen_key() {
    assert!(
        BinanceListenKeyResponse::default()
            .require_listen_key()
            .is_err()
    );
    let response =
        serde_json::from_value::<BinanceListenKeyResponse>(json!({"listenKey": ""})).unwrap();
    assert!(response.require_listen_key().is_err());
}
