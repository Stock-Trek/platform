use platform_exchange_types::{
    binance::urls::BinanceUrls,
    urls::{LocalhostUrls, Protocol, TradingMode, Urls},
};

#[test]
fn exchange_defaults_are_available_without_environment_configuration() {
    assert_eq!(
        BinanceUrls.default(Protocol::Http, TradingMode::Real),
        "https://api.binance.com/api/v3"
    );
    assert_eq!(
        BinanceUrls.default(Protocol::Http, TradingMode::Paper),
        "https://testnet.binance.vision/api/v3"
    );
    assert_eq!(
        BinanceUrls.default(Protocol::Websocket, TradingMode::Real),
        "wss://ws-api.binance.com:443/ws-api/v3"
    );
}

#[test]
fn localhost_defaults_use_the_protocol_scheme() {
    assert_eq!(
        LocalhostUrls.default(Protocol::Http, TradingMode::Real),
        "http://localhost"
    );
    assert_eq!(
        LocalhostUrls.default(Protocol::Websocket, TradingMode::Real),
        "ws://localhost"
    );
}

#[test]
fn protocol_validation_checks_the_scheme() {
    assert!(
        Protocol::Http
            .validate_url("URL", "https://example.test/api")
            .is_ok()
    );
    assert!(
        Protocol::Websocket
            .validate_url("URL", "https://example.test/ws")
            .is_err()
    );
    assert!(
        Protocol::Websocket
            .validate_url("URL", "wss://example.test/ws")
            .is_ok()
    );
}
