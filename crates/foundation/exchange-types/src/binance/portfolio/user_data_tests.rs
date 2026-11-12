use crate::binance::portfolio::BinanceUserDataEvent;
use serde_json::json;

#[test]
fn parses_account_position_events() {
    let event = BinanceUserDataEvent::from_value(&json!({
        "e": "outboundAccountPosition",
        "E": 1_700_000_000_000_i64,
        "u": 42,
        "B": [
            {"a": "BTC", "f": "1.5", "l": "0.5"},
            {"a": "USDT", "f": "250", "l": "0"}
        ]
    }))
    .unwrap();
    assert!(event.is_account_position());
    assert!(!event.is_listen_key_expired());
    assert_eq!(event.event_time(), 1_700_000_000_000);
    assert_eq!(event.version(), Some(42));
    let balances = event.balances().unwrap();
    assert_eq!(balances[0].asset(), "BTC");
    assert_eq!(balances[0].free(), "1.5");
    assert_eq!(balances[0].locked(), "0.5");
}

#[test]
fn parses_listen_key_expiration_events() {
    let event = BinanceUserDataEvent::from_value(&json!({
        "e": "listenKeyExpired",
        "E": 1_700_000_000_000_i64
    }))
    .unwrap();
    assert!(event.is_listen_key_expired());
    assert_eq!(event.balances(), None);
}
