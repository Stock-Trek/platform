use super::*;

#[test]
fn rejects_publish_subjects_with_wildcards_or_empty_tokens() {
    assert!(NatsSubject::validate("market.events.binance.btcusdt-trade").is_ok());
    assert!(NatsSubject::validate("market..trade").is_err());
    assert!(NatsSubject::validate("market.*").is_err());
    assert!(NatsSubject::validate("market.>").is_err());
    assert!(NatsSubject::validate("market trade").is_err());
}

#[test]
fn normalizes_into_one_non_empty_token() {
    assert_eq!(
        NatsSubject::normalize_segment("BTC/USDT @ Trade"),
        "btc-usdt-trade"
    );
    assert_eq!(NatsSubject::normalize_segment("..BTC..USDT.."), "btc-usdt");
    assert_eq!(NatsSubject::normalize_segment("!!!"), "unknown");

    for value in ["BTC/USDT @ Trade", "..BTC..USDT..", "btcusdt@trade.."] {
        let segment = NatsSubject::normalize_segment(value);
        assert!(!segment.contains('.'), "{segment}");
        assert!(NatsSubject::validate(&format!("market.events.binance.{segment}")).is_ok());
    }
}
