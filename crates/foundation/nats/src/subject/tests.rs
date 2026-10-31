use super::*;

#[test]
fn rejects_publish_subjects_with_wildcards_or_empty_tokens() {
    assert!(NatsSubject::validate("market.events.binance.btcusdt.trade").is_ok());
    assert!(NatsSubject::validate("market..trade").is_err());
    assert!(NatsSubject::validate("market.*").is_err());
    assert!(NatsSubject::validate("market.>").is_err());
    assert!(NatsSubject::validate("market trade").is_err());
}

#[test]
fn normalizes_without_creating_empty_tokens() {
    assert_eq!(
        NatsSubject::normalize_segment("BTC/USDT @ Trade"),
        "btc.usdt.trade"
    );
    assert_eq!(NatsSubject::normalize_segment("..BTC..USDT.."), "btc.usdt");
    assert_eq!(NatsSubject::normalize_segment("!!!"), "unknown");
}
