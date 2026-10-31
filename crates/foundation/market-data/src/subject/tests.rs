use super::*;
use platform_nats::NatsSubject;
use stock_trek_types::cex::cex_id::CexId;

#[test]
fn builds_event_and_gap_subjects_from_the_same_contract() {
    let subject = MarketEventSubject::new("market", CexId::Binance);

    assert_eq!(
        subject.event("btcusdt@trade"),
        "market.events.binance.btcusdt.trade"
    );
    assert_eq!(
        subject.gap("btcusdt@trade"),
        "market.gaps.binance.btcusdt.trade"
    );
}

#[test]
fn normalizes_stream_segments() {
    let subject = MarketEventSubject::new("market", CexId::Binance);

    let event_subject = subject.event("BTC/USDT @ Trade");
    assert_eq!(event_subject, "market.events.binance.btc.usdt.trade");
    assert!(NatsSubject::validate(&event_subject).is_ok());
    assert!(NatsSubject::validate(&subject.gap("BTC/USDT @ Trade")).is_ok());
}

#[test]
fn builds_wildcards_for_subscriptions() {
    let subject = MarketEventSubject::new("market", CexId::Binance);

    assert_eq!(subject.wildcard(), "market.>");
    assert_eq!(subject.event_wildcard(), "market.events.binance.>");
    assert_eq!(subject.gap_wildcard(), "market.gaps.binance.>");
    assert_eq!(MarketEventSubject::prefix_wildcard("market"), "market.>");
}
