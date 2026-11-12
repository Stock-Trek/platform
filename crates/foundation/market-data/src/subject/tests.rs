use crate::subject::MarketEventSubject;
use platform_nats::NatsSubject;
use stock_trek_types::cex::cex_id::CexId;

#[test]
fn builds_event_and_gap_subjects_from_the_same_contract() {
    let subject = MarketEventSubject::new("market", CexId::Binance);

    assert_eq!(
        subject.event("btcusdt@trade"),
        format!("market.events.{}.btcusdt-trade", CexId::Binance)
    );
    assert_eq!(
        subject.gap("btcusdt@trade"),
        format!("market.gaps.{}.btcusdt-trade", CexId::Binance)
    );
}

#[test]
fn normalizes_stream_segments_to_one_token() {
    let subject = MarketEventSubject::new("market", CexId::Binance);

    let event_subject = subject.event("BTC/USDT @ Trade");
    assert_eq!(
        event_subject,
        format!("market.events.{}.btc-usdt-trade", CexId::Binance)
    );
    assert_eq!(event_subject.split('.').count(), 4);
    assert!(NatsSubject::validate(&event_subject).is_ok());
    assert!(NatsSubject::validate(&subject.gap("BTC/USDT @ Trade")).is_ok());
}

#[test]
fn builds_wildcards_for_subscriptions() {
    let subject = MarketEventSubject::new("market", CexId::Binance);

    assert_eq!(subject.wildcard(), "market.>");
    assert_eq!(
        subject.event_wildcard(),
        format!("market.events.{}.>", CexId::Binance)
    );
    assert_eq!(
        subject.gap_wildcard(),
        format!("market.gaps.{}.>", CexId::Binance)
    );
    assert_eq!(MarketEventSubject::prefix_wildcard("market"), "market.>");
}
