use super::*;
use stock_trek_types::cex::cex_id::CexId;

fn event() -> MarketEvent {
    MarketEvent {
        exchange: CexId::Binance,
        stream: "btcusdt@trade".to_string(),
        sequence: Some(7),
        exchange_timestamp: Some(TimestampMilliseconds(1_700_000_000_000)),
        received_at: TimestampMilliseconds(1_700_000_000_001),
        payload: "eyJlIjoidHJhZGUifQ==".to_string(),
    }
}

fn message(subject: &str, payload: Vec<u8>) -> Message {
    Message {
        subject: subject.to_string(),
        payload,
        ..Message::default()
    }
}

#[test]
fn decodes_market_events_from_event_subjects() {
    let expected = event();
    let message = message(
        "market.events.binance.btcusdt.trade",
        expected.encode().unwrap(),
    );

    assert_eq!(
        MarketInput::from_market_message(&message).unwrap(),
        MarketInput::Event(expected)
    );
}

#[test]
fn decodes_gap_markers_from_gap_subjects() {
    let expected = GapMarker {
        exchange: CexId::Binance,
        stream: "btcusdt@trade".to_string(),
        previous_sequence: 7,
        observed_sequence: 9,
        missing_from: 8,
        missing_to: 8,
        detected_at: TimestampMilliseconds(10),
    };
    let message = message(
        "market.gaps.binance.btcusdt.trade",
        expected.encode().unwrap(),
    );

    assert_eq!(
        MarketInput::from_market_message(&message).unwrap(),
        MarketInput::Gap(expected)
    );
    assert!(MarketInput::is_gap_subject(
        "market.gaps.binance.btcusdt.trade"
    ));
}

#[test]
fn routed_inputs_round_trip() {
    let input = MarketInput::Event(event());

    assert_eq!(
        MarketInput::decode(&input.encode().unwrap()).unwrap(),
        input
    );
    assert_eq!(
        NatsShardSource::decode_routed(&input.encode().unwrap()).unwrap(),
        input
    );
}

#[test]
fn malformed_payloads_are_rejected() {
    let message = message("market.events.binance.btcusdt.trade", b"not json".to_vec());
    assert!(MarketInput::from_market_message(&message).is_err());
}

#[test]
fn event_and_gap_prefixes_share_the_stream_root() {
    assert_eq!(
        NatsMarketSource::common_subject_prefix("market.events", "market.gaps").as_deref(),
        Some("market")
    );
    assert_eq!(
        NatsMarketSource::common_subject_prefix("market.events.binance", "market.events")
            .as_deref(),
        Some("market.events")
    );
    assert_eq!(
        NatsMarketSource::common_subject_prefix("events", "gaps"),
        None
    );
}
