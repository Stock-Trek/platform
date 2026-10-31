use super::*;
use crate::{
    candle::{Candle, CandleInterval, CandleKey, ClosedCandle},
    event::{GapMarker, MarketEvent},
};
use platform_types::new_types::TimestampMilliseconds;
use stock_trek::markets::{MarketCandle, MarketOhlcv};
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

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

fn closed_candle() -> ClosedCandle {
    let candle = Candle {
        key: CandleKey::new(
            CexId::Binance,
            TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            CandleInterval::from_label("1m").unwrap(),
        ),
        market_candle: MarketCandle::new(
            60_000,
            120_000,
            60_000,
            true,
            MarketOhlcv::new(1.0, 2.0, 0.5, 1.5, 10.0, 15.0, 1.5),
            5,
        ),
        is_complete: true,
    };
    ClosedCandle::new(candle, TimestampMilliseconds(121_000), 3, 0)
}

#[test]
fn market_event_round_trips_through_the_envelope() {
    let expected = event();
    let encoded = expected.encode().unwrap();
    let decoded = MarketEvent::decode(&encoded).unwrap();

    assert_eq!(decoded, expected);
}

#[test]
fn gap_marker_round_trips_through_the_envelope() {
    let expected = GapMarker::gap(7, 7);
    let encoded = expected.encode().unwrap();
    let decoded = GapMarker::decode(&encoded).unwrap();

    assert_eq!(decoded, expected);
}

#[test]
fn closed_candle_round_trips_through_the_envelope() {
    let expected = closed_candle();
    let encoded = expected.encode().unwrap();
    let decoded = ClosedCandle::decode(&encoded).unwrap();

    assert_eq!(decoded.encode().unwrap(), encoded);
}

#[test]
fn invalid_payloads_return_an_error() {
    assert!(MarketEvent::decode(b"not json").is_err());
}
