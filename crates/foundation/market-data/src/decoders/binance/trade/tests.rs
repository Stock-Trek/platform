use super::*;
use crate::decoders::trade::TradeDecoder;
use crate::event::MarketEvent;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use platform_metrics::descriptions::{DROP_REASON_INVALID_TRADE, STAGE_AGGREGATOR};
use platform_types::new_types::TimestampMilliseconds;
use rust_decimal::Decimal;
use serde_json::Value;
use stock_trek_types::cex::trading_pair::TradingPair;
use platform_metrics::descriptions::MARKET_DATA_DROPPED_EVENTS_TOTAL;
use platform_metrics::test_utils::TestRecorder;
use serde_json::json;
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId};

fn event(stream: &str, payload: Value) -> MarketEvent {
    MarketEvent {
        exchange: CexId::Binance,
        stream: stream.to_string(),
        sequence: None,
        exchange_timestamp: Some(TimestampMilliseconds(1_700_000_000_000)),
        received_at: TimestampMilliseconds(1_700_000_000_001),
        payload: STANDARD.encode(serde_json::to_vec(&payload).unwrap()),
    }
}

fn sbe_trade_event() -> MarketEvent {
    sbe_trade_event_with_trades(&[(1, 10_025, 2_500), (2, 10_050, 1_000)])
}

fn sbe_trade_event_with_trades(trades: &[(i64, i64, i64)]) -> MarketEvent {
    let event_time_us = 1_700_000_000_123_000_i64;
    let mut frame = Vec::new();
    frame.extend_from_slice(&18u16.to_le_bytes());
    frame.extend_from_slice(&10_000u16.to_le_bytes());
    frame.extend_from_slice(&1u16.to_le_bytes());
    frame.extend_from_slice(&0u16.to_le_bytes());
    frame.extend_from_slice(&event_time_us.to_le_bytes());
    frame.extend_from_slice(&event_time_us.to_le_bytes());
    frame.push((-2_i8) as u8);
    frame.push((-3_i8) as u8);
    frame.extend_from_slice(&25u16.to_le_bytes());
    frame.extend_from_slice(&(trades.len() as u32).to_le_bytes());
    for &(trade_id, price, quantity) in trades {
        frame.extend_from_slice(&trade_id.to_le_bytes());
        frame.extend_from_slice(&price.to_le_bytes());
        frame.extend_from_slice(&quantity.to_le_bytes());
        frame.push(0);
    }
    frame.push(7);
    frame.extend_from_slice(b"btcusdt");

    MarketEvent {
        exchange: CexId::Binance,
        stream: "btcusdt@trade".to_string(),
        sequence: Some(2),
        exchange_timestamp: Some(TimestampMilliseconds(1_700_000_000_123)),
        received_at: TimestampMilliseconds(1_700_000_000_124),
        payload: STANDARD.encode(frame),
    }
}

#[test]
fn decodes_a_combined_trade_stream() {
    let market_event = event(
        "btcusdt@aggTrade",
        json!({
            "stream": "btcusdt@aggTrade",
            "data": {
                "e": "aggTrade",
                "s": "BTCUSDT",
                "p": "100.25",
                "q": "2.5",
                "T": 1_700_000_000_123_i64
            }
        }),
    );

    let trades = BinanceTradeDecoder.decode(&market_event).unwrap();
    assert_eq!(trades.len(), 1);
    assert_eq!(
        trades[0].trading_pair,
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD)
    );
    assert_eq!(trades[0].price, Decimal::from_str_exact("100.25").unwrap());
    assert_eq!(trades[0].quantity, Decimal::from_str_exact("2.5").unwrap());
    assert_eq!(
        trades[0].timestamp,
        TimestampMilliseconds(1_700_000_000_123)
    );
}

#[test]
fn resolves_streams_to_trading_pairs() {
    let decoder = BinanceTradeDecoder;
    let expected = TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD);

    assert_eq!(
        decoder.trading_pair_for_stream("btcusdt@trade").unwrap(),
        expected
    );
    assert_eq!(
        decoder.trading_pair_for_stream("BTCUSDT@aggTrade").unwrap(),
        expected
    );
    assert_eq!(
        decoder.trading_pair_for_stream("btcusdt").unwrap(),
        expected
    );
    assert!(decoder.trading_pair_for_stream("unknown@trade").is_err());
    assert!(decoder.trading_pair_for_stream("@trade").is_err());
}

#[test]
fn ignores_non_trade_streams() {
    let market_event = event("btcusdt@depth", json!({ "e": "depthUpdate" }));
    assert!(
        BinanceTradeDecoder
            .decode(&market_event)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn ignores_non_positive_trades() {
    let recorder = TestRecorder::default();
    let market_event = event(
        "btcusdt@trade",
        json!({ "e": "trade", "s": "BTCUSDT", "p": "0", "q": "1" }),
    );
    let trades = metrics::with_local_recorder(&recorder, || {
        BinanceTradeDecoder.decode(&market_event).unwrap()
    });
    assert!(trades.is_empty());
    assert_eq!(
        recorder.counter(
            MARKET_DATA_DROPPED_EVENTS_TOTAL,
            &[
                ("reason", DROP_REASON_INVALID_TRADE),
                ("stage", STAGE_AGGREGATOR),
            ],
        ),
        1
    );
}

#[test]
fn decodes_sbe_trades() {
    let trades = BinanceTradeDecoder.decode(&sbe_trade_event()).unwrap();

    assert_eq!(trades.len(), 2);
    assert_eq!(
        trades[0].trading_pair,
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD)
    );
    assert_eq!(trades[0].price, Decimal::from_str_exact("100.25").unwrap());
    assert_eq!(trades[0].quantity, Decimal::from_str_exact("2.5").unwrap());
    assert_eq!(
        trades[0].timestamp,
        TimestampMilliseconds(1_700_000_000_123)
    );
    assert_eq!(trades[1].price, Decimal::from_str_exact("100.5").unwrap());
    assert_eq!(trades[1].quantity, Decimal::from_str_exact("1").unwrap());
}

#[test]
fn counts_invalid_sbe_trades_as_dropped() {
    let recorder = TestRecorder::default();
    let market_event = sbe_trade_event_with_trades(&[(1, 0, 2_500), (2, 10_050, 1_000)]);
    let trades = metrics::with_local_recorder(&recorder, || {
        BinanceTradeDecoder.decode(&market_event).unwrap()
    });

    assert_eq!(trades.len(), 1);
    assert_eq!(
        recorder.counter(
            MARKET_DATA_DROPPED_EVENTS_TOTAL,
            &[
                ("reason", DROP_REASON_INVALID_TRADE),
                ("stage", STAGE_AGGREGATOR),
            ],
        ),
        1
    );
}

#[test]
fn rejects_malformed_base64() {
    let market_event = MarketEvent {
        exchange: CexId::Binance,
        stream: "btcusdt@trade".to_string(),
        sequence: None,
        exchange_timestamp: None,
        received_at: TimestampMilliseconds(0),
        payload: "not base64".to_string(),
    };
    assert!(BinanceTradeDecoder.decode(&market_event).is_err());
}
