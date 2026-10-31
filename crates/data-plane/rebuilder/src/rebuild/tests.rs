use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use platform_aggregator::processor::{CandleProcessor, ProcessorConfig};
use platform_market_data::candle::CandleInterval;
use platform_market_data::candle::ClosedCandle;
use platform_market_data::decoders::trade::TradeDecoderFactory;
use platform_market_data::event::MarketEvent;
use platform_types::new_types::DurationMilliseconds;
use platform_types::new_types::TimestampMilliseconds;
use std::collections::BTreeMap;
use stock_trek_types::cex::cex_id::CexId;

fn event(sequence: u64, timestamp: i64) -> MarketEvent {
    stream_event("btcusdt@trade", sequence, timestamp, "100", "1")
}

fn stream_event(
    stream: &str,
    sequence: u64,
    timestamp: i64,
    price: &str,
    quantity: &str,
) -> MarketEvent {
    let symbol = stream
        .split_once('@')
        .map_or(stream, |(symbol, _)| symbol)
        .to_ascii_uppercase();
    let event_type = if stream.to_ascii_lowercase().ends_with("@aggtrade") {
        "aggTrade"
    } else {
        "trade"
    };
    MarketEvent {
            exchange: CexId::Binance,
            stream: stream.to_string(),
            sequence: Some(sequence),
            exchange_timestamp: Some(TimestampMilliseconds(timestamp)),
            received_at: TimestampMilliseconds(timestamp),
            payload: STANDARD.encode(format!(
                r#"{{"e":"{event_type}","E":{timestamp},"s":"{symbol}","p":"{price}","q":"{quantity}","T":{timestamp}}}"#
            )),
        }
}

fn event_with_trade_time(sequence: u64, event_timestamp: i64, trade_timestamp: i64) -> MarketEvent {
    MarketEvent {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            sequence: Some(sequence),
            exchange_timestamp: Some(TimestampMilliseconds(event_timestamp)),
            received_at: TimestampMilliseconds(event_timestamp),
            payload: STANDARD.encode(format!(
                r#"{{"e":"trade","E":{event_timestamp},"s":"BTCUSDT","p":"100","q":"1","T":{trade_timestamp}}}"#
            )),
        }
}

fn options() -> RebuildOptions {
    RebuildOptions {
        processor: ProcessorConfig::without_retention(
            vec![CandleInterval::from_label("1m").unwrap()],
            DurationMilliseconds::ZERO,
        ),
        state_epoch: 7,
        finish_open_windows: true,
    }
}

#[test]
fn replay_is_deterministic_and_assigns_the_configured_epoch() {
    let events = vec![
        event(1, 1_000),
        event(2, 61_000),
        event(3, 122_000),
        event(4, 183_000),
    ];

    let first = options().rebuild(events.clone()).unwrap();
    let second = options().rebuild(events).unwrap();

    assert_eq!(first.stats.events_read, 4);
    assert_eq!(first.stats.trade_stream_groups, 1);
    assert_eq!(first.stats.trades_applied, 4);
    assert_eq!(first.stats.decode_failures, 0);
    assert_eq!(first.stats.unique_candles, 4);
    assert_eq!(
        first
            .candles
            .iter()
            .map(ClosedCandle::message_id)
            .collect::<Vec<_>>(),
        second
            .candles
            .iter()
            .map(ClosedCandle::message_id)
            .collect::<Vec<_>>()
    );
    assert!(
        first
            .candles
            .iter()
            .all(|candle| candle.revision.state_epoch == 7)
    );
}

#[test]
fn duplicate_sequences_do_not_change_the_regenerated_candles() {
    let unique = options()
        .rebuild(vec![event(1, 1_000), event(2, 61_000), event(3, 122_000)])
        .unwrap();
    let duplicated = options()
        .rebuild(vec![
            event(1, 1_000),
            event(2, 61_000),
            event(2, 61_000),
            event(3, 122_000),
        ])
        .unwrap();

    assert_eq!(duplicated.stats.duplicate_events, 1);
    assert_eq!(
        unique
            .candles
            .iter()
            .map(ClosedCandle::message_id)
            .collect::<Vec<_>>(),
        duplicated
            .candles
            .iter()
            .map(ClosedCandle::message_id)
            .collect::<Vec<_>>()
    );
}

#[test]
fn multi_stream_pairs_share_one_processor_and_candle() {
    // @trade and @aggTrade use independent sequence spaces, so equal
    // sequence numbers must not be mistaken for duplicate events.
    let events = vec![
        stream_event("btcusdt@trade", 1, 1_000, "100", "1"),
        stream_event("btcusdt@aggTrade", 1, 1_000, "200", "2"),
    ];

    let result = options().rebuild(events).unwrap();

    assert_eq!(result.stats.trade_stream_groups, 1);
    assert_eq!(result.stats.trades_applied, 2);
    assert_eq!(result.stats.duplicate_events, 0);
    assert_eq!(result.stats.unique_candles, 1);
    let candle = &result.candles[0].candle;
    assert_eq!(candle.market_candle.trade_count, 2);
    assert_eq!(candle.market_candle.ohlcv.high, 200.0);
    assert_eq!(candle.market_candle.ohlcv.low, 100.0);
    assert_eq!(candle.market_candle.ohlcv.volume, 3.0);
}

#[test]
fn multi_stream_pair_revisions_do_not_collide() {
    let events = vec![
        stream_event("btcusdt@trade", 1, 1_000, "100", "1"),
        stream_event("btcusdt@aggTrade", 1, 1_000, "200", "2"),
        stream_event("btcusdt@trade", 2, 61_000, "100", "1"),
        stream_event("btcusdt@aggTrade", 2, 61_000, "200", "2"),
    ];

    let result = options().rebuild(events).unwrap();

    assert_eq!(result.stats.trade_stream_groups, 1);
    assert_eq!(result.stats.unique_candles, 2);
    assert_eq!(
        result
            .candles
            .iter()
            .map(ClosedCandle::message_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        2
    );
}

#[test]
fn pairs_are_replayed_independently() {
    let other = stream_event("ethusdt@trade", 1, 1_000, "10", "2");
    let events = vec![event(1, 1_000), event(2, 61_000), other];

    let result = options().rebuild(events).unwrap();

    assert_eq!(result.stats.trade_stream_groups, 2);
    assert_eq!(result.stats.trades_applied, 3);
    assert_eq!(result.stats.late_trades, 0);
}

#[test]
fn finishing_is_optional() {
    let events = vec![event(1, 1_000)];
    let mut unfinished = options();
    unfinished.finish_open_windows = false;

    let finished = options().rebuild(events.clone()).unwrap();
    let unfinished = unfinished.rebuild(events).unwrap();

    assert_eq!(finished.candles.len(), 1);
    assert!(unfinished.candles.is_empty());
}

#[test]
fn rebuilt_candles_match_live_processing_for_cap_eviction_and_a_rejected_correction() {
    let interval = CandleInterval::from_label("1m").unwrap();
    let cutoff = DurationMilliseconds(1_000);
    let processor = ProcessorConfig::new(
        vec![interval],
        DurationMilliseconds::ZERO,
        BTreeMap::from([(interval, 1)]),
        cutoff,
    );

    let events = vec![
        event_with_trade_time(1, 61_000, 61_000),
        event_with_trade_time(2, 121_000, 121_000),
        event_with_trade_time(3, 181_000, 181_000),
        event_with_trade_time(4, 181_000, 61_500),
    ];

    let mut live = CandleProcessor::with_config(processor.clone());
    live.state_mut().state_epoch = 7;
    let decoder = TradeDecoderFactory::for_exchange(CexId::Binance).unwrap();
    let mut live_candles = Vec::new();
    for (offset, event) in events.iter().enumerate() {
        live.state_mut().mark_processed(offset as u64);
        let trades = decoder.decode(event).unwrap();
        let update = live.apply_event(event, &trades);
        live_candles.extend(update.closed);
    }

    let mut options = options();
    options.processor = processor;
    options.finish_open_windows = false;
    let rebuilt = options.rebuild(events).unwrap();

    assert_eq!(rebuilt.candles.len(), 2);
    assert_eq!(
        rebuilt
            .candles
            .iter()
            .map(ClosedCandle::message_id)
            .collect::<Vec<_>>(),
        live_candles
            .iter()
            .map(ClosedCandle::message_id)
            .collect::<Vec<_>>()
    );
    assert!(
        rebuilt
            .candles
            .iter()
            .all(
                |candle| candle.candle.start_time() != TimestampMilliseconds(60_000)
                    || candle.candle.market_candle.trade_count == 1
            )
    );
}
