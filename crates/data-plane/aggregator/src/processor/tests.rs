//! Tests for candle folding, deduplication, and window/gap handling.

use super::{CandleProcessor, ProcessorConfig};
use crate::state::{AggregatorState, SequenceRange};
use platform_market_data::{
    candle::{CandleInterval, CandleKey},
    decoders::trade::Trade,
    event::{GapMarker, MarketEvent},
};
use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

fn trade(price: &str, timestamp: i64) -> Trade {
    Trade {
        exchange: CexId::Binance,
        trading_pair: TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        price: Decimal::from_str_exact(price).unwrap(),
        quantity: Decimal::ONE,
        timestamp: TimestampMilliseconds(timestamp),
    }
}

fn event(sequence: u64, timestamp: i64) -> MarketEvent {
    MarketEvent {
        exchange: CexId::Binance,
        stream: "btcusdt@trade".to_string(),
        sequence: Some(sequence),
        exchange_timestamp: Some(TimestampMilliseconds(timestamp)),
        received_at: TimestampMilliseconds(timestamp + 1),
        payload: String::new(),
    }
}

fn one_minute() -> CandleInterval {
    CandleInterval::from_label("1m").unwrap()
}

fn gap(detected_at: i64) -> GapMarker {
    GapMarker {
        exchange: CexId::Binance,
        stream: "btcusdt@trade".to_string(),
        previous_sequence: 1,
        observed_sequence: 3,
        missing_from: 2,
        missing_to: 2,
        detected_at: TimestampMilliseconds(detected_at),
    }
}

#[test]
fn configured_schema_version_is_used_for_new_and_restored_state() {
    let config = ProcessorConfig::without_retention(vec![one_minute()], DurationMilliseconds::ZERO)
        .with_state_schema_version(9);

    let processor = CandleProcessor::with_config(config.clone());
    assert_eq!(processor.state().schema_version, 9);

    let restored = CandleProcessor::from_state(config.clone(), processor.into_state()).unwrap();
    assert_eq!(restored.state().schema_version, 9);
    assert!(CandleProcessor::from_state(config, AggregatorState::new()).is_err());
}

#[test]
fn first_trade_opens_a_candle() {
    let mut processor = CandleProcessor::new(vec![one_minute()]);
    let update = processor.apply_trade(&trade("100", 61_000));

    assert!(update.closed.is_empty());
    assert_eq!(update.open.len(), 1);
    assert_eq!(update.open[0].start_time(), TimestampMilliseconds(60_000));
    assert_eq!(update.open[0].market_candle.ohlcv.open, 100.0);
    assert!(!update.open[0].market_candle.is_candle_closed);
}

#[test]
fn invalid_timestamps_are_rejected_without_mutating_state() {
    let mut processor = CandleProcessor::new(vec![one_minute()]);

    for timestamp in [-1, i64::MAX] {
        let update = processor.apply_trade(&trade("100", timestamp));

        assert!(update.closed.is_empty());
        assert!(update.open.is_empty());
        assert_eq!(processor.state().open_candles_len(), 0);
        assert!(
            processor
                .seen_sequences(CexId::Binance, "btcusdt@trade")
                .is_empty()
        );
    }
}

#[test]
fn crossing_a_boundary_closes_the_previous_candle_from_event_time() {
    let mut processor = CandleProcessor::new(vec![one_minute()]);
    processor.apply_trade(&trade("100", 61_000));
    let update = processor.apply_trade(&trade("101", 121_000));

    assert_eq!(update.closed.len(), 1);
    assert_eq!(
        update.closed[0].revision.close_watermark,
        TimestampMilliseconds(121_000)
    );
    assert_eq!(update.closed[0].revision.source_offset, 0);
    assert_eq!(update.closed[0].revision.state_epoch, 0);
    assert!(update.closed[0].candle.market_candle.is_candle_closed);
    assert_eq!(
        update.closed[0].candle.start_time(),
        TimestampMilliseconds(60_000)
    );
    assert_eq!(update.open.len(), 1);
    assert_eq!(update.open[0].start_time(), TimestampMilliseconds(120_000));
}

#[test]
fn closed_candle_revisions_are_reproducible_from_state() {
    let mut first = CandleProcessor::new(vec![one_minute()]);
    first.state_mut().mark_processed(42);
    first.apply_trade(&trade("100", 61_000));
    let first_update = first.apply_trade(&trade("101", 121_000));

    let mut replay = CandleProcessor::new(vec![one_minute()]);
    replay.state_mut().mark_processed(42);
    replay.apply_trade(&trade("100", 61_000));
    let replay_update = replay.apply_trade(&trade("101", 121_000));

    assert_eq!(
        first_update.closed[0].revision,
        replay_update.closed[0].revision
    );
    assert_eq!(
        first_update.closed[0].message_id(),
        replay_update.closed[0].message_id()
    );
    assert_eq!(first_update.closed[0].revision.source_offset, 42);
}

#[test]
fn events_are_deduplicated_by_exchange_stream_and_sequence() {
    let mut processor = CandleProcessor::new(vec![one_minute()]);
    let first = processor.apply_event(&event(7, 61_000), &[trade("100", 61_000)]);
    let duplicate = processor.apply_event(&event(7, 61_000), &[trade("100", 61_000)]);
    let next = processor.apply_event(&event(8, 62_000), &[trade("101", 62_000)]);

    assert_eq!(first.open[0].market_candle.trade_count, 1);
    assert_eq!(duplicate.duplicates, 1);
    assert!(duplicate.open.is_empty());
    assert_eq!(next.duplicates, 0);
    assert_eq!(
        processor
            .state()
            .open_candle(&CandleKey::new(
                CexId::Binance,
                TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
                one_minute()
            ))
            .unwrap()
            .market_candle
            .trade_count,
        2
    );
}

#[test]
fn replaying_the_same_events_produces_identical_state() {
    let events = [
        (event(1, 61_000), trade("100", 61_000)),
        (event(2, 62_000), trade("101", 62_000)),
        (event(3, 121_000), trade("102", 121_000)),
        (event(2, 62_000), trade("101", 62_000)),
    ];

    let mut first = CandleProcessor::new(vec![one_minute()]);
    let mut second = CandleProcessor::new(vec![one_minute()]);
    for (event, trade) in &events {
        first.apply_event(event, std::slice::from_ref(trade));
    }
    for (event, trade) in &events {
        second.apply_event(event, std::slice::from_ref(trade));
    }

    assert_eq!(first.state(), second.state());
    assert_eq!(
        first.seen_sequences(CexId::Binance, "btcusdt@trade"),
        vec![SequenceRange::new(1, 3)]
    );
}

#[test]
fn trades_inside_the_allowed_lateness_update_the_older_window() {
    let mut processor = CandleProcessor::with_config(ProcessorConfig::without_retention(
        vec![one_minute()],
        DurationMilliseconds(5_000),
    ));
    processor.apply_trade(&trade("100", 61_000));
    let update = processor.apply_trade(&trade("90", 119_000));

    assert!(update.closed.is_empty());
    let candle = processor
        .state()
        .open_candle(&CandleKey::new(
            CexId::Binance,
            TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            one_minute(),
        ))
        .unwrap();
    assert_eq!(candle.market_candle.ohlcv.high, 100.0);
    assert_eq!(candle.market_candle.ohlcv.low, 90.0);
    assert_eq!(candle.market_candle.trade_count, 2);
}

#[test]
fn trades_after_allowed_lateness_correct_the_closed_candle() {
    let mut processor = CandleProcessor::with_config(ProcessorConfig::without_retention(
        vec![one_minute()],
        DurationMilliseconds(5_000),
    ));
    processor.apply_trade(&trade("100", 61_000));
    processor.flush_closed_at_or_before(TimestampMilliseconds(125_000));
    let update = processor.apply_trade(&trade("90", 61_500));

    assert_eq!(update.late_trades, 1);
    assert!(update.open.is_empty());
    assert_eq!(update.closed.len(), 1);
    assert_eq!(update.closed[0].candle.market_candle.trade_count, 2);
    assert_eq!(update.closed[0].candle.market_candle.ohlcv.low, 90.0);
}

#[test]
fn far_future_exchange_timestamps_cannot_advance_the_watermark_past_receipt() {
    let mut processor = CandleProcessor::new(vec![one_minute()]);
    let mut far_future = event(1, 9_000_000_000_000);
    far_future.received_at = TimestampMilliseconds(62_000);

    processor.apply_event(&far_future, &[]);
    assert_eq!(
        processor.state().watermark,
        Some(TimestampMilliseconds(62_000))
    );
    assert_eq!(
        processor
            .state()
            .stream_watermark(CexId::Binance, "btcusdt@trade"),
        Some(TimestampMilliseconds(62_000))
    );

    let update = processor.apply_event(&event(2, 63_000), &[trade("100", 63_000)]);
    assert_eq!(update.late_trades, 0);
    assert_eq!(update.open[0].market_candle.trade_count, 1);
}

#[test]
fn idle_flushes_do_not_push_event_time_stream_watermarks() {
    let mut processor = CandleProcessor::new(vec![one_minute()]);
    processor.apply_event(&event(1, 61_000), &[trade("100", 61_000)]);

    let update = processor.flush_expired();
    assert!(update.closed.is_empty());
    assert_eq!(
        processor.state().watermark,
        Some(TimestampMilliseconds(61_000))
    );
    assert_eq!(
        processor
            .state()
            .stream_watermark(CexId::Binance, "btcusdt@trade"),
        Some(TimestampMilliseconds(61_000))
    );

    let update = processor.apply_event(&event(2, 61_500), &[trade("105", 61_500)]);
    assert_eq!(update.late_trades, 0);
    assert!(update.closed.is_empty());
    assert_eq!(update.open.len(), 1);
    assert_eq!(update.open[0].market_candle.trade_count, 2);
}

#[test]
fn idle_flushes_never_close_windows_with_the_wall_clock() {
    let mut processor = CandleProcessor::new(vec![one_minute()]);
    processor.apply_event(&event(1, 61_000), &[trade("100", 61_000)]);

    let update = processor.flush_expired();

    assert!(update.closed.is_empty());
    assert_eq!(processor.state().open_candles_len(), 1);
    assert_eq!(
        processor.state().watermark,
        Some(TimestampMilliseconds(61_000))
    );
}

#[test]
fn stream_watermarks_do_not_close_other_pairs_candles() {
    let mut processor = CandleProcessor::new(vec![one_minute()]);
    processor.apply_event(&event(1, 61_000), &[trade("100", 61_000)]);

    let mut ethereum_event = event(2, 500_000);
    ethereum_event.stream = "ethusdt@trade".to_string();
    let mut ethereum_trade = trade("200", 500_000);
    ethereum_trade.trading_pair = TradingPair::new(AssetId::Ethereum, AssetId::TetherUSD);
    processor.apply_event(&ethereum_event, &[ethereum_trade]);

    let update = processor.apply_event(&event(3, 62_000), &[trade("101", 62_000)]);

    assert_eq!(update.late_trades, 0);
    let key = CandleKey::new(
        CexId::Binance,
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        one_minute(),
    );
    let candle = processor.state().open_candle(&key).unwrap();
    assert_eq!(candle.start_time(), TimestampMilliseconds(60_000));
    assert_eq!(candle.market_candle.trade_count, 2);
    assert!(!candle.market_candle.is_candle_closed);
}

#[test]
fn backfilled_trades_revise_closed_candles() {
    let mut processor = CandleProcessor::new(vec![one_minute()]);
    processor.state_mut().mark_processed(1);
    processor.apply_trade(&trade("100", 61_000));
    processor.state_mut().mark_processed(2);
    let original = processor.apply_trade(&trade("101", 121_000));
    processor.state_mut().mark_processed(3);
    let update = processor.apply_trade(&trade("105", 61_500));

    assert_eq!(update.late_trades, 1);
    assert!(update.open.is_empty());
    assert_eq!(update.closed.len(), 1);
    assert_eq!(
        update.closed[0].candle.start_time(),
        TimestampMilliseconds(60_000)
    );
    assert_eq!(update.closed[0].candle.market_candle.trade_count, 2);
    assert_eq!(update.closed[0].candle.market_candle.ohlcv.close, 105.0);
    assert_ne!(
        update.closed[0].message_id(),
        original.closed[0].message_id()
    );
    let key = CandleKey::new(
        CexId::Binance,
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        one_minute(),
    );
    let retained = processor
        .state()
        .closed_candle(&key, TimestampMilliseconds(60_000))
        .unwrap();
    assert_eq!(retained.market_candle.trade_count, 2);
    assert_eq!(retained.market_candle.ohlcv.close, 105.0);
}

#[test]
fn backfilled_trades_create_candles_for_windows_without_history() {
    let mut processor = CandleProcessor::new(vec![one_minute()]);
    processor.apply_trade(&trade("100", 121_000));
    let update = processor.apply_trade(&trade("90", 61_000));

    assert_eq!(update.late_trades, 1);
    assert!(update.open.is_empty());
    assert_eq!(update.closed.len(), 1);
    assert_eq!(
        update.closed[0].candle.start_time(),
        TimestampMilliseconds(60_000)
    );
    assert_eq!(update.closed[0].candle.market_candle.trade_count, 1);
}

#[test]
fn quiet_event_time_does_not_close_candles_with_the_wall_clock() {
    let mut processor = CandleProcessor::new(vec![one_minute()]);
    processor.apply_trade(&trade("100", 61_000));
    let update = processor.apply_event(&event(2, 62_000), &[]);

    assert!(update.closed.is_empty());
    assert_eq!(processor.state().open_candles_len(), 1);
}

#[test]
fn a_gap_records_an_incomplete_window_and_closes_expired_candles() {
    let mut processor = CandleProcessor::new(vec![one_minute()]);
    processor.apply_trade(&trade("100", 61_000));

    let update = processor.apply_gap(&gap(120_000));

    assert_eq!(update.closed.len(), 1);
    assert!(update.closed[0].candle.market_candle.is_candle_closed);
    assert!(update.closed[0].is_incomplete());
    assert_eq!(update.incomplete_windows.len(), 1);
    assert_eq!(
        update.incomplete_windows[0].window_start,
        TimestampMilliseconds(60_000)
    );
    assert_eq!(update.incomplete_windows[0].from_sequence, 2);
    assert_eq!(update.incomplete_windows[0].to_sequence, 2);
    // The closed window no longer needs to be tracked once the flag has
    // been carried onto the candle.
    assert!(processor.state().incomplete_windows.is_empty());
}

#[test]
fn a_gap_marks_the_open_candle_and_republishes_it() {
    let mut processor = CandleProcessor::new(vec![one_minute()]);
    processor.apply_trade(&trade("100", 61_000));

    let update = processor.apply_gap(&GapMarker {
        detected_at: TimestampMilliseconds(65_000),
        ..gap(65_000)
    });

    assert_eq!(update.open.len(), 1);
    assert!(update.open[0].is_incomplete());
    let candle = processor
        .state()
        .open_candle(&CandleKey::new(
            CexId::Binance,
            TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            one_minute(),
        ))
        .unwrap();
    assert!(candle.is_incomplete());
}

#[test]
fn a_gap_marks_a_retained_closed_candle_incomplete() {
    let mut processor = CandleProcessor::new(vec![one_minute()]);
    processor.apply_trade(&trade("100", 61_000));
    let closed = processor.apply_trade(&trade("101", 121_000));
    assert!(closed.closed[0].is_complete());

    let update = processor.apply_gap(&GapMarker {
        detected_at: TimestampMilliseconds(65_000),
        ..gap(65_000)
    });

    assert_eq!(update.closed.len(), 1);
    assert!(update.closed[0].is_incomplete());
    let key = CandleKey::new(
        CexId::Binance,
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        one_minute(),
    );
    let retained = processor
        .state()
        .closed_candle(&key, TimestampMilliseconds(60_000))
        .unwrap();
    assert!(retained.is_incomplete());
}

#[test]
fn pruned_gap_bookkeeping_does_not_grow_without_bound() {
    let mut processor = CandleProcessor::new(vec![one_minute()]);
    processor.apply_trade(&trade("100", 61_000));
    processor.apply_gap(&gap(120_000));

    assert!(processor.state().known_gaps.is_empty());
    assert!(processor.state().incomplete_windows.is_empty());
}

#[test]
fn gaps_only_mark_windows_for_their_stream_pair() {
    let mut processor = CandleProcessor::new(vec![one_minute()]);
    processor.apply_trade(&trade("100", 61_000));
    let mut ethereum_trade = trade("200", 62_000);
    ethereum_trade.trading_pair = TradingPair::new(AssetId::Ethereum, AssetId::TetherUSD);
    processor.apply_trade(&ethereum_trade);

    let update = processor.apply_gap(&gap(120_000));

    assert_eq!(update.incomplete_windows.len(), 1);
    assert_eq!(
        update.incomplete_windows[0].key.trading_pair,
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD)
    );
}

#[test]
fn open_updates_are_coalesced_by_key_keeping_the_last_value() {
    let mut processor = CandleProcessor::new(vec![one_minute()]);
    let update = processor.apply_event(
        &event(1, 62_000),
        &[trade("100", 61_000), trade("101", 61_500)],
    );

    assert_eq!(update.open.len(), 1);
    assert_eq!(update.open[0].market_candle.trade_count, 2);
    assert_eq!(update.open[0].market_candle.ohlcv.close, 101.0);
}

#[test]
fn update_includes_every_configured_interval() {
    let mut processor = CandleProcessor::new(vec![
        CandleInterval::from_label("1m").unwrap(),
        CandleInterval::from_label("5m").unwrap(),
    ]);
    let update = processor.apply_trade(&trade("100", 61_000));

    assert_eq!(update.open.len(), 2);
    assert_eq!(update.open[0].key.interval.label(), "1m");
    assert_eq!(update.open[1].key.interval.label(), "5m");
}

#[test]
fn late_trades_do_not_reopen_a_closed_candle() {
    let mut processor = CandleProcessor::new(vec![one_minute()]);
    processor.apply_trade(&trade("100", 61_000));
    processor.apply_trade(&trade("101", 121_000));
    let update = processor.apply_trade(&trade("90", 61_500));

    assert_eq!(update.late_trades, 1);
    assert!(update.open.is_empty());
    assert_eq!(update.closed.len(), 1);
    assert_eq!(processor.state().open_candles_len(), 1);
}

fn retention_config(
    intervals: Vec<CandleInterval>,
    caps: &[(CandleInterval, usize)],
    allowed_lateness: DurationMilliseconds,
    correction_cutoff: DurationMilliseconds,
) -> ProcessorConfig {
    ProcessorConfig::new(
        intervals,
        allowed_lateness,
        caps.iter().copied().collect::<BTreeMap<_, _>>(),
        correction_cutoff,
    )
}

#[test]
fn a_late_trade_inside_the_correction_cutoff_revises_the_full_candle() {
    let mut processor = CandleProcessor::with_config(retention_config(
        vec![one_minute()],
        &[(one_minute(), 10)],
        DurationMilliseconds::ZERO,
        DurationMilliseconds(100_000),
    ));
    processor.apply_trade(&trade("100", 61_000));
    processor.flush_closed_at_or_before(TimestampMilliseconds(121_000));

    let update = processor.apply_trade(&trade("90", 61_500));

    assert_eq!(update.late_trades, 1);
    assert_eq!(update.out_of_contract_trades, 0);
    assert_eq!(update.closed.len(), 1);
    assert_eq!(update.closed[0].candle.market_candle.trade_count, 2);
    assert_eq!(update.closed[0].candle.market_candle.ohlcv.low, 90.0);
}

#[test]
fn a_late_trade_outside_the_correction_cutoff_publishes_nothing() {
    let mut processor = CandleProcessor::with_config(retention_config(
        vec![one_minute()],
        &[(one_minute(), 10)],
        DurationMilliseconds::ZERO,
        DurationMilliseconds(1_000),
    ));
    processor.apply_trade(&trade("100", 61_000));
    processor.flush_closed_at_or_before(TimestampMilliseconds(200_000));

    let update = processor.apply_trade(&trade("90", 61_500));

    assert_eq!(update.late_trades, 0);
    assert_eq!(update.out_of_contract_trades, 1);
    assert!(update.closed.is_empty());
    let candle = processor
        .state()
        .closed_candle(
            &CandleKey::new(
                CexId::Binance,
                TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
                one_minute(),
            ),
            TimestampMilliseconds(60_000),
        )
        .unwrap();
    assert_eq!(candle.market_candle.trade_count, 1);
    assert_eq!(candle.market_candle.ohlcv.low, 100.0);
}

#[test]
fn a_correction_for_a_cap_evicted_window_is_rejected() {
    let mut processor = CandleProcessor::with_config(retention_config(
        vec![one_minute()],
        &[(one_minute(), 1)],
        DurationMilliseconds::ZERO,
        DurationMilliseconds(i64::MAX),
    ));
    processor.apply_trade(&trade("100", 61_000));
    processor.apply_trade(&trade("101", 121_000));
    let evicting_update = processor.apply_trade(&trade("102", 181_000));
    assert_eq!(evicting_update.closed_candle_evictions, 1);

    let update = processor.apply_trade(&trade("90", 61_500));

    assert_eq!(update.late_trades, 0);
    assert_eq!(update.out_of_contract_trades, 1);
    assert!(update.closed.is_empty());
    let key = CandleKey::new(
        CexId::Binance,
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        one_minute(),
    );
    assert!(
        processor
            .state()
            .closed_candle(&key, TimestampMilliseconds(60_000))
            .is_none()
    );
    assert!(
        processor
            .state()
            .closed_candle(&key, TimestampMilliseconds(120_000))
            .is_some()
    );
}

#[test]
fn different_intervals_apply_their_own_caps() {
    let one_minute = one_minute();
    let five_minutes = CandleInterval::from_label("5m").unwrap();
    let mut processor = CandleProcessor::with_config(retention_config(
        vec![one_minute, five_minutes],
        &[(one_minute, 1), (five_minutes, 1)],
        DurationMilliseconds::ZERO,
        DurationMilliseconds(i64::MAX),
    ));
    processor.apply_trade(&trade("100", 61_000));
    processor.apply_trade(&trade("101", 121_000));
    processor.flush_closed_at_or_before(TimestampMilliseconds(301_000));

    let update = processor.apply_trade(&trade("102", 121_500));

    assert_eq!(update.out_of_contract_trades, 0);
    let pair = TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD);
    let one_minute_key = CandleKey::new(CexId::Binance, pair, one_minute);
    let five_minute_key = CandleKey::new(CexId::Binance, pair, five_minutes);
    assert!(
        processor
            .state()
            .closed_candle(&one_minute_key, TimestampMilliseconds(60_000))
            .is_none()
    );
    assert!(
        processor
            .state()
            .closed_candle(&one_minute_key, TimestampMilliseconds(120_000))
            .is_some()
    );
    assert!(
        processor
            .state()
            .closed_candle(&five_minute_key, TimestampMilliseconds(0))
            .is_some()
    );
}

#[test]
fn a_gap_inside_the_correction_cutoff_republishes_the_retained_candle() {
    let mut processor = CandleProcessor::with_config(retention_config(
        vec![one_minute()],
        &[(one_minute(), 10)],
        DurationMilliseconds::ZERO,
        DurationMilliseconds(100_000),
    ));
    processor.apply_trade(&trade("100", 61_000));
    processor.flush_closed_at_or_before(TimestampMilliseconds(121_000));

    let update = processor.apply_gap(&gap(130_000));

    assert_eq!(update.out_of_contract_gap_windows, 0);
    assert_eq!(update.incomplete_windows.len(), 1);
    assert_eq!(update.closed.len(), 1);
    assert!(update.closed[0].candle.is_incomplete());
}

#[test]
fn a_gap_outside_the_correction_cutoff_leaves_the_candle_unchanged() {
    let mut processor = CandleProcessor::with_config(retention_config(
        vec![one_minute()],
        &[(one_minute(), 10)],
        DurationMilliseconds::ZERO,
        DurationMilliseconds(1_000),
    ));
    processor.apply_trade(&trade("100", 61_000));
    processor.flush_closed_at_or_before(TimestampMilliseconds(121_000));

    let update = processor.apply_gap(&gap(200_000));

    assert_eq!(update.out_of_contract_gap_windows, 1);
    assert!(update.incomplete_windows.is_empty());
    assert!(update.closed.is_empty());
    let candle = processor
        .state()
        .closed_candle(
            &CandleKey::new(
                CexId::Binance,
                TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
                one_minute(),
            ),
            TimestampMilliseconds(60_000),
        )
        .unwrap();
    assert!(!candle.is_incomplete());
}
