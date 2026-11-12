use crate::processor::tests::fixtures_tests::Fixtures;
use crate::processor::{CandleProcessor, ProcessorConfig};
use crate::state::{AggregatorState, SequenceRange};
use platform_market_data::{AlignedWindow, CandleKey};
use platform_types::{DurationMilliseconds, TimestampMilliseconds};
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

#[test]
fn configured_schema_version_is_used_for_new_and_restored_state() {
    let config = ProcessorConfig::without_retention(
        vec![Fixtures::one_minute()],
        DurationMilliseconds::ZERO,
    )
    .with_state_schema_version(9);

    let processor = CandleProcessor::with_config(config.clone());
    assert_eq!(processor.state().schema_version, 9);

    let restored = CandleProcessor::from_state(config.clone(), processor.into_state()).unwrap();
    assert_eq!(restored.state().schema_version, 9);
    assert!(CandleProcessor::from_state(config, AggregatorState::new()).is_err());
}

#[test]
fn first_trade_opens_a_candle() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    let update = processor.apply_trade(&Fixtures::trade("100", 61_000));

    assert!(update.closed.is_empty());
    assert_eq!(update.open.len(), 1);
    assert_eq!(update.open[0].start_time(), TimestampMilliseconds(60_000));
    assert_eq!(update.open[0].ohlcv.open(), 100.0);
    assert!(!update.open[0].is_candle_closed);
}

#[test]
fn invalid_timestamps_are_rejected_without_mutating_state() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);

    for timestamp in [-1, i64::MAX] {
        let update = processor.apply_trade(&Fixtures::trade("100", timestamp));

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
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    let update = processor.apply_trade(&Fixtures::trade("101", 121_000));

    assert_eq!(update.closed.len(), 1);
    assert_eq!(
        update.closed[0].revision().close_watermark,
        TimestampMilliseconds(121_000)
    );
    assert_eq!(update.closed[0].revision().source_offset, 0);
    assert_eq!(update.closed[0].revision().state_epoch, 0);
    assert!(update.closed[0].candle().is_candle_closed);
    assert_eq!(
        update.closed[0].candle().start_time(),
        TimestampMilliseconds(60_000)
    );
    assert_eq!(update.open.len(), 1);
    assert_eq!(update.open[0].start_time(), TimestampMilliseconds(120_000));
}

#[test]
fn closed_candle_revisions_are_reproducible_from_state() {
    let mut first = CandleProcessor::new(vec![Fixtures::one_minute()]);
    first.state_mut().mark_processed(42);
    first.apply_trade(&Fixtures::trade("100", 61_000));
    let first_update = first.apply_trade(&Fixtures::trade("101", 121_000));

    let mut replay = CandleProcessor::new(vec![Fixtures::one_minute()]);
    replay.state_mut().mark_processed(42);
    replay.apply_trade(&Fixtures::trade("100", 61_000));
    let replay_update = replay.apply_trade(&Fixtures::trade("101", 121_000));

    assert_eq!(
        first_update.closed[0].revision(),
        replay_update.closed[0].revision()
    );
    assert_eq!(
        first_update.closed[0].message_id(),
        replay_update.closed[0].message_id()
    );
    assert_eq!(first_update.closed[0].revision().source_offset, 42);
}

#[test]
fn events_are_deduplicated_by_exchange_stream_and_sequence() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    let first = processor.apply_event(
        &Fixtures::event(7, 61_000),
        &[Fixtures::trade("100", 61_000)],
    );
    let duplicate = processor.apply_event(
        &Fixtures::event(7, 61_000),
        &[Fixtures::trade("100", 61_000)],
    );
    let next = processor.apply_event(
        &Fixtures::event(8, 62_000),
        &[Fixtures::trade("101", 62_000)],
    );

    assert_eq!(first.open[0].trade_count, 1);
    assert_eq!(duplicate.duplicates, 1);
    assert!(duplicate.open.is_empty());
    assert_eq!(next.duplicates, 0);
    assert_eq!(
        processor
            .state()
            .open_candle(&CandleKey::new(
                CexId::Binance,
                TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
                Fixtures::one_minute()
            ))
            .unwrap()
            .trade_count,
        2
    );
}

#[test]
fn replaying_the_same_events_produces_identical_state() {
    let events = [
        (Fixtures::event(1, 61_000), Fixtures::trade("100", 61_000)),
        (Fixtures::event(2, 62_000), Fixtures::trade("101", 62_000)),
        (Fixtures::event(3, 121_000), Fixtures::trade("102", 121_000)),
        (Fixtures::event(2, 62_000), Fixtures::trade("101", 62_000)),
    ];

    let mut first = CandleProcessor::new(vec![Fixtures::one_minute()]);
    let mut second = CandleProcessor::new(vec![Fixtures::one_minute()]);
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
fn open_updates_are_coalesced_by_key_keeping_the_last_value() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    let update = processor.apply_event(
        &Fixtures::event(1, 62_000),
        &[
            Fixtures::trade("100", 61_000),
            Fixtures::trade("101", 61_500),
        ],
    );

    assert_eq!(update.open.len(), 1);
    assert_eq!(update.open[0].trade_count, 2);
    assert_eq!(update.open[0].ohlcv.close(), 101.0);
}

#[test]
fn update_includes_every_configured_interval() {
    let mut processor = CandleProcessor::new(vec![
        AlignedWindow::try_from("1m").unwrap(),
        AlignedWindow::try_from("5m").unwrap(),
    ]);
    let update = processor.apply_trade(&Fixtures::trade("100", 61_000));

    assert_eq!(update.open.len(), 2);
    assert_eq!(update.open[0].key.interval.label(), "1m");
    assert_eq!(update.open[1].key.interval.label(), "5m");
}
