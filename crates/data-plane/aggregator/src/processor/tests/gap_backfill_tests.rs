use crate::processor::CandleProcessor;
use crate::processor::tests::fixtures_tests::Fixtures;
use platform_market_data::CandleKey;
use platform_market_data::{GapMarker, MarketEvent};
use platform_types::TimestampMilliseconds;
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

#[test]
fn a_fully_backfilled_gap_restores_closed_candle_completeness() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    let gap_update = processor.apply_gap(&Fixtures::gap(120_000));
    assert!(gap_update.closed[0].is_incomplete());

    let update = processor.apply_event(
        &Fixtures::event(2, 62_000),
        &[
            Fixtures::trade("101", 62_000),
            Fixtures::trade("102", 62_500),
        ],
    );

    assert_eq!(update.closed.len(), 1);
    assert!(update.closed[0].is_complete());
    assert!(processor.state().incomplete_windows.is_empty());
    let key = CandleKey::new(
        CexId::Binance,
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        Fixtures::one_minute(),
    );
    let retained = processor
        .state()
        .closed_candle(&key, TimestampMilliseconds(60_000))
        .unwrap();
    assert!(retained.is_complete());
}

#[test]
fn a_gap_is_only_resolved_after_every_missing_sequence_is_observed() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    processor.apply_gap(&GapMarker {
        observed_sequence: 4,
        missing_from: 2,
        missing_to: 3,
        ..Fixtures::gap(120_000)
    });

    let first = processor.apply_event(
        &Fixtures::event(2, 62_000),
        &[Fixtures::trade("101", 62_000)],
    );
    assert_eq!(first.closed.len(), 1);
    assert!(first.closed[0].is_incomplete());

    let second = processor.apply_event(
        &Fixtures::event(3, 63_000),
        &[Fixtures::trade("102", 63_000)],
    );
    assert_eq!(second.closed.len(), 1);
    assert!(second.closed[0].is_complete());
    assert!(processor.state().incomplete_windows.is_empty());
}

#[test]
fn a_gap_already_covered_by_observed_sequences_is_resolved_immediately() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    processor.apply_event(
        &Fixtures::event(1, 61_000),
        &[Fixtures::trade("100", 61_000)],
    );
    processor.apply_event(
        &Fixtures::event(2, 62_000),
        &[Fixtures::trade("101", 62_000)],
    );

    let update = processor.apply_gap(&GapMarker {
        detected_at: TimestampMilliseconds(63_000),
        ..Fixtures::gap(63_000)
    });

    assert!(update.incomplete_windows.is_empty());
    assert_eq!(update.open.len(), 1);
    assert!(update.open[0].is_complete());
    assert!(processor.state().incomplete_windows.is_empty());
}

#[test]
fn a_candle_stays_incomplete_until_every_affected_stream_is_backfilled() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    processor.apply_gap(&Fixtures::gap(120_000));
    processor.apply_gap(&GapMarker {
        stream: "btcusdt@depth".to_string(),
        detected_at: TimestampMilliseconds(120_000),
        ..Fixtures::gap(120_000)
    });
    assert_eq!(processor.state().incomplete_windows.len(), 2);

    let trade_update = processor.apply_event(
        &Fixtures::event(2, 62_000),
        &[Fixtures::trade("101", 62_000)],
    );
    assert!(trade_update.closed[0].is_incomplete());
    assert_eq!(processor.state().incomplete_windows.len(), 1);

    let depth_update = processor.apply_event(
        &MarketEvent {
            stream: "btcusdt@depth".to_string(),
            ..Fixtures::event(2, 63_000)
        },
        &[],
    );
    assert!(depth_update.closed[0].is_complete());
    assert!(processor.state().incomplete_windows.is_empty());
}

#[test]
fn a_fully_backfilled_gap_restores_open_candle_completeness() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    let gap_update = processor.apply_gap(&GapMarker {
        detected_at: TimestampMilliseconds(65_000),
        ..Fixtures::gap(65_000)
    });
    assert!(gap_update.open[0].is_incomplete());

    let update = processor.apply_event(
        &Fixtures::event(2, 66_000),
        &[Fixtures::trade("101", 66_000)],
    );

    assert_eq!(update.open.len(), 1);
    assert!(update.open[0].is_complete());
    assert!(processor.state().incomplete_windows.is_empty());
}
