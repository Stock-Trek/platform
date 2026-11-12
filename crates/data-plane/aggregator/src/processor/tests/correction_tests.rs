use crate::processor::CandleProcessor;
use crate::processor::tests::fixtures_tests::Fixtures;
use platform_market_data::CandleKey;
use platform_types::{DurationMilliseconds, TimestampMilliseconds};
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

#[test]
fn a_late_trade_inside_the_correction_cutoff_revises_the_full_candle() {
    let mut processor = CandleProcessor::with_config(Fixtures::retention_config(
        vec![Fixtures::one_minute()],
        &[(Fixtures::one_minute(), 10)],
        DurationMilliseconds::ZERO,
        DurationMilliseconds(100_000),
    ));
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    processor.flush_closed_at_or_before(TimestampMilliseconds(121_000));

    let update = processor.apply_trade(&Fixtures::trade("90", 61_500));

    assert_eq!(update.late_trades, 1);
    assert_eq!(update.out_of_contract_trades, 0);
    assert_eq!(update.closed.len(), 1);
    assert_eq!(update.closed[0].candle().trade_count, 2);
    assert_eq!(update.closed[0].candle().ohlcv.low(), 90.0);
}

#[test]
fn a_late_trade_outside_the_correction_cutoff_publishes_nothing() {
    let mut processor = CandleProcessor::with_config(Fixtures::retention_config(
        vec![Fixtures::one_minute()],
        &[(Fixtures::one_minute(), 10)],
        DurationMilliseconds::ZERO,
        DurationMilliseconds(1_000),
    ));
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    processor.flush_closed_at_or_before(TimestampMilliseconds(200_000));

    let update = processor.apply_trade(&Fixtures::trade("90", 61_500));

    assert_eq!(update.late_trades, 0);
    assert_eq!(update.out_of_contract_trades, 1);
    assert!(update.closed.is_empty());
    let candle = processor
        .state()
        .closed_candle(
            &CandleKey::new(
                CexId::Binance,
                TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
                Fixtures::one_minute(),
            ),
            TimestampMilliseconds(60_000),
        )
        .unwrap();
    assert_eq!(candle.trade_count, 1);
    assert_eq!(candle.ohlcv.low(), 100.0);
}

#[test]
fn a_gap_inside_the_correction_cutoff_republishes_the_retained_candle() {
    let mut processor = CandleProcessor::with_config(Fixtures::retention_config(
        vec![Fixtures::one_minute()],
        &[(Fixtures::one_minute(), 10)],
        DurationMilliseconds::ZERO,
        DurationMilliseconds(100_000),
    ));
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    processor.flush_closed_at_or_before(TimestampMilliseconds(121_000));

    let update = processor.apply_gap(&Fixtures::gap(130_000));

    assert_eq!(update.out_of_contract_gap_windows, 0);
    assert_eq!(update.incomplete_windows.len(), 1);
    assert_eq!(update.closed.len(), 1);
    assert!(update.closed[0].candle().is_incomplete());
}

#[test]
fn a_gap_outside_the_correction_cutoff_leaves_the_candle_unchanged() {
    let mut processor = CandleProcessor::with_config(Fixtures::retention_config(
        vec![Fixtures::one_minute()],
        &[(Fixtures::one_minute(), 10)],
        DurationMilliseconds::ZERO,
        DurationMilliseconds(1_000),
    ));
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    processor.flush_closed_at_or_before(TimestampMilliseconds(121_000));

    let update = processor.apply_gap(&Fixtures::gap(200_000));

    assert_eq!(update.out_of_contract_gap_windows, 1);
    assert!(update.incomplete_windows.is_empty());
    assert!(update.closed.is_empty());
    let candle = processor
        .state()
        .closed_candle(
            &CandleKey::new(
                CexId::Binance,
                TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
                Fixtures::one_minute(),
            ),
            TimestampMilliseconds(60_000),
        )
        .unwrap();
    assert!(!candle.is_incomplete());
}
