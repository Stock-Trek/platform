use crate::processor::CandleProcessor;
use crate::processor::tests::fixtures_tests::Fixtures;
use platform_market_data::CandleKey;
use platform_types::{DurationMilliseconds, TimestampMilliseconds};
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

#[test]
fn trades_inside_the_allowed_lateness_update_the_older_window() {
    let mut processor =
        CandleProcessor::with_config(crate::processor::ProcessorConfig::without_retention(
            vec![Fixtures::one_minute()],
            DurationMilliseconds(5_000),
        ));
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    let update = processor.apply_trade(&Fixtures::trade("90", 119_000));

    assert!(update.closed.is_empty());
    let candle = processor
        .state()
        .open_candle(&CandleKey::new(
            CexId::Binance,
            TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            Fixtures::one_minute(),
        ))
        .unwrap();
    assert_eq!(candle.ohlcv.high(), 100.0);
    assert_eq!(candle.ohlcv.low(), 90.0);
    assert_eq!(candle.trade_count, 2);
}

#[test]
fn trades_after_allowed_lateness_correct_the_closed_candle() {
    let mut processor =
        CandleProcessor::with_config(crate::processor::ProcessorConfig::without_retention(
            vec![Fixtures::one_minute()],
            DurationMilliseconds(5_000),
        ));
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    processor.flush_closed_at_or_before(TimestampMilliseconds(125_000));
    let update = processor.apply_trade(&Fixtures::trade("90", 61_500));

    assert_eq!(update.late_trades, 1);
    assert!(update.open.is_empty());
    assert_eq!(update.closed.len(), 1);
    assert_eq!(update.closed[0].candle().trade_count, 2);
    assert_eq!(update.closed[0].candle().ohlcv.low(), 90.0);
}

#[test]
fn backfilled_trades_revise_closed_candles() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    processor.state_mut().mark_processed(1);
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    processor.state_mut().mark_processed(2);
    let original = processor.apply_trade(&Fixtures::trade("101", 121_000));
    processor.state_mut().mark_processed(3);
    let update = processor.apply_trade(&Fixtures::trade("105", 61_500));

    assert_eq!(update.late_trades, 1);
    assert!(update.open.is_empty());
    assert_eq!(update.closed.len(), 1);
    assert_eq!(
        update.closed[0].candle().start_time(),
        TimestampMilliseconds(60_000)
    );
    assert_eq!(update.closed[0].candle().trade_count, 2);
    assert_eq!(update.closed[0].candle().ohlcv.close(), 105.0);
    assert_ne!(
        update.closed[0].message_id(),
        original.closed[0].message_id()
    );
    let key = CandleKey::new(
        CexId::Binance,
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        Fixtures::one_minute(),
    );
    let retained = processor
        .state()
        .closed_candle(&key, TimestampMilliseconds(60_000))
        .unwrap();
    assert_eq!(retained.trade_count, 2);
    assert_eq!(retained.ohlcv.close(), 105.0);
}

#[test]
fn backfilled_trades_create_candles_for_windows_without_history() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    processor.apply_trade(&Fixtures::trade("100", 121_000));
    let update = processor.apply_trade(&Fixtures::trade("90", 61_000));

    assert_eq!(update.late_trades, 1);
    assert!(update.open.is_empty());
    assert_eq!(update.closed.len(), 1);
    assert_eq!(
        update.closed[0].candle().start_time(),
        TimestampMilliseconds(60_000)
    );
    assert_eq!(update.closed[0].candle().trade_count, 1);
}

#[test]
fn late_trades_do_not_reopen_a_closed_candle() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    processor.apply_trade(&Fixtures::trade("101", 121_000));
    let update = processor.apply_trade(&Fixtures::trade("90", 61_500));

    assert_eq!(update.late_trades, 1);
    assert!(update.open.is_empty());
    assert_eq!(update.closed.len(), 1);
    assert_eq!(processor.state().open_candles_len(), 1);
}
