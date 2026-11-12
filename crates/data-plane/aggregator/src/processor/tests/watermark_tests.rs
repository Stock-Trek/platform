use crate::processor::CandleProcessor;
use crate::processor::tests::fixtures_tests::Fixtures;
use platform_market_data::CandleKey;
use platform_types::TimestampMilliseconds;
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

#[test]
fn far_future_exchange_timestamps_cannot_advance_the_watermark_past_receipt() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    let mut far_future = Fixtures::event(1, 9_000_000_000_000);
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

    let update = processor.apply_event(
        &Fixtures::event(2, 63_000),
        &[Fixtures::trade("100", 63_000)],
    );
    assert_eq!(update.late_trades, 0);
    assert_eq!(update.open[0].trade_count, 1);
}

#[test]
fn idle_flushes_do_not_push_event_time_stream_watermarks() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    processor.apply_event(
        &Fixtures::event(1, 61_000),
        &[Fixtures::trade("100", 61_000)],
    );

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

    let update = processor.apply_event(
        &Fixtures::event(2, 61_500),
        &[Fixtures::trade("105", 61_500)],
    );
    assert_eq!(update.late_trades, 0);
    assert!(update.closed.is_empty());
    assert_eq!(update.open.len(), 1);
    assert_eq!(update.open[0].trade_count, 2);
}

#[test]
fn idle_flushes_never_close_windows_with_the_wall_clock() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    processor.apply_event(
        &Fixtures::event(1, 61_000),
        &[Fixtures::trade("100", 61_000)],
    );

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
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    processor.apply_event(
        &Fixtures::event(1, 61_000),
        &[Fixtures::trade("100", 61_000)],
    );

    let mut ethereum_event = Fixtures::event(2, 500_000);
    ethereum_event.stream = "ethusdt@trade".to_string();
    let mut ethereum_trade = Fixtures::trade("200", 500_000);
    ethereum_trade.trading_pair = TradingPair::new(AssetId::Ethereum, AssetId::TetherUSD);
    processor.apply_event(&ethereum_event, &[ethereum_trade]);

    let update = processor.apply_event(
        &Fixtures::event(3, 62_000),
        &[Fixtures::trade("101", 62_000)],
    );

    assert_eq!(update.late_trades, 0);
    let key = CandleKey::new(
        CexId::Binance,
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        Fixtures::one_minute(),
    );
    let candle = processor.state().open_candle(&key).unwrap();
    assert_eq!(candle.start_time(), TimestampMilliseconds(60_000));
    assert_eq!(candle.trade_count, 2);
    assert!(!candle.is_candle_closed);
}

#[test]
fn quiet_event_time_does_not_close_candles_with_the_wall_clock() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    let update = processor.apply_event(&Fixtures::event(2, 62_000), &[]);

    assert!(update.closed.is_empty());
    assert_eq!(processor.state().open_candles_len(), 1);
}
