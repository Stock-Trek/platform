use crate::processor::CandleProcessor;
use crate::processor::tests::fixtures_tests::Fixtures;
use platform_market_data::CandleKey;
use platform_market_data::GapMarker;
use platform_types::TimestampMilliseconds;
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

#[test]
fn a_gap_records_an_incomplete_window_and_closes_expired_candles() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    processor.apply_trade(&Fixtures::trade("100", 61_000));

    let update = processor.apply_gap(&Fixtures::gap(120_000));

    assert_eq!(update.closed.len(), 1);
    assert!(update.closed[0].candle().is_candle_closed);
    assert!(update.closed[0].is_incomplete());
    assert_eq!(update.incomplete_windows.len(), 1);
    assert_eq!(
        update.incomplete_windows[0].window_start,
        TimestampMilliseconds(60_000)
    );
    assert_eq!(update.incomplete_windows[0].from_sequence, 2);
    assert_eq!(update.incomplete_windows[0].to_sequence, 2);
    assert_eq!(processor.state().incomplete_windows.len(), 1);
}

#[test]
fn a_gap_marks_the_open_candle_and_republishes_it() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    processor.apply_trade(&Fixtures::trade("100", 61_000));

    let update = processor.apply_gap(&GapMarker {
        detected_at: TimestampMilliseconds(65_000),
        ..Fixtures::gap(65_000)
    });

    assert_eq!(update.open.len(), 1);
    assert!(update.open[0].is_incomplete());
    let candle = processor
        .state()
        .open_candle(&CandleKey::new(
            CexId::Binance,
            TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            Fixtures::one_minute(),
        ))
        .unwrap();
    assert!(candle.is_incomplete());
}

#[test]
fn a_gap_marks_a_retained_closed_candle_incomplete() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    let closed = processor.apply_trade(&Fixtures::trade("101", 121_000));
    assert!(closed.closed[0].is_complete());

    let update = processor.apply_gap(&GapMarker {
        detected_at: TimestampMilliseconds(65_000),
        ..Fixtures::gap(65_000)
    });

    assert_eq!(update.closed.len(), 1);
    assert!(update.closed[0].is_incomplete());
    let key = CandleKey::new(
        CexId::Binance,
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        Fixtures::one_minute(),
    );
    let retained = processor
        .state()
        .closed_candle(&key, TimestampMilliseconds(60_000))
        .unwrap();
    assert!(retained.is_incomplete());
}

#[test]
fn pruned_gap_bookkeeping_keeps_windows_for_retained_candles() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    processor.apply_gap(&Fixtures::gap(120_000));

    assert!(processor.state().known_gaps.is_empty());
    assert_eq!(processor.state().incomplete_windows.len(), 1);
}

#[test]
fn gaps_only_mark_windows_for_their_stream_pair() {
    let mut processor = CandleProcessor::new(vec![Fixtures::one_minute()]);
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    let mut ethereum_trade = Fixtures::trade("200", 62_000);
    ethereum_trade.trading_pair = TradingPair::new(AssetId::Ethereum, AssetId::TetherUSD);
    processor.apply_trade(&ethereum_trade);

    let update = processor.apply_gap(&Fixtures::gap(120_000));

    assert_eq!(update.incomplete_windows.len(), 1);
    assert_eq!(
        update.incomplete_windows[0].key.trading_pair,
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD)
    );
}
