use crate::processor::CandleProcessor;
use crate::processor::tests::fixtures_tests::Fixtures;
use platform_market_data::GapMarker;
use platform_market_data::{AlignedWindow, CandleKey};
use platform_types::{DurationMilliseconds, TimestampMilliseconds};
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

#[test]
fn gap_affected_updates_are_ordered_across_keys_intervals_and_windows() {
    let one_minute = Fixtures::one_minute();
    let five_minutes = AlignedWindow::try_from("5m").unwrap();
    let pairs = [
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        TradingPair::new(AssetId::Ethereum, AssetId::TetherUSD),
    ];
    let config = crate::processor::ProcessorConfig::without_retention(
        vec![one_minute, five_minutes],
        DurationMilliseconds::ZERO,
    );

    let mut first = CandleProcessor::with_config(config.clone());
    let mut second = CandleProcessor::with_config(config);
    for pair in pairs {
        for timestamp in [61_000, 121_000, 181_000] {
            first.apply_trade(&Fixtures::trade_for(pair, "100", timestamp));
            second.apply_trade(&Fixtures::trade_for(pair, "100", timestamp));
        }
    }
    first.flush_closed_at_or_before(TimestampMilliseconds(200_000));
    second.flush_closed_at_or_before(TimestampMilliseconds(200_000));

    let all_pairs_gap = GapMarker {
        stream: "all@trades".to_string(),
        detected_at: TimestampMilliseconds(190_000),
        ..Fixtures::gap(190_000)
    };
    let first_update = first.apply_gap(&all_pairs_gap);
    let second_update = second.apply_gap(&all_pairs_gap);

    assert_eq!(first_update.open, second_update.open);
    assert_eq!(
        first_update
            .closed
            .iter()
            .map(|candle| candle.message_id())
            .collect::<Vec<_>>(),
        second_update
            .closed
            .iter()
            .map(|candle| candle.message_id())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        first_update.incomplete_windows,
        second_update.incomplete_windows
    );
    assert_eq!(first.state(), second.state());

    let subject = |pair, interval| CandleKey::new(CexId::Binance, pair, interval).subject_suffix();
    let closed_order = first_update
        .closed
        .iter()
        .map(|candle| {
            (
                candle.candle().key.subject_suffix(),
                candle.candle().start_time().0,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        closed_order,
        vec![
            (subject(pairs[0], one_minute), 60_000),
            (subject(pairs[0], one_minute), 120_000),
            (subject(pairs[1], one_minute), 60_000),
            (subject(pairs[1], one_minute), 120_000),
        ]
    );

    let open_order = first_update
        .open
        .iter()
        .map(|candle| (candle.key.subject_suffix(), candle.start_time().0))
        .collect::<Vec<_>>();
    assert_eq!(
        open_order,
        vec![
            (subject(pairs[0], one_minute), 180_000),
            (subject(pairs[0], five_minutes), 0),
            (subject(pairs[1], one_minute), 180_000),
            (subject(pairs[1], five_minutes), 0),
        ]
    );

    let affected_order = first_update
        .incomplete_windows
        .iter()
        .map(|window| (window.key.subject_suffix(), window.window_start.0))
        .collect::<Vec<_>>();
    assert_eq!(
        affected_order,
        vec![
            (subject(pairs[0], one_minute), 60_000),
            (subject(pairs[0], one_minute), 120_000),
            (subject(pairs[0], one_minute), 180_000),
            (subject(pairs[0], five_minutes), 0),
            (subject(pairs[1], one_minute), 60_000),
            (subject(pairs[1], one_minute), 120_000),
            (subject(pairs[1], one_minute), 180_000),
            (subject(pairs[1], five_minutes), 0),
        ]
    );
}
