use crate::processor::CandleProcessor;
use crate::processor::tests::fixtures_tests::Fixtures;
use platform_market_data::{AlignedWindow, CandleKey};
use platform_types::{DurationMilliseconds, TimestampMilliseconds};
use std::collections::BTreeMap;
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

#[test]
fn replay_with_different_caps_per_interval_and_a_cutoff_preserves_state_and_message_ids() {
    let one_minute = Fixtures::one_minute();
    let five_minutes = AlignedWindow::try_from("5m").unwrap();
    let config = Fixtures::retention_config(
        vec![one_minute, five_minutes],
        &[(one_minute, 1), (five_minutes, 2)],
        DurationMilliseconds::ZERO,
        DurationMilliseconds(60_000),
    );
    let events = [
        (Fixtures::event(1, 61_000), Fixtures::trade("100", 61_000)),
        (Fixtures::event(2, 121_000), Fixtures::trade("101", 121_000)),
        (Fixtures::event(3, 181_000), Fixtures::trade("102", 181_000)),
        (Fixtures::event(4, 241_000), Fixtures::trade("103", 241_000)),
        (Fixtures::event(5, 301_000), Fixtures::trade("104", 301_000)),
        (Fixtures::event(6, 601_000), Fixtures::trade("105", 601_000)),
        (Fixtures::event(7, 602_000), Fixtures::trade("90", 61_500)),
        (Fixtures::event(8, 605_000), Fixtures::trade("95", 301_500)),
        (Fixtures::event(9, 901_000), Fixtures::trade("106", 901_000)),
    ];

    let mut first = CandleProcessor::with_config(config.clone());
    let mut second = CandleProcessor::with_config(config);
    let mut first_ids = Vec::new();
    let mut second_ids = Vec::new();
    let mut first_evictions = 0;
    let mut second_evictions = 0;
    let mut first_out_of_contract = 0;
    let mut second_out_of_contract = 0;
    let mut evictions_by_interval = BTreeMap::new();

    for (event, trade) in &events {
        let update = first.apply_event(event, std::slice::from_ref(trade));
        first_ids.extend(update.closed.iter().map(|candle| candle.message_id()));
        first_evictions += update.closed_candle_evictions;
        first_out_of_contract += update.out_of_contract_trades;
        for (interval, count) in &update.closed_candle_evictions_by_interval {
            *evictions_by_interval.entry(*interval).or_default() += count;
        }
    }
    for (event, trade) in &events {
        let update = second.apply_event(event, std::slice::from_ref(trade));
        second_ids.extend(update.closed.iter().map(|candle| candle.message_id()));
        second_evictions += update.closed_candle_evictions;
        second_out_of_contract += update.out_of_contract_trades;
    }

    assert_eq!(first.state(), second.state());
    assert_eq!(first_ids, second_ids);
    assert_eq!(first_evictions, second_evictions);
    assert_eq!(first_evictions, 6);
    assert_eq!(
        evictions_by_interval,
        BTreeMap::from([(one_minute, 5), (five_minutes, 1)])
    );
    assert_eq!(first_out_of_contract, second_out_of_contract);
    assert_eq!(first_out_of_contract, 3);
}

#[test]
fn a_correction_for_a_cap_evicted_window_is_rejected() {
    let mut processor = CandleProcessor::with_config(Fixtures::retention_config(
        vec![Fixtures::one_minute()],
        &[(Fixtures::one_minute(), 1)],
        DurationMilliseconds::ZERO,
        DurationMilliseconds(i64::MAX),
    ));
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    processor.apply_trade(&Fixtures::trade("101", 121_000));
    let evicting_update = processor.apply_trade(&Fixtures::trade("102", 181_000));
    assert_eq!(evicting_update.closed_candle_evictions, 1);

    let update = processor.apply_trade(&Fixtures::trade("90", 61_500));

    assert_eq!(update.late_trades, 0);
    assert_eq!(update.out_of_contract_trades, 1);
    assert!(update.closed.is_empty());
    let key = CandleKey::new(
        CexId::Binance,
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        Fixtures::one_minute(),
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
    let one_minute = Fixtures::one_minute();
    let five_minutes = AlignedWindow::try_from("5m").unwrap();
    let mut processor = CandleProcessor::with_config(Fixtures::retention_config(
        vec![one_minute, five_minutes],
        &[(one_minute, 1), (five_minutes, 1)],
        DurationMilliseconds::ZERO,
        DurationMilliseconds(i64::MAX),
    ));
    processor.apply_trade(&Fixtures::trade("100", 61_000));
    processor.apply_trade(&Fixtures::trade("101", 121_000));
    processor.flush_closed_at_or_before(TimestampMilliseconds(301_000));

    let update = processor.apply_trade(&Fixtures::trade("102", 121_500));

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
