use super::*;
use crate::decoders::trade::Trade;
use platform_types::new_types::TimestampMilliseconds;
use rust_decimal::Decimal;
use stock_trek_types::cex::asset_id::AssetId;
use stock_trek_types::cex::cex_id::CexId;
use stock_trek_types::cex::trading_pair::TradingPair;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str_exact(value).unwrap()
}

fn trade(price: &str, quantity: &str, timestamp: i64) -> Trade {
    Trade {
        exchange: CexId::Binance,
        trading_pair: TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        price: decimal(price),
        quantity: decimal(quantity),
        timestamp: TimestampMilliseconds(timestamp),
    }
}

#[test]
fn intervals_are_parsed_and_canonicalized() {
    let interval = CandleInterval::from_label("60s").unwrap();
    assert_eq!(interval.label(), "1m");
    assert_eq!(interval.duration().0, 60_000);
    assert_eq!(
        interval.start_for(TimestampMilliseconds(61_234)).unwrap().0,
        60_000
    );
    assert_eq!(
        interval.end_for(TimestampMilliseconds(61_234)).unwrap().0,
        120_000
    );
}

#[test]
fn invalid_intervals_are_rejected() {
    assert!(CandleInterval::from_label("").is_err());
    assert!(CandleInterval::from_label("0s").is_err());
    assert!(CandleInterval::from_label("45s").is_err());
    assert!(CandleInterval::from_label("abc").is_err());
}

#[test]
fn aligned_window_intervals_have_unique_labels() {
    let intervals = CandleInterval::all();
    let labels = intervals
        .iter()
        .map(|interval| interval.label())
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(labels.len(), intervals.len());
    assert!(labels.contains("5s"));
    assert!(labels.contains("1m"));
    assert!(labels.contains("1h"));
    assert!(labels.contains("1d"));
    assert!(labels.contains("7d"));
    assert!(labels.contains("1mo"));
    assert!(labels.contains("1y"));
}

#[test]
fn calendar_intervals_use_calendar_bounds() {
    let interval = CandleInterval::from_label("1mo").unwrap();

    assert_eq!(interval.label(), "1mo");
    assert_eq!(
        interval
            .start_for(TimestampMilliseconds(1_704_067_200_000))
            .unwrap()
            .0,
        1_704_067_200_000
    );
    assert_eq!(
        interval
            .end_for(TimestampMilliseconds(1_704_067_200_000))
            .unwrap()
            .0,
        1_706_745_600_000
    );
}

#[test]
fn negative_timestamps_are_rejected() {
    let interval = CandleInterval::from_label("1m").unwrap();

    assert!(interval.start_for(TimestampMilliseconds(-1)).is_err());
    assert!(interval.end_for(TimestampMilliseconds(-1)).is_err());
    assert!(Candle::open(&trade("100", "1", -1), interval).is_err());
}

#[test]
fn timestamp_bounds_that_overflow_are_rejected() {
    let minute = CandleInterval::from_label("1m").unwrap();
    let month = CandleInterval::from_label("1mo").unwrap();

    assert!(minute.end_for(TimestampMilliseconds(i64::MAX)).is_err());
    assert!(month.start_for(TimestampMilliseconds(i64::MAX)).is_err());
    assert!(Candle::open(&trade("100", "1", i64::MAX), minute).is_err());
    assert!(Candle::open(&trade("100", "1", i64::MAX), month).is_err());
}

#[test]
fn invalid_candle_timestamps_cannot_be_deserialized() {
    let candle = Candle::open(
        &trade("100", "1", 61_000),
        CandleInterval::from_label("1m").unwrap(),
    )
    .unwrap();

    let mut out_of_range = serde_json::to_value(&candle).unwrap();
    out_of_range["market_candle"]["end_time_millis_exc"] = u64::MAX.into();
    assert!(serde_json::from_value::<Candle>(out_of_range).is_err());

    let mut reversed = serde_json::to_value(&candle).unwrap();
    reversed["market_candle"]["end_time_millis_exc"] = 0.into();
    assert!(serde_json::from_value::<Candle>(reversed).is_err());
}

#[test]
fn applying_trades_updates_ohlcv() {
    let interval = CandleInterval::from_label("1m").unwrap();
    let mut candle = Candle::open(&trade("100", "2", 61_000), interval).unwrap();
    candle.apply_trade(&trade("110", "1", 62_000)).unwrap();
    candle.apply_trade(&trade("90", "3", 63_000)).unwrap();

    let ohlcv = &candle.market_candle.ohlcv;
    assert_eq!(ohlcv.open, 100.0);
    assert_eq!(ohlcv.high, 110.0);
    assert_eq!(ohlcv.low, 90.0);
    assert_eq!(ohlcv.close, 90.0);
    assert_eq!(ohlcv.volume, 6.0);
    assert_eq!(ohlcv.quote_volume, 580.0);
    assert!((ohlcv.vwap - 96.666_666_666_666_67).abs() < f64::EPSILON);
    assert_eq!(candle.market_candle.trade_count, 3);
}

#[test]
fn candles_start_complete_and_can_be_marked_incomplete() {
    let interval = CandleInterval::from_label("1m").unwrap();
    let mut candle = Candle::open(&trade("100", "2", 61_000), interval).unwrap();
    assert!(candle.is_complete());
    assert!(!candle.is_incomplete());

    candle.mark_incomplete();
    assert!(candle.is_incomplete());

    candle.mark_complete();
    assert!(candle.is_complete());
}

#[test]
fn intervals_serialize_without_losing_calendar_windows() {
    for label in ["1m", "1mo", "1y"] {
        let interval = CandleInterval::from_label(label).unwrap();
        let json = serde_json::to_string(&interval).unwrap();
        let decoded: CandleInterval = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, interval);
    }
}

#[test]
fn key_uses_asset_ids_and_canonical_interval() {
    let key = CandleKey::new(
        CexId::Binance,
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        CandleInterval::from_label("60s").unwrap(),
    );
    assert_eq!(key.subject_suffix(), "binance.bitcoin.tether_usd.1m");
}

fn closed_candle(offset: u64, epoch: u64) -> ClosedCandle {
    let interval = CandleInterval::from_label("1m").unwrap();
    let mut candle = Candle::open(&trade("100", "2", 61_000), interval).unwrap();
    candle.close();
    ClosedCandle::new(candle, TimestampMilliseconds(121_000), offset, epoch)
}

#[test]
fn closed_candle_revisions_are_deterministic() {
    let first = closed_candle(7, 0);
    let replay = closed_candle(7, 0);

    assert_eq!(first.revision, replay.revision);
    assert_eq!(first.message_id(), replay.message_id());
    assert_eq!(first.revision.window_start, TimestampMilliseconds(60_000));
    assert_eq!(first.revision.window_end, TimestampMilliseconds(120_000));
    assert_eq!(
        first.revision.close_watermark,
        TimestampMilliseconds(121_000)
    );
}

#[test]
fn closed_candle_ids_cover_key_window_epoch_and_revision() {
    let base = closed_candle(7, 0);

    let other_offset = closed_candle(8, 0);
    let other_epoch = closed_candle(7, 1);
    let mut other_key = closed_candle(7, 0);
    let other_interval = CandleInterval::from_label("5m").unwrap();
    other_key.candle.key.interval = other_interval;
    other_key.revision.key.interval = other_interval;

    assert_ne!(base.message_id(), other_offset.message_id());
    assert_ne!(base.message_id(), other_epoch.message_id());
    assert_ne!(base.message_id(), other_key.message_id());
}
