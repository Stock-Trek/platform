use super::*;
use crate::decoders::trade::Trade;
use platform_types::new_types::TimestampMilliseconds;
use rust_decimal::Decimal;
use stock_trek::markets::{AlignedWindow, TimestampMillis};
use stock_trek_types::cex::asset_id::AssetId;
use stock_trek_types::cex::cex_id::CexId;
use stock_trek_types::cex::trading_pair::TradingPair;
use strum::IntoEnumIterator;

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
fn intervals_are_parsed_and_aligned() {
    let interval = AlignedWindow::try_from("1m").unwrap();
    assert_eq!(interval, AlignedWindow::Minutes1);
    assert_eq!(interval.label(), "1m");
    assert_eq!(interval.millis().exact(), Some(60_000));
    let bounds = interval.bounds_for(TimestampMillis::new(61_234)).unwrap();
    assert_eq!(bounds.lower().as_millis(), 60_000);
    assert_eq!(bounds.upper().as_millis(), 120_000);
}

#[test]
fn invalid_intervals_are_rejected() {
    assert!(AlignedWindow::try_from("").is_err());
    assert!(AlignedWindow::try_from("0s").is_err());
    assert!(AlignedWindow::try_from("45s").is_err());
    assert!(AlignedWindow::try_from("abc").is_err());
}

#[test]
fn non_canonical_interval_labels_are_rejected() {
    assert!(AlignedWindow::try_from("minutes1").is_err());
    assert!(AlignedWindow::try_from("120m").is_err());
    assert!(AlignedWindow::try_from("14d").is_err());
}

#[test]
fn every_aligned_window_is_supported() {
    for window in AlignedWindow::iter() {
        assert!(
            window.millis().exact().is_some() || window.months_exact().is_some(),
            "unsupported AlignedWindow variant {window:?}"
        );
        assert!(
            window.bounds_for(TimestampMillis::new(0)).is_ok(),
            "unsupported AlignedWindow variant {window:?}"
        );
    }
}

#[test]
fn aligned_window_intervals_have_unique_labels() {
    let intervals = all_intervals();
    let labels = intervals
        .iter()
        .map(|interval| interval.label())
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(labels.len(), intervals.len());
    assert!(labels.contains("5s"));
    assert!(labels.contains("1m"));
    assert!(labels.contains("1h"));
    assert!(labels.contains("1d"));
    assert!(labels.contains("1w"));
    assert!(labels.contains("1mo"));
    assert!(labels.contains("1y"));
}

#[test]
fn calendar_intervals_use_calendar_bounds() {
    let interval = AlignedWindow::try_from("1mo").unwrap();

    assert_eq!(interval.label(), "1mo");
    let bounds = interval
        .bounds_for(TimestampMillis::new(1_704_067_200_000))
        .unwrap();
    assert_eq!(bounds.lower().as_millis(), 1_704_067_200_000);
    assert_eq!(bounds.upper().as_millis(), 1_706_745_600_000);
}

#[test]
fn negative_candle_timestamps_are_rejected() {
    let interval = AlignedWindow::try_from("1m").unwrap();

    assert!(Candle::open(&trade("100", "1", -1), interval).is_err());
}

#[test]
fn timestamp_bounds_that_overflow_are_rejected() {
    let minute = AlignedWindow::try_from("1m").unwrap();
    let month = AlignedWindow::try_from("1mo").unwrap();

    let max_timestamp = TimestampMillis::new(u64::MAX);

    assert!(minute.bounds_for(max_timestamp).is_err());
    assert!(month.bounds_for(max_timestamp).is_err());
}

#[test]
fn invalid_candle_timestamps_cannot_be_deserialized() {
    let candle = Candle::open(
        &trade("100", "1", 61_000),
        AlignedWindow::try_from("1m").unwrap(),
    )
    .unwrap();

    let mut out_of_range = serde_json::to_value(&candle).unwrap();
    out_of_range["end_time_millis_exc"] = u64::MAX.into();
    assert!(serde_json::from_value::<Candle>(out_of_range).is_err());

    let mut reversed = serde_json::to_value(&candle).unwrap();
    reversed["end_time_millis_exc"] = 0.into();
    assert!(serde_json::from_value::<Candle>(reversed).is_err());
}

#[test]
fn applying_trades_updates_ohlcv() {
    let interval = AlignedWindow::try_from("1m").unwrap();
    let mut candle = Candle::open(&trade("100", "2", 61_000), interval).unwrap();
    candle.apply_trade(&trade("110", "1", 62_000)).unwrap();
    candle.apply_trade(&trade("90", "3", 63_000)).unwrap();

    let ohlcv = &candle.ohlcv;
    assert_eq!(ohlcv.open, 100.0);
    assert_eq!(ohlcv.high, 110.0);
    assert_eq!(ohlcv.low, 90.0);
    assert_eq!(ohlcv.close, 90.0);
    assert_eq!(ohlcv.volume, 6.0);
    assert_eq!(ohlcv.quote_volume, 580.0);
    assert!((ohlcv.vwap - 96.666_666_666_666_67).abs() < f64::EPSILON);
    assert_eq!(candle.trade_count, 3);
}

#[test]
fn candles_start_complete_and_can_be_marked_incomplete() {
    let interval = AlignedWindow::try_from("1m").unwrap();
    let mut candle = Candle::open(&trade("100", "2", 61_000), interval).unwrap();
    assert!(candle.is_complete());
    assert!(!candle.is_incomplete());

    candle.mark_incomplete();
    assert!(candle.is_incomplete());

    candle.mark_complete();
    assert!(candle.is_complete());
}

#[test]
fn candle_keys_serialize_intervals_by_label() {
    for label in ["5s", "1m", "1mo", "1y"] {
        let key = CandleKey::new(
            CexId::Binance,
            TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            AlignedWindow::try_from(label).unwrap(),
        );
        let json = serde_json::to_string(&key).unwrap();
        assert!(json.contains(&format!("\"interval\":\"{label}\"")));
        let decoded: CandleKey = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, key);
    }
}

#[test]
fn candle_keys_reject_non_label_intervals() {
    let key = CandleKey::new(
        CexId::Binance,
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        AlignedWindow::try_from("1m").unwrap(),
    );
    let mut value = serde_json::to_value(key).unwrap();
    value["interval"] = serde_json::json!(60_000);

    assert!(serde_json::from_value::<CandleKey>(value).is_err());
}

#[test]
fn key_uses_asset_ids_and_window_label() {
    let key = CandleKey::new(
        CexId::Binance,
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        AlignedWindow::try_from("1m").unwrap(),
    );
    assert_eq!(key.subject_suffix(), "binance.bitcoin.tether_usd.1m");
}

fn closed_candle(offset: u64, epoch: u64) -> ClosedCandle {
    let interval = AlignedWindow::try_from("1m").unwrap();
    let mut candle = Candle::open(&trade("100", "2", 61_000), interval).unwrap();
    candle.close();
    ClosedCandle::new(candle, TimestampMilliseconds(121_000), offset, epoch)
}

#[test]
fn closed_candle_revisions_are_deterministic() {
    let first = closed_candle(7, 0);
    let replay = closed_candle(7, 0);

    assert_eq!(first.revision(), replay.revision());
    assert_eq!(first.message_id(), replay.message_id());
    assert_eq!(first.revision().window_start, TimestampMilliseconds(60_000));
    assert_eq!(first.revision().window_end, TimestampMilliseconds(120_000));
    assert_eq!(
        first.revision().close_watermark,
        TimestampMilliseconds(121_000)
    );
}

#[test]
fn closed_candle_ids_cover_key_window_epoch_and_revision() {
    let base = closed_candle(7, 0);

    let other_offset = closed_candle(8, 0);
    let other_epoch = closed_candle(7, 1);
    let other_key = {
        let interval = AlignedWindow::try_from("5m").unwrap();
        let mut candle = Candle::open(&trade("100", "2", 61_000), interval).unwrap();
        candle.close();
        ClosedCandle::new(candle, TimestampMilliseconds(121_000), 7, 0)
    };

    assert_ne!(base.message_id(), other_offset.message_id());
    assert_ne!(base.message_id(), other_epoch.message_id());
    assert_ne!(base.message_id(), other_key.message_id());
}
