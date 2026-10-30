use super::{ArchiveHour, ArchivePath};
use crate::{ArchiveKind, ArchivePartition, routing::RoutingBucket};
use chrono::{TimeZone, Utc};
use platform_market_data::candle::CandleInterval;
use platform_market_data::candle::CandleKey;
use platform_types::new_types::TimestampMilliseconds;
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

fn timestamp(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> i64 {
    Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
        .single()
        .unwrap()
        .timestamp_millis()
}

fn event_partition() -> ArchivePartition {
    ArchivePartition::Events {
        exchange: CexId::Binance,
        stream: "btc/usdt@trade".to_string(),
    }
}

fn candle_partition() -> ArchivePartition {
    ArchivePartition::Candles {
        candle: CandleKey::new(
            CexId::Binance,
            TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            CandleInterval::from_label("1h").unwrap(),
        ),
    }
}

#[test]
fn buckets_are_quantized_to_their_start() {
    let start = timestamp(2024, 1, 2, 3, 0);
    let within = TimestampMilliseconds(start + 37 * 60 * 1_000);
    assert_eq!(
        ArchivePath::bucket_start(within, 60 * 60 * 1_000),
        TimestampMilliseconds(start)
    );
    assert_eq!(
        ArchivePath::bucket_start(TimestampMilliseconds(-1), 60 * 60 * 1_000),
        TimestampMilliseconds(-60 * 60 * 1_000)
    );
    assert_eq!(
        ArchivePath::bucket_start(within, 5 * 60 * 1_000),
        TimestampMilliseconds(timestamp(2024, 1, 2, 3, 35))
    );
}

#[test]
fn archive_hours_are_quantized_to_the_top_of_the_hour() {
    let start = timestamp(2024, 1, 2, 3, 0);
    let within = TimestampMilliseconds(start + 37 * 60 * 1_000 + 123);

    assert_eq!(
        ArchiveHour::from_timestamp(within).unwrap().bucket_start(),
        TimestampMilliseconds(start)
    );
}

#[test]
fn raw_and_compacted_keys_share_the_hourly_layout() {
    let bucket = TimestampMilliseconds(timestamp(2024, 1, 2, 3, 0));
    let routing_bucket = RoutingBucket::from_index(7).unwrap();

    assert_eq!(
        ArchivePath::raw_key(
            "market-data/raw/",
            ArchiveKind::Events,
            bucket,
            routing_bucket,
            "file.parquet"
        )
        .unwrap(),
        "market-data/raw/events/bucket=00007/year=2024/month=01/day=02/hour=03/file.parquet"
    );
    assert_eq!(
        ArchivePath::compacted_key(
            "/market-data/compacted/",
            &event_partition(),
            bucket,
            routing_bucket,
            "/file.parquet"
        )
        .unwrap(),
        "market-data/compacted/kind=events/exchange=binance/stream=btc_usdt@trade/bucket=00007/year=2024/month=01/day=02/hour=03/file.parquet"
    );
    assert_eq!(
        ArchivePath::compacted_key(
            "/",
            &candle_partition(),
            bucket,
            routing_bucket,
            "data.parquet"
        )
        .unwrap(),
        "kind=candles/exchange=binance/base=bitcoin/quote=tether_usd/interval=1h/bucket=00007/year=2024/month=01/day=02/hour=03/data.parquet"
    );
}

#[test]
fn keys_parse_back_to_the_same_bucket() {
    let bucket = TimestampMilliseconds(timestamp(2024, 1, 2, 3, 0));
    let routing_bucket = RoutingBucket::from_index(42).unwrap();
    let raw = ArchivePath::raw_key(
        "market-data/raw",
        ArchiveKind::Candles,
        bucket,
        routing_bucket,
        "file.parquet",
    )
    .unwrap();
    let compacted = ArchivePath::compacted_key(
        "market-data/compacted",
        &candle_partition(),
        bucket,
        routing_bucket,
        "data.parquet",
    )
    .unwrap();

    for key in [raw, compacted] {
        let parsed = ArchivePath::parse_key(&key).unwrap();
        assert_eq!(parsed.kind, ArchiveKind::Candles);
        assert_eq!(parsed.bucket_start, bucket);
        assert_eq!(
            ArchivePath::routing_bucket_from_key(&key),
            Some(routing_bucket)
        );
        assert_eq!(
            ArchiveHour::parse(&key).unwrap().path(),
            "year=2024/month=01/day=02/hour=03"
        );
    }
}

#[test]
fn keys_can_be_made_relative_to_a_configured_prefix() {
    let key = "market-data/raw/events/year=2024/month=01/day=02/hour=03/file.parquet";
    let relative = ArchivePath::relative_key(key, "market-data/raw/").unwrap();
    assert_eq!(
        relative,
        "events/year=2024/month=01/day=02/hour=03/file.parquet"
    );
    assert_eq!(ArchivePath::relative_key(key, "").unwrap(), key);
    assert_eq!(
        ArchivePath::relative_key("other/key", "market-data/raw"),
        None
    );
}

#[test]
fn invalid_hourly_paths_are_rejected() {
    assert!(ArchiveHour::parse("events/year=2024/month=13/day=01/hour=03").is_none());
    assert!(ArchiveHour::parse("events/year=2024/month=01/day=32/hour=03").is_none());
    assert!(ArchiveHour::parse("events/year=2024/month=01/day=01/hour=24").is_none());
    assert!(ArchiveHour::parse("events/year=2024/month=01/day=01").is_none());
}

#[test]
fn compaction_targets_are_normalized_to_hour_prefixes() {
    assert_eq!(
        ArchivePath::compaction_prefix("market-data/raw/events/year=2024/month=01/day=02/hour=03/")
            .unwrap(),
        "market-data/raw/events/year=2024/month=01/day=02/hour=03/"
    );
    assert_eq!(
        ArchivePath::compaction_prefix(
            "market-data/compacted/kind=candles/exchange=binance/base=bitcoin/quote=tether_usd/interval=1h/year=2024/month=01/day=02/hour=03/data.parquet"
        )
        .unwrap(),
        "market-data/compacted/kind=candles/exchange=binance/base=bitcoin/quote=tether_usd/interval=1h/year=2024/month=01/day=02/hour=03/"
    );
}

#[test]
fn invalid_compaction_targets_are_rejected() {
    assert!(ArchivePath::compaction_prefix("").is_err());
    assert!(
        ArchivePath::compaction_prefix("market-data/raw/events/year=2024/month=01/day=02").is_err()
    );
    assert!(
        ArchivePath::compaction_prefix(
            "market-data/raw/unknown/year=2024/month=01/day=02/hour=03/"
        )
        .is_err()
    );
    assert!(
        ArchivePath::compaction_prefix(
            "market-data/raw/events/../../year=2024/month=01/day=02/hour=03/"
        )
        .is_err()
    );
}
