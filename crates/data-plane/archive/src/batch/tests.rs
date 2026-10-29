use super::*;
use crate::{ArchiveParquet, ArchiveParquetWriter};
use bytes::Bytes;
use platform_market_data::{
    candle::{Candle, CandleInterval, CandleKey, ClosedCandle},
    event::MarketEvent,
};
use platform_types::new_types::TimestampMilliseconds;
use stock_trek::markets::{MarketCandle, MarketOhlcv};
use stock_trek_types::cex::cex_id::CexId;
use stock_trek_types::cex::{asset_id::AssetId, trading_pair::TradingPair};

fn event(sequence: u64) -> MarketEvent {
    MarketEvent {
        exchange: CexId::Binance,
        stream: "btcusdt@trade".to_string(),
        sequence: Some(sequence),
        exchange_timestamp: Some(TimestampMilliseconds(1_700_000_000_000)),
        received_at: TimestampMilliseconds(1_700_000_000_123),
        payload: "eyJlIjoidHJhZGUifQ==".to_string(),
    }
}

fn closed_candle(revision: u64) -> ClosedCandle {
    ClosedCandle {
        revision,
        candle: Candle {
            key: CandleKey::new(
                CexId::Binance,
                TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
                CandleInterval::from_label("60s").unwrap(),
            ),
            market_candle: MarketCandle::new(
                60_000,
                120_000,
                60_000,
                true,
                MarketOhlcv::new(100.0, 110.0, 90.0, 105.0, 6.0, 580.0, 96.666_666_666_666_67),
                3,
            ),
        },
    }
}

#[test]
fn batches_reject_records_of_a_different_kind() {
    let mixed = ArchiveBatch::new(
        ArchiveKind::Events,
        vec![
            ArchiveRecord::MarketEvent(event(1)),
            ArchiveRecord::ClosedCandle(closed_candle(1)),
        ],
    );
    assert!(mixed.is_err());

    let mut buffer = ArchiveBatch::new(ArchiveKind::Candles, Vec::new()).unwrap();
    assert!(buffer.push(ArchiveRecord::MarketEvent(event(1))).is_err());
    assert!(buffer.is_empty());
}

#[test]
fn decoded_batches_resolve_columns_by_name() {
    let batch = ArchiveBatch::new(
        ArchiveKind::Events,
        vec![ArchiveRecord::MarketEvent(event(42))],
    )
    .unwrap();
    let encoded = ArchiveParquet::encode(&batch).unwrap();
    let decoded = ArchiveParquet::decode(ArchiveKind::Events, Bytes::from(encoded)).unwrap();

    assert_eq!(decoded.len(), 1);
    let records = decoded.records();
    let ArchiveRecord::MarketEvent(actual) = &records[0] else {
        panic!("expected a market event");
    };
    assert_eq!(actual, &event(42));
}

#[test]
fn parquet_files_carry_iceberg_field_ids() {
    let batch = ArchiveBatch::new(
        ArchiveKind::Events,
        vec![ArchiveRecord::MarketEvent(event(42))],
    )
    .unwrap();
    let encoded = ArchiveParquet::encode(&batch).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        Bytes::from(encoded),
    )
    .unwrap();
    let field_ids = reader
        .schema()
        .fields()
        .iter()
        .map(|field| {
            field
                .metadata()
                .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
                .map(String::as_str)
        })
        .collect::<Vec<_>>();

    assert_eq!(
        field_ids,
        vec![
            Some("1"),
            Some("2"),
            Some("3"),
            Some("4"),
            Some("5"),
            Some("6")
        ]
    );
}

#[test]
fn candles_round_trip_with_named_columns() {
    let batch = ArchiveBatch::new(
        ArchiveKind::Candles,
        vec![ArchiveRecord::ClosedCandle(closed_candle(7))],
    )
    .unwrap();
    let encoded = ArchiveParquet::encode(&batch).unwrap();
    let decoded = ArchiveParquet::decode(ArchiveKind::Candles, Bytes::from(encoded)).unwrap();

    assert_eq!(decoded.len(), 1);
    let records = decoded.records();
    let ArchiveRecord::ClosedCandle(actual) = &records[0] else {
        panic!("expected a closed candle");
    };
    assert_eq!(actual.revision, 7);
    assert_eq!(actual.candle.key.trading_pair.base, AssetId::Bitcoin);
}

#[test]
fn writers_reject_batches_of_a_different_kind() {
    let batch = ArchiveBatch::new(
        ArchiveKind::Candles,
        vec![ArchiveRecord::ClosedCandle(closed_candle(7))],
    )
    .unwrap();
    let mut writer = ArchiveParquetWriter::try_new(ArchiveKind::Events, Vec::new()).unwrap();
    assert!(writer.write(&batch).is_err());
}
