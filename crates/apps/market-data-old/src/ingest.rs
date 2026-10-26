use crate::{
    parquet_writer::writer_properties,
    path::MarketDataPath,
    plan::Window,
    s3::{Bucket, S3},
    updaters::exchange_updater::ExchangeUpdater,
};
use arrow_array::{ArrayRef, Decimal128Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use aws_sdk_s3_transfer_manager::Client as TransferClient;
use platform_errors::{BpError, BpResult, DecimalScaleError};
use platform_exchange_types::klines::Kline;
use platform_metrics::market_data::MarketDataMetrics;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use parquet::arrow::ArrowWriter;
use rust_decimal::Decimal;
use std::sync::Arc;
use stock_trek_types::cex::trading_pair::TradingPair;
use tracing::warn;

/// Decimal precision and scale used for market values in Parquet files.
///
/// Prices and volumes are supplied as `Decimal`, so storing them as `f64` would
/// lose precision. `Decimal128(38, 18)` keeps enough fractional digits for
/// exchange tick sizes while leaving ample room for integer magnitudes.
const DECIMAL_PRECISION: u8 = 38;
const DECIMAL_SCALE: i8 = 18;
const DECIMAL_SCALE_U32: u32 = DECIMAL_SCALE as u32;

pub struct Ingest;

/// The result of trying to ingest one window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngestOutcome {
    /// The window was written to storage.
    Ingested,
    /// The exchange did not return a complete set of candles, so nothing was written.
    SkippedIncomplete { expected: usize, received: usize },
}

impl IngestOutcome {
    pub fn is_ingested(self) -> bool {
        matches!(self, Self::Ingested)
    }
}

pub struct WindowIngest<'a> {
    pub trading_pair: &'a TradingPair,
    pub window: Window,
    pub candles_base_path: &'a str,
    pub schema: &'a SchemaRef,
}

impl Ingest {
    pub async fn ingest_window(
        transfer_client: &TransferClient,
        bucket: &Bucket,
        updater: &dyn ExchangeUpdater,
        ingest: WindowIngest<'_>,
    ) -> BpResult<IngestOutcome> {
        let WindowIngest {
            trading_pair,
            window,
            candles_base_path,
            schema,
        } = ingest;
        let candles = match updater.klines(trading_pair, window.start, window.end).await {
            Ok(candles) => candles,
            Err(error) => {
                MarketDataMetrics::record_ingest_error(error.error_type());
                return Err(error);
            }
        };
        if !Self::candles_are_complete(&candles, window.start, window.end) {
            let expected = Self::expected_candles(window.start, window.end);
            let received = candles.len();
            MarketDataMetrics::record_ingest_gap(expected, received);
            warn!(
                "incomplete klines for {:?} between {} and {}: expected {} candles, received {}; skipping upload",
                trading_pair, window.start, window.end, expected, received
            );
            return Ok(IngestOutcome::SkippedIncomplete { expected, received });
        }
        let candles_bytes = Self::candles_to_bytes(&candles, schema)
            .inspect_err(|error| MarketDataMetrics::record_ingest_error(error.error_type()))?;
        let candles_file_path = MarketDataPath::window_file(candles_base_path, window.start);
        S3::upload_bytes(transfer_client, bucket, &candles_file_path, candles_bytes)
            .await
            .inspect_err(|error| MarketDataMetrics::record_ingest_error(error.error_type()))?;
        MarketDataMetrics::record_ingest_ingested();
        Ok(IngestOutcome::Ingested)
    }

    pub fn schema() -> SchemaRef {
        let decimal = || DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE);
        Arc::new(Schema::new(vec![
            Field::new("open_time", DataType::Int64, false),
            Field::new("open", decimal(), false),
            Field::new("high", decimal(), false),
            Field::new("low", decimal(), false),
            Field::new("close", decimal(), false),
            Field::new("volume", decimal(), false),
        ]))
    }

    fn expected_candles(start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> usize {
        (end_time - start_time).num_minutes().max(0) as usize
    }

    fn candles_are_complete(
        candles: &[Kline],
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> bool {
        if candles.len() != Self::expected_candles(start_time, end_time) {
            return false;
        }
        candles.iter().enumerate().all(|(index, kline)| {
            let expected_open_time = start_time + chrono::Duration::minutes(index as i64);
            kline.open_time.0 == expected_open_time.timestamp_millis()
        })
    }

    fn candle_field_to_decimal(field: &'static str, mut value: Decimal) -> BpResult<i128> {
        let scale_error = |value: Decimal| {
            BpError::candle_conversion(field, DecimalScaleError::new(value, DECIMAL_SCALE_U32))
        };
        let source_scale = value.scale();
        if source_scale > DECIMAL_SCALE_U32 {
            value.rescale(DECIMAL_SCALE_U32);
            if value.scale() != DECIMAL_SCALE_U32 {
                return Err(scale_error(value));
            }
            Ok(value.mantissa())
        } else {
            let factor = 10_i128.pow(DECIMAL_SCALE_U32 - source_scale);
            value
                .mantissa()
                .checked_mul(factor)
                .ok_or_else(|| scale_error(value))
        }
    }

    fn decimal_array(
        field: &'static str,
        values: impl Iterator<Item = Decimal>,
    ) -> BpResult<ArrayRef> {
        let values = values
            .map(|value| Self::candle_field_to_decimal(field, value))
            .collect::<BpResult<Vec<_>>>()?;
        let array = Decimal128Array::from_iter_values(values)
            .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)?;
        Ok(Arc::new(array))
    }

    fn candles_to_bytes(candles: &[Kline], schema: &SchemaRef) -> BpResult<Bytes> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter(candles.iter().map(|k| k.open_time.0))),
            Self::decimal_array("open", candles.iter().map(|k| k.open))?,
            Self::decimal_array("high", candles.iter().map(|k| k.high))?,
            Self::decimal_array("low", candles.iter().map(|k| k.low))?,
            Self::decimal_array("close", candles.iter().map(|k| k.close))?,
            Self::decimal_array("volume", candles.iter().map(|k| k.volume))?,
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, batch.schema(), Some(writer_properties()))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(Bytes::from(buffer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use platform_exchange_types::new_types::TimestampMilliseconds;
    use chrono::TimeZone;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn utc(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .unwrap()
    }

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str_exact(value).unwrap()
    }

    fn candle() -> Kline {
        Kline {
            open_time: TimestampMilliseconds(utc(2024, 1, 1, 0, 0).timestamp_millis()),
            close_time: TimestampMilliseconds(utc(2024, 1, 1, 0, 1).timestamp_millis()),
            open: decimal("123.456789012345678"),
            high: decimal("124.000000000000001"),
            low: decimal("122.999999999999999"),
            close: decimal("123.5"),
            volume: decimal("9876543210.12345678"),
            trades: 42,
        }
    }

    #[test]
    fn expected_candles_are_one_per_minute() {
        assert_eq!(
            Ingest::expected_candles(utc(2024, 1, 1, 0, 0), utc(2024, 1, 1, 0, 6)),
            6
        );
    }

    #[test]
    fn empty_window_expects_no_candles() {
        assert_eq!(
            Ingest::expected_candles(utc(2024, 1, 1, 0, 6), utc(2024, 1, 1, 0, 0)),
            0
        );
    }

    #[test]
    fn schema_stores_market_values_as_decimals() {
        let schema = Ingest::schema();
        for field in ["open", "high", "low", "close", "volume"] {
            assert_eq!(
                schema.field_with_name(field).unwrap().data_type(),
                &DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
            );
        }
    }

    #[test]
    fn decimal_conversion_scales_values_without_losing_precision() {
        let scaled = Ingest::candle_field_to_decimal("open", decimal("100000000000")).unwrap();
        assert_eq!(
            scaled,
            100_000_000_000_i128 * 10_i128.pow(DECIMAL_SCALE_U32)
        );
    }

    #[test]
    fn candles_are_written_as_exact_decimals() {
        let candle = candle();
        let bytes =
            Ingest::candles_to_bytes(std::slice::from_ref(&candle), &Ingest::schema()).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.into_iter().next().unwrap().unwrap();

        let decimals = |column: usize| {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            Decimal::from_i128_with_scale(array.value(0), DECIMAL_SCALE_U32)
        };

        assert_eq!(decimals(1), candle.open);
        assert_eq!(decimals(2), candle.high);
        assert_eq!(decimals(3), candle.low);
        assert_eq!(decimals(4), candle.close);
        assert_eq!(decimals(5), candle.volume);
    }
}
