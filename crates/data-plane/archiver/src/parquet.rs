use crate::record::{ArchiveKind, ArchiveRecord};
use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use platform_errors::BpResult;
use std::sync::Arc;

/// Encodes archive records as Parquet record batches.
pub struct ArchiveParquet;

impl ArchiveParquet {
    pub fn encode(kind: ArchiveKind, records: &[ArchiveRecord]) -> BpResult<Vec<u8>> {
        let batch = match kind {
            ArchiveKind::Events => Self::event_batch(records)?,
            ArchiveKind::Candles => Self::candle_batch(records)?,
        };
        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, batch.schema(), Some(Self::writer_properties()))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(buffer)
    }

    fn event_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("exchange", DataType::Utf8, false),
            Field::new("stream", DataType::Utf8, false),
            Field::new("sequence", DataType::UInt64, true),
            Field::new("exchange_timestamp", DataType::Int64, true),
            Field::new("received_at", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]))
    }

    fn candle_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("exchange", DataType::Utf8, false),
            Field::new("base", DataType::Utf8, false),
            Field::new("quote", DataType::Utf8, false),
            Field::new("interval", DataType::Utf8, false),
            Field::new("start_time_millis_inc", DataType::Int64, false),
            Field::new("end_time_millis_exc", DataType::Int64, false),
            Field::new("duration_millis", DataType::Int64, false),
            Field::new("is_candle_closed", DataType::Boolean, false),
            Field::new("open", DataType::Float64, false),
            Field::new("high", DataType::Float64, false),
            Field::new("low", DataType::Float64, false),
            Field::new("close", DataType::Float64, false),
            Field::new("volume", DataType::Float64, false),
            Field::new("quote_volume", DataType::Float64, false),
            Field::new("vwap", DataType::Float64, false),
            Field::new("trade_count", DataType::UInt64, false),
            Field::new("revision", DataType::UInt64, false),
        ]))
    }

    fn event_batch(records: &[ArchiveRecord]) -> BpResult<RecordBatch> {
        let events = records
            .iter()
            .filter_map(|record| match record {
                ArchiveRecord::MarketEvent(event) => Some(event),
                ArchiveRecord::ClosedCandle(_) => None,
            })
            .collect::<Vec<_>>();

        let columns: Vec<ArrayRef> = vec![
            Self::strings(
                events
                    .iter()
                    .map(|event| event.exchange.to_string())
                    .collect(),
            ),
            Self::strings(events.iter().map(|event| event.stream.clone()).collect()),
            Arc::new(UInt64Array::from_iter(
                events.iter().map(|event| event.sequence),
            )),
            Arc::new(Int64Array::from_iter(events.iter().map(|event| {
                event.exchange_timestamp.map(|timestamp| timestamp.0)
            }))),
            Arc::new(Int64Array::from_iter_values(
                events.iter().map(|event| event.received_at.0),
            )),
            Self::strings(events.iter().map(|event| event.payload.clone()).collect()),
        ];
        Ok(RecordBatch::try_new(Self::event_schema(), columns)?)
    }

    fn candle_batch(records: &[ArchiveRecord]) -> BpResult<RecordBatch> {
        let candles = records
            .iter()
            .filter_map(|record| match record {
                ArchiveRecord::ClosedCandle(closed) => Some(closed),
                ArchiveRecord::MarketEvent(_) => None,
            })
            .collect::<Vec<_>>();

        let columns: Vec<ArrayRef> = vec![
            Self::strings(
                candles
                    .iter()
                    .map(|closed| closed.candle.key.exchange.to_string())
                    .collect(),
            ),
            Self::strings(
                candles
                    .iter()
                    .map(|closed| closed.candle.key.trading_pair.base.to_string())
                    .collect(),
            ),
            Self::strings(
                candles
                    .iter()
                    .map(|closed| closed.candle.key.trading_pair.quote.to_string())
                    .collect(),
            ),
            Self::strings(
                candles
                    .iter()
                    .map(|closed| closed.candle.key.interval.label())
                    .collect(),
            ),
            Arc::new(Int64Array::from_iter_values(candles.iter().map(|closed| {
                i64::try_from(closed.candle.market_candle.start_time_millis_inc).unwrap_or(i64::MAX)
            }))),
            Arc::new(Int64Array::from_iter_values(candles.iter().map(|closed| {
                i64::try_from(closed.candle.market_candle.end_time_millis_exc).unwrap_or(i64::MAX)
            }))),
            Arc::new(Int64Array::from_iter_values(candles.iter().map(|closed| {
                i64::try_from(closed.candle.market_candle.duration_millis).unwrap_or(i64::MAX)
            }))),
            Arc::new(BooleanArray::from_iter(candles.iter().map(|closed| {
                Some(closed.candle.market_candle.is_candle_closed)
            }))),
            Arc::new(Float64Array::from_iter_values(
                candles
                    .iter()
                    .map(|closed| closed.candle.market_candle.ohlcv.open),
            )),
            Arc::new(Float64Array::from_iter_values(
                candles
                    .iter()
                    .map(|closed| closed.candle.market_candle.ohlcv.high),
            )),
            Arc::new(Float64Array::from_iter_values(
                candles
                    .iter()
                    .map(|closed| closed.candle.market_candle.ohlcv.low),
            )),
            Arc::new(Float64Array::from_iter_values(
                candles
                    .iter()
                    .map(|closed| closed.candle.market_candle.ohlcv.close),
            )),
            Arc::new(Float64Array::from_iter_values(
                candles
                    .iter()
                    .map(|closed| closed.candle.market_candle.ohlcv.volume),
            )),
            Arc::new(Float64Array::from_iter_values(
                candles
                    .iter()
                    .map(|closed| closed.candle.market_candle.ohlcv.quote_volume),
            )),
            Arc::new(Float64Array::from_iter_values(
                candles
                    .iter()
                    .map(|closed| closed.candle.market_candle.ohlcv.vwap),
            )),
            Arc::new(UInt64Array::from_iter_values(
                candles
                    .iter()
                    .map(|closed| closed.candle.market_candle.trade_count),
            )),
            Arc::new(UInt64Array::from_iter_values(
                candles.iter().map(|closed| closed.revision),
            )),
        ];
        Ok(RecordBatch::try_new(Self::candle_schema(), columns)?)
    }

    fn strings(values: Vec<String>) -> ArrayRef {
        Arc::new(StringArray::from_iter_values(
            values.iter().map(String::as_str),
        ))
    }

    fn writer_properties() -> WriterProperties {
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build()
    }
}
