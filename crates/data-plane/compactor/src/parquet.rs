use crate::record::{ArchiveKind, ArchiveRecord, CandleRecord, EventRecord};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};
use platform_errors::{BpError, BpResult};
use std::{io::ErrorKind, sync::Arc};

/// Reads and writes the Parquet format shared with the archiver.
///
/// TODO: stream batches through an external merge sort instead of buffering each
/// source partition in memory.
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

    pub fn decode(kind: ArchiveKind, bytes: Bytes) -> BpResult<Vec<ArchiveRecord>> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
        let reader = builder.with_batch_size(8_192).build()?;
        let mut records = Vec::new();
        for batch in reader {
            let batch = batch?;
            match kind {
                ArchiveKind::Events => records.extend(
                    Self::decode_events(&batch)?
                        .into_iter()
                        .map(ArchiveRecord::Event),
                ),
                ArchiveKind::Candles => records.extend(
                    Self::decode_candles(&batch)?
                        .into_iter()
                        .map(ArchiveRecord::Candle),
                ),
            }
        }
        Ok(records)
    }

    fn decode_events(batch: &RecordBatch) -> BpResult<Vec<EventRecord>> {
        let exchange = Self::strings(batch, 0, "exchange")?;
        let stream = Self::strings(batch, 1, "stream")?;
        let sequence = Self::u64s(batch, 2, "sequence")?;
        let exchange_timestamp = Self::i64s(batch, 3, "exchange_timestamp")?;
        let received_at = Self::i64s(batch, 4, "received_at")?;
        let payload = Self::strings(batch, 5, "payload")?;

        Ok((0..batch.num_rows())
            .map(|row| EventRecord {
                exchange: exchange.value(row).to_string(),
                stream: stream.value(row).to_string(),
                sequence: (!sequence.is_null(row)).then(|| sequence.value(row)),
                exchange_timestamp: (!exchange_timestamp.is_null(row))
                    .then(|| exchange_timestamp.value(row)),
                received_at: received_at.value(row),
                payload: payload.value(row).to_string(),
            })
            .collect())
    }

    fn decode_candles(batch: &RecordBatch) -> BpResult<Vec<CandleRecord>> {
        let exchange = Self::strings(batch, 0, "exchange")?;
        let base = Self::strings(batch, 1, "base")?;
        let quote = Self::strings(batch, 2, "quote")?;
        let interval = Self::strings(batch, 3, "interval")?;
        let start_time_millis_inc = Self::i64s(batch, 4, "start_time_millis_inc")?;
        let end_time_millis_exc = Self::i64s(batch, 5, "end_time_millis_exc")?;
        let duration_millis = Self::i64s(batch, 6, "duration_millis")?;
        let is_candle_closed = Self::booleans(batch, 7, "is_candle_closed")?;
        let open = Self::f64s(batch, 8, "open")?;
        let high = Self::f64s(batch, 9, "high")?;
        let low = Self::f64s(batch, 10, "low")?;
        let close = Self::f64s(batch, 11, "close")?;
        let volume = Self::f64s(batch, 12, "volume")?;
        let quote_volume = Self::f64s(batch, 13, "quote_volume")?;
        let vwap = Self::f64s(batch, 14, "vwap")?;
        let trade_count = Self::u64s(batch, 15, "trade_count")?;
        let revision = Self::u64s(batch, 16, "revision")?;

        Ok((0..batch.num_rows())
            .map(|row| CandleRecord {
                exchange: exchange.value(row).to_string(),
                base: base.value(row).to_string(),
                quote: quote.value(row).to_string(),
                interval: interval.value(row).to_string(),
                start_time_millis_inc: start_time_millis_inc.value(row),
                end_time_millis_exc: end_time_millis_exc.value(row),
                duration_millis: duration_millis.value(row),
                is_candle_closed: is_candle_closed.value(row),
                open: open.value(row),
                high: high.value(row),
                low: low.value(row),
                close: close.value(row),
                volume: volume.value(row),
                quote_volume: quote_volume.value(row),
                vwap: vwap.value(row),
                trade_count: trade_count.value(row),
                revision: revision.value(row),
            })
            .collect())
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
                ArchiveRecord::Event(event) => Some(event),
                ArchiveRecord::Candle(_) => None,
            })
            .collect::<Vec<_>>();

        let columns: Vec<ArrayRef> = vec![
            Self::string_array(events.iter().map(|event| event.exchange.clone()).collect()),
            Self::string_array(events.iter().map(|event| event.stream.clone()).collect()),
            Arc::new(UInt64Array::from_iter(
                events.iter().map(|event| event.sequence),
            )),
            Arc::new(Int64Array::from_iter(
                events.iter().map(|event| event.exchange_timestamp),
            )),
            Arc::new(Int64Array::from_iter_values(
                events.iter().map(|event| event.received_at),
            )),
            Self::string_array(events.iter().map(|event| event.payload.clone()).collect()),
        ];
        Ok(RecordBatch::try_new(Self::event_schema(), columns)?)
    }

    fn candle_batch(records: &[ArchiveRecord]) -> BpResult<RecordBatch> {
        let candles = records
            .iter()
            .filter_map(|record| match record {
                ArchiveRecord::Candle(candle) => Some(candle),
                ArchiveRecord::Event(_) => None,
            })
            .collect::<Vec<_>>();

        let columns: Vec<ArrayRef> = vec![
            Self::string_array(
                candles
                    .iter()
                    .map(|candle| candle.exchange.clone())
                    .collect(),
            ),
            Self::string_array(candles.iter().map(|candle| candle.base.clone()).collect()),
            Self::string_array(candles.iter().map(|candle| candle.quote.clone()).collect()),
            Self::string_array(
                candles
                    .iter()
                    .map(|candle| candle.interval.clone())
                    .collect(),
            ),
            Arc::new(Int64Array::from_iter_values(
                candles.iter().map(|candle| candle.start_time_millis_inc),
            )),
            Arc::new(Int64Array::from_iter_values(
                candles.iter().map(|candle| candle.end_time_millis_exc),
            )),
            Arc::new(Int64Array::from_iter_values(
                candles.iter().map(|candle| candle.duration_millis),
            )),
            Arc::new(BooleanArray::from_iter(
                candles.iter().map(|candle| Some(candle.is_candle_closed)),
            )),
            Arc::new(Float64Array::from_iter_values(
                candles.iter().map(|candle| candle.open),
            )),
            Arc::new(Float64Array::from_iter_values(
                candles.iter().map(|candle| candle.high),
            )),
            Arc::new(Float64Array::from_iter_values(
                candles.iter().map(|candle| candle.low),
            )),
            Arc::new(Float64Array::from_iter_values(
                candles.iter().map(|candle| candle.close),
            )),
            Arc::new(Float64Array::from_iter_values(
                candles.iter().map(|candle| candle.volume),
            )),
            Arc::new(Float64Array::from_iter_values(
                candles.iter().map(|candle| candle.quote_volume),
            )),
            Arc::new(Float64Array::from_iter_values(
                candles.iter().map(|candle| candle.vwap),
            )),
            Arc::new(UInt64Array::from_iter_values(
                candles.iter().map(|candle| candle.trade_count),
            )),
            Arc::new(UInt64Array::from_iter_values(
                candles.iter().map(|candle| candle.revision),
            )),
        ];
        Ok(RecordBatch::try_new(Self::candle_schema(), columns)?)
    }

    fn strings<'a>(batch: &'a RecordBatch, index: usize, name: &str) -> BpResult<&'a StringArray> {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Self::column_error(name))
    }

    fn i64s<'a>(batch: &'a RecordBatch, index: usize, name: &str) -> BpResult<&'a Int64Array> {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Self::column_error(name))
    }

    fn u64s<'a>(batch: &'a RecordBatch, index: usize, name: &str) -> BpResult<&'a UInt64Array> {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| Self::column_error(name))
    }

    fn f64s<'a>(batch: &'a RecordBatch, index: usize, name: &str) -> BpResult<&'a Float64Array> {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| Self::column_error(name))
    }

    fn booleans<'a>(
        batch: &'a RecordBatch,
        index: usize,
        name: &str,
    ) -> BpResult<&'a BooleanArray> {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| Self::column_error(name))
    }

    fn column_error(name: &str) -> BpError {
        BpError::io(
            "decode archived parquet",
            name,
            std::io::Error::new(
                ErrorKind::InvalidData,
                format!("column {name} has an unexpected type"),
            ),
        )
    }

    fn string_array(values: Vec<String>) -> ArrayRef {
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
