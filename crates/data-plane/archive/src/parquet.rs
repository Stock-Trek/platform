use crate::{
    parquet_reader::ParquetReader,
    parquet_writer::writer_properties,
    record::{ArchiveKind, ArchiveRecord},
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use futures::StreamExt;
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
        async_reader::ParquetRecordBatchStreamBuilder,
    },
    file::reader::ChunkReader,
};
use platform_errors::{BpError, BpResult};
use platform_market_data::{
    candle::{Candle, CandleInterval, CandleKey, ClosedCandle},
    event::MarketEvent,
};
use platform_types::new_types::TimestampMilliseconds;
use std::{
    fmt::Display,
    io::{ErrorKind, Write},
    str::FromStr,
    sync::Arc,
};
use stock_trek::markets::{MarketCandle, MarketOhlcv};
use stock_trek_types::cex::{asset_id::AssetId, trading_pair::TradingPair};

/// Reads and writes the Parquet format shared by the archiver and compactor.
pub struct ArchiveParquet;

/// Streams decoded archive records one bounded batch at a time.
pub struct ArchiveParquetBatches {
    kind: ArchiveKind,
    reader: ParquetRecordBatchReader,
}

impl Iterator for ArchiveParquetBatches {
    type Item = BpResult<Vec<ArchiveRecord>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.next()? {
            Ok(batch) => Some(ArchiveParquet::decode_batch(self.kind, &batch)),
            Err(error) => Some(Err(error.into())),
        }
    }
}

/// Streams encoded archive records into a writer without building the whole
/// Parquet file in memory.
pub struct ArchiveParquetWriter<W: Write + Send> {
    kind: ArchiveKind,
    writer: ArrowWriter<W>,
}

impl<W: Write + Send> ArchiveParquetWriter<W> {
    pub fn try_new(kind: ArchiveKind, sink: W) -> BpResult<Self> {
        let schema = match kind {
            ArchiveKind::Events => ArchiveParquet::event_schema(),
            ArchiveKind::Candles => ArchiveParquet::candle_schema(),
        };
        Ok(Self {
            kind,
            writer: ArrowWriter::try_new(sink, schema, Some(writer_properties()))?,
        })
    }

    pub fn write(&mut self, records: &[ArchiveRecord]) -> BpResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        let batch = match self.kind {
            ArchiveKind::Events => ArchiveParquet::event_batch(records)?,
            ArchiveKind::Candles => ArchiveParquet::candle_batch(records)?,
        };
        self.writer.write(&batch)?;
        Ok(())
    }

    pub fn close(self) -> BpResult<()> {
        self.writer.close()?;
        Ok(())
    }
}

impl ArchiveParquet {
    pub fn encode(kind: ArchiveKind, records: &[ArchiveRecord]) -> BpResult<Vec<u8>> {
        let batch = match kind {
            ArchiveKind::Events => Self::event_batch(records)?,
            ArchiveKind::Candles => Self::candle_batch(records)?,
        };
        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, batch.schema(), Some(writer_properties()))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(buffer)
    }

    pub fn decode(kind: ArchiveKind, bytes: Bytes) -> BpResult<Vec<ArchiveRecord>> {
        let mut records = Vec::new();
        for batch in Self::decode_batches(kind, bytes)? {
            records.extend(batch?);
        }
        Ok(records)
    }

    /// Decodes a Parquet source in bounded batches. The source can be an
    /// in-memory buffer or a file, so callers can avoid materializing both the
    /// encoded object and every decoded record at once.
    pub fn decode_batches<R>(kind: ArchiveKind, source: R) -> BpResult<ArchiveParquetBatches>
    where
        R: ChunkReader + 'static,
    {
        let builder = ParquetRecordBatchReaderBuilder::try_new(source)?;
        let reader = builder.with_batch_size(8_192).build()?;
        Ok(ArchiveParquetBatches { kind, reader })
    }

    /// Decode an object without downloading it in full.
    ///
    /// The reader fetches the Parquet footer/metadata and only the column chunks
    /// needed for each record batch through the S3 transfer manager.
    pub async fn decode_reader(
        kind: ArchiveKind,
        reader: ParquetReader,
    ) -> BpResult<Vec<ArchiveRecord>> {
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        let mut stream = builder.with_batch_size(8_192).build()?;
        let mut records = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            records.extend(Self::decode_batch(kind, &batch)?);
        }
        Ok(records)
    }

    fn decode_batch(kind: ArchiveKind, batch: &RecordBatch) -> BpResult<Vec<ArchiveRecord>> {
        match kind {
            ArchiveKind::Events => Ok(Self::decode_events(batch)?
                .into_iter()
                .map(ArchiveRecord::MarketEvent)
                .collect()),
            ArchiveKind::Candles => Ok(Self::decode_candles(batch)?
                .into_iter()
                .map(ArchiveRecord::ClosedCandle)
                .collect()),
        }
    }

    fn decode_events(batch: &RecordBatch) -> BpResult<Vec<MarketEvent>> {
        let exchange = Self::strings_column(batch, 0, "exchange")?;
        let stream = Self::strings_column(batch, 1, "stream")?;
        let sequence = Self::u64s(batch, 2, "sequence")?;
        let exchange_timestamp = Self::i64s(batch, 3, "exchange_timestamp")?;
        let received_at = Self::i64s(batch, 4, "received_at")?;
        let payload = Self::strings_column(batch, 5, "payload")?;

        (0..batch.num_rows())
            .map(|row| {
                Ok(MarketEvent {
                    exchange: Self::parse(exchange.value(row), "exchange")?,
                    stream: stream.value(row).to_string(),
                    sequence: (!sequence.is_null(row)).then(|| sequence.value(row)),
                    exchange_timestamp: (!exchange_timestamp.is_null(row))
                        .then(|| TimestampMilliseconds(exchange_timestamp.value(row))),
                    received_at: TimestampMilliseconds(received_at.value(row)),
                    payload: payload.value(row).to_string(),
                })
            })
            .collect()
    }

    fn decode_candles(batch: &RecordBatch) -> BpResult<Vec<ClosedCandle>> {
        let exchange = Self::strings_column(batch, 0, "exchange")?;
        let base = Self::strings_column(batch, 1, "base")?;
        let quote = Self::strings_column(batch, 2, "quote")?;
        let interval = Self::strings_column(batch, 3, "interval")?;
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

        (0..batch.num_rows())
            .map(|row| {
                let exchange = Self::parse(exchange.value(row), "exchange")?;
                let base: AssetId = Self::parse(base.value(row), "base")?;
                let quote: AssetId = Self::parse(quote.value(row), "quote")?;
                if base == quote {
                    return Err(Self::invalid_value(
                        "base/quote",
                        format!("{base}/{quote}"),
                        "trading pair base and quote must differ",
                    ));
                }
                let interval = CandleInterval::from_label(interval.value(row))
                    .map_err(|error| Self::invalid_value("interval", interval.value(row), error))?;
                let market_candle = MarketCandle::new(
                    Self::timestamp(start_time_millis_inc.value(row), "start_time_millis_inc")?,
                    Self::timestamp(end_time_millis_exc.value(row), "end_time_millis_exc")?,
                    Self::timestamp(duration_millis.value(row), "duration_millis")?,
                    is_candle_closed.value(row),
                    MarketOhlcv::new(
                        open.value(row),
                        high.value(row),
                        low.value(row),
                        close.value(row),
                        volume.value(row),
                        quote_volume.value(row),
                        vwap.value(row),
                    ),
                    trade_count.value(row),
                );
                Ok(ClosedCandle {
                    revision: revision.value(row),
                    candle: Candle {
                        key: CandleKey::new(exchange, TradingPair::new(base, quote), interval),
                        market_candle,
                    },
                })
            })
            .collect()
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
            Self::strings(events.iter().map(|event| event.exchange.to_string())),
            Self::strings(events.iter().map(|event| event.stream.clone())),
            Arc::new(UInt64Array::from_iter(
                events.iter().map(|event| event.sequence),
            )),
            Arc::new(Int64Array::from_iter(events.iter().map(|event| {
                event.exchange_timestamp.map(|timestamp| timestamp.0)
            }))),
            Arc::new(Int64Array::from_iter_values(
                events.iter().map(|event| event.received_at.0),
            )),
            Self::strings(events.iter().map(|event| event.payload.clone())),
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
                    .map(|closed| closed.candle.key.exchange.to_string()),
            ),
            Self::strings(
                candles
                    .iter()
                    .map(|closed| closed.candle.key.trading_pair.base.to_string()),
            ),
            Self::strings(
                candles
                    .iter()
                    .map(|closed| closed.candle.key.trading_pair.quote.to_string()),
            ),
            Self::strings(
                candles
                    .iter()
                    .map(|closed| closed.candle.key.interval.label()),
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

    fn strings(values: impl IntoIterator<Item = String>) -> ArrayRef {
        Arc::new(StringArray::from_iter_values(values))
    }

    fn parse<T>(value: &str, column: &str) -> BpResult<T>
    where
        T: FromStr,
        T::Err: Display,
    {
        value
            .parse()
            .map_err(|error| Self::invalid_value(column, value, error))
    }

    fn timestamp(value: i64, column: &str) -> BpResult<u64> {
        u64::try_from(value).map_err(|error| Self::invalid_value(column, value, error))
    }

    fn invalid_value(column: &str, value: impl Display, reason: impl Display) -> BpError {
        BpError::io(
            "decode archived parquet",
            column.to_string(),
            std::io::Error::new(
                ErrorKind::InvalidData,
                format!("column {column} has invalid value {value}: {reason}"),
            ),
        )
    }

    fn strings_column<'a>(
        batch: &'a RecordBatch,
        index: usize,
        name: &str,
    ) -> BpResult<&'a StringArray> {
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
            name.to_string(),
            std::io::Error::new(
                ErrorKind::InvalidData,
                format!("column {name} has an unexpected type"),
            ),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId};

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
    fn events_round_trip_through_parquet() {
        let expected = event(42);
        let encoded = ArchiveParquet::encode(
            ArchiveKind::Events,
            &[ArchiveRecord::MarketEvent(expected.clone())],
        )
        .unwrap();
        let decoded = ArchiveParquet::decode(ArchiveKind::Events, Bytes::from(encoded)).unwrap();

        assert_eq!(decoded.len(), 1);
        let ArchiveRecord::MarketEvent(actual) = &decoded[0] else {
            panic!("expected a market event");
        };
        assert_eq!(actual, &expected);
    }

    #[test]
    fn candles_round_trip_through_parquet() {
        let expected = closed_candle(7);
        let encoded = ArchiveParquet::encode(
            ArchiveKind::Candles,
            &[ArchiveRecord::ClosedCandle(expected.clone())],
        )
        .unwrap();
        let decoded = ArchiveParquet::decode(ArchiveKind::Candles, Bytes::from(encoded)).unwrap();

        assert_eq!(decoded.len(), 1);
        let ArchiveRecord::ClosedCandle(actual) = &decoded[0] else {
            panic!("expected a closed candle");
        };
        assert_eq!(actual.revision, expected.revision);
        assert_eq!(actual.candle.key, expected.candle.key);
        assert_eq!(
            actual.candle.market_candle.start_time_millis_inc,
            expected.candle.market_candle.start_time_millis_inc
        );
        assert_eq!(
            actual.candle.market_candle.end_time_millis_exc,
            expected.candle.market_candle.end_time_millis_exc
        );
        assert_eq!(
            actual.candle.market_candle.duration_millis,
            expected.candle.market_candle.duration_millis
        );
        assert_eq!(
            actual.candle.market_candle.trade_count,
            expected.candle.market_candle.trade_count
        );
        assert_eq!(
            actual.candle.market_candle.ohlcv.quote_volume,
            expected.candle.market_candle.ohlcv.quote_volume
        );
        assert_eq!(
            actual.candle.market_candle.ohlcv.vwap,
            expected.candle.market_candle.ohlcv.vwap
        );
    }

    #[test]
    fn records_of_the_other_kind_are_ignored() {
        let encoded = ArchiveParquet::encode(
            ArchiveKind::Events,
            &[
                ArchiveRecord::MarketEvent(event(1)),
                ArchiveRecord::ClosedCandle(closed_candle(1)),
            ],
        )
        .unwrap();
        let decoded = ArchiveParquet::decode(ArchiveKind::Events, Bytes::from(encoded)).unwrap();

        assert_eq!(decoded.len(), 1);
        assert!(matches!(decoded[0], ArchiveRecord::MarketEvent(_)));
    }
}
