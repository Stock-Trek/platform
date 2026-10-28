use crate::record::{ArchiveKind, ArchiveRecord};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use platform_errors::{BpError, BpResult};
use platform_market_data::{
    candle::{Candle, CandleInterval, CandleKey, ClosedCandle},
    event::MarketEvent,
};
use platform_types::new_types::TimestampMilliseconds;
use std::{fmt::Display, io::ErrorKind, str::FromStr, sync::Arc};
use stock_trek::markets::{MarketCandle, MarketOhlcv};
use stock_trek_types::cex::{asset_id::AssetId, trading_pair::TradingPair};

#[derive(Debug, Clone)]
pub struct ArchiveBatch {
    kind: ArchiveKind,
    records: Vec<ArchiveRecord>,
}

impl ArchiveBatch {
    pub fn new(kind: ArchiveKind, records: Vec<ArchiveRecord>) -> BpResult<Self> {
        if let Some(record) = records.iter().find(|record| record.kind() != kind) {
            return Err(Self::kind_mismatch(kind, record));
        }
        Ok(Self { kind, records })
    }

    pub fn push(&mut self, record: ArchiveRecord) -> BpResult<()> {
        if record.kind() != self.kind {
            return Err(Self::kind_mismatch(self.kind, &record));
        }
        self.records.push(record);
        Ok(())
    }

    pub fn clear(&mut self) {
        self.records.clear();
    }

    pub fn kind(&self) -> ArchiveKind {
        self.kind
    }

    pub fn records(&self) -> &[ArchiveRecord] {
        &self.records
    }

    pub fn into_records(self) -> Vec<ArchiveRecord> {
        self.records
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub(crate) fn schema(kind: ArchiveKind) -> SchemaRef {
        match kind {
            ArchiveKind::Events => Arc::new(Schema::new(vec![
                Field::new("exchange", DataType::Utf8, false),
                Field::new("stream", DataType::Utf8, false),
                Field::new("sequence", DataType::UInt64, true),
                Field::new("exchange_timestamp", DataType::Int64, true),
                Field::new("received_at", DataType::Int64, false),
                Field::new("payload", DataType::Utf8, false),
            ])),
            ArchiveKind::Candles => Arc::new(Schema::new(vec![
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
            ])),
        }
    }

    pub(crate) fn to_record_batch(&self) -> BpResult<RecordBatch> {
        Self::record_batch(self.kind, &self.records)
    }

    pub(crate) fn from_record_batch(kind: ArchiveKind, batch: &RecordBatch) -> BpResult<Self> {
        let records = match kind {
            ArchiveKind::Events => Self::decode_events(batch)?
                .into_iter()
                .map(ArchiveRecord::MarketEvent)
                .collect(),
            ArchiveKind::Candles => Self::decode_candles(batch)?
                .into_iter()
                .map(ArchiveRecord::ClosedCandle)
                .collect(),
        };
        Ok(Self { kind, records })
    }

    pub(crate) fn record_batch(
        kind: ArchiveKind,
        records: &[ArchiveRecord],
    ) -> BpResult<RecordBatch> {
        match kind {
            ArchiveKind::Events => {
                let events = Self::events(records)?;
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
                Ok(RecordBatch::try_new(Self::schema(kind), columns)?)
            }
            ArchiveKind::Candles => {
                let candles = Self::candles(records)?;
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
                        i64::try_from(closed.candle.market_candle.start_time_millis_inc)
                            .unwrap_or(i64::MAX)
                    }))),
                    Arc::new(Int64Array::from_iter_values(candles.iter().map(|closed| {
                        i64::try_from(closed.candle.market_candle.end_time_millis_exc)
                            .unwrap_or(i64::MAX)
                    }))),
                    Arc::new(Int64Array::from_iter_values(candles.iter().map(|closed| {
                        i64::try_from(closed.candle.market_candle.duration_millis)
                            .unwrap_or(i64::MAX)
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
                Ok(RecordBatch::try_new(Self::schema(kind), columns)?)
            }
        }
    }

    fn events(records: &[ArchiveRecord]) -> BpResult<Vec<&MarketEvent>> {
        records
            .iter()
            .map(|record| match record {
                ArchiveRecord::MarketEvent(event) => Ok(event),
                ArchiveRecord::ClosedCandle(_) => {
                    Err(Self::kind_mismatch(ArchiveKind::Events, record))
                }
            })
            .collect()
    }

    fn candles(records: &[ArchiveRecord]) -> BpResult<Vec<&ClosedCandle>> {
        records
            .iter()
            .map(|record| match record {
                ArchiveRecord::ClosedCandle(closed) => Ok(closed),
                ArchiveRecord::MarketEvent(_) => {
                    Err(Self::kind_mismatch(ArchiveKind::Candles, record))
                }
            })
            .collect()
    }

    fn kind_mismatch(expected: ArchiveKind, record: &ArchiveRecord) -> BpError {
        BpError::io(
            "encode archived parquet",
            expected.label(),
            std::io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "record kind {} does not match batch kind {}",
                    record.kind().label(),
                    expected.label()
                ),
            ),
        )
    }

    fn decode_events(batch: &RecordBatch) -> BpResult<Vec<MarketEvent>> {
        let exchange = Self::string_column(batch, "exchange")?;
        let stream = Self::string_column(batch, "stream")?;
        let sequence = Self::u64_column(batch, "sequence")?;
        let exchange_timestamp = Self::i64_column(batch, "exchange_timestamp")?;
        let received_at = Self::i64_column(batch, "received_at")?;
        let payload = Self::string_column(batch, "payload")?;

        (0..batch.num_rows())
            .map(|row| {
                Ok(MarketEvent {
                    exchange: parse(exchange.value(row), "exchange")?,
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
        let exchange = Self::string_column(batch, "exchange")?;
        let base = Self::string_column(batch, "base")?;
        let quote = Self::string_column(batch, "quote")?;
        let interval = Self::string_column(batch, "interval")?;
        let start_time_millis_inc = Self::i64_column(batch, "start_time_millis_inc")?;
        let end_time_millis_exc = Self::i64_column(batch, "end_time_millis_exc")?;
        let duration_millis = Self::i64_column(batch, "duration_millis")?;
        let is_candle_closed = Self::boolean_column(batch, "is_candle_closed")?;
        let open = Self::f64_column(batch, "open")?;
        let high = Self::f64_column(batch, "high")?;
        let low = Self::f64_column(batch, "low")?;
        let close = Self::f64_column(batch, "close")?;
        let volume = Self::f64_column(batch, "volume")?;
        let quote_volume = Self::f64_column(batch, "quote_volume")?;
        let vwap = Self::f64_column(batch, "vwap")?;
        let trade_count = Self::u64_column(batch, "trade_count")?;
        let revision = Self::u64_column(batch, "revision")?;

        (0..batch.num_rows())
            .map(|row| {
                let exchange = parse(exchange.value(row), "exchange")?;
                let base: AssetId = parse(base.value(row), "base")?;
                let quote: AssetId = parse(quote.value(row), "quote")?;
                if base == quote {
                    return Err(invalid_value(
                        "base/quote",
                        format!("{base}/{quote}"),
                        "trading pair base and quote must differ",
                    ));
                }
                let interval = CandleInterval::from_label(interval.value(row))
                    .map_err(|error| invalid_value("interval", interval.value(row), error))?;
                let market_candle = MarketCandle::new(
                    timestamp(start_time_millis_inc.value(row), "start_time_millis_inc")?,
                    timestamp(end_time_millis_exc.value(row), "end_time_millis_exc")?,
                    timestamp(duration_millis.value(row), "duration_millis")?,
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

    fn strings(values: impl IntoIterator<Item = String>) -> ArrayRef {
        Arc::new(StringArray::from_iter_values(values))
    }

    fn array<'a>(batch: &'a RecordBatch, name: &str) -> BpResult<&'a ArrayRef> {
        batch.column_by_name(name).ok_or_else(|| column_error(name))
    }

    fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> BpResult<&'a StringArray> {
        Self::array(batch, name)?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| column_error(name))
    }

    fn i64_column<'a>(batch: &'a RecordBatch, name: &str) -> BpResult<&'a Int64Array> {
        Self::array(batch, name)?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| column_error(name))
    }

    fn u64_column<'a>(batch: &'a RecordBatch, name: &str) -> BpResult<&'a UInt64Array> {
        Self::array(batch, name)?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| column_error(name))
    }

    fn f64_column<'a>(batch: &'a RecordBatch, name: &str) -> BpResult<&'a Float64Array> {
        Self::array(batch, name)?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| column_error(name))
    }

    fn boolean_column<'a>(batch: &'a RecordBatch, name: &str) -> BpResult<&'a BooleanArray> {
        Self::array(batch, name)?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| column_error(name))
    }
}

fn parse<T>(value: &str, column: &str) -> BpResult<T>
where
    T: FromStr,
    T::Err: Display,
{
    value
        .parse()
        .map_err(|error| invalid_value(column, value, error))
}

fn timestamp(value: i64, column: &str) -> BpResult<u64> {
    u64::try_from(value).map_err(|error| invalid_value(column, value, error))
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

fn column_error(name: &str) -> BpError {
    BpError::io(
        "decode archived parquet",
        name.to_string(),
        std::io::Error::new(
            ErrorKind::InvalidData,
            format!("column {name} is missing or has an unexpected type"),
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArchiveParquet, ArchiveParquetWriter};
    use bytes::Bytes;
    use stock_trek_types::cex::cex_id::CexId;

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
}
