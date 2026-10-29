use super::ArchiveBatch;
use crate::record::{ArchiveKind, ArchiveRecord};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use platform_errors::{BpError, BpResult};
use platform_market_data::{
    candle::{Candle, CandleInterval, CandleKey, CandleRevision, ClosedCandle},
    envelope::EnvelopeCodec,
    event::MarketEvent,
};
use platform_types::new_types::TimestampMilliseconds;
use std::{fmt::Display, io::ErrorKind, str::FromStr};
use stock_trek::markets::{MarketCandle, MarketOhlcv};
use stock_trek_types::cex::{asset_id::AssetId, trading_pair::TradingPair};

impl ArchiveBatch {
    pub fn from_record_batch(kind: ArchiveKind, batch: &RecordBatch) -> BpResult<Self> {
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
        let revision = Self::string_column(batch, "revision")?;

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
                    revision: CandleRevision::decode(revision.value(row).as_bytes())?,
                    candle: Candle {
                        key: CandleKey::new(exchange, TradingPair::new(base, quote), interval),
                        market_candle,
                    },
                })
            })
            .collect()
    }

    fn array<'a>(batch: &'a RecordBatch, name: &str) -> BpResult<&'a ArrayRef> {
        batch
            .column_by_name(name)
            .ok_or_else(|| Self::column_error(name))
    }

    fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> BpResult<&'a StringArray> {
        Self::array(batch, name)?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Self::column_error(name))
    }

    fn i64_column<'a>(batch: &'a RecordBatch, name: &str) -> BpResult<&'a Int64Array> {
        Self::array(batch, name)?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Self::column_error(name))
    }

    fn u64_column<'a>(batch: &'a RecordBatch, name: &str) -> BpResult<&'a UInt64Array> {
        Self::array(batch, name)?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| Self::column_error(name))
    }

    fn f64_column<'a>(batch: &'a RecordBatch, name: &str) -> BpResult<&'a Float64Array> {
        Self::array(batch, name)?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| Self::column_error(name))
    }

    fn boolean_column<'a>(batch: &'a RecordBatch, name: &str) -> BpResult<&'a BooleanArray> {
        Self::array(batch, name)?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| Self::column_error(name))
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
}
