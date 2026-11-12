use crate::archive_record::{ArchiveKind, ArchiveRecord, ArchivedMarketEvent};
use crate::batch::ArchiveBatch;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use platform_errors::{BuildError, BuildResult};
use platform_market_data::{
    AlignedWindow, Candle, CandleKey, CandleRevision, ClosedCandle, MarketOhlcv,
};
use platform_market_data::{EnvelopeCodec, MarketEvent};
use platform_types::TimestampMilliseconds;
use std::{fmt::Display, io::ErrorKind, str::FromStr};
use stock_trek_types::cex::{asset_id::AssetId, trading_pair::TradingPair};

impl ArchiveBatch {
    pub fn from_record_batch(kind: ArchiveKind, batch: &RecordBatch) -> BuildResult<Self> {
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

    fn decode_events(batch: &RecordBatch) -> BuildResult<Vec<ArchivedMarketEvent>> {
        let exchange = Self::string_column(batch, "exchange")?;
        let stream = Self::string_column(batch, "stream")?;
        let sequence = Self::u64_column(batch, "sequence")?;
        let exchange_timestamp = Self::i64_column(batch, "exchange_timestamp")?;
        let received_at = Self::i64_column(batch, "received_at")?;
        let payload = Self::string_column(batch, "payload")?;
        let state_epoch = Self::u64_column(batch, "state_epoch")?;
        let routing_key = Self::u64_column(batch, "routing_key")?;

        (0..batch.num_rows())
            .map(|row| {
                let decoded = ArchivedMarketEvent {
                    event: MarketEvent {
                        exchange: Self::parse(exchange.value(row), "exchange")?,
                        stream: stream.value(row).to_string(),
                        sequence: (!sequence.is_null(row)).then(|| sequence.value(row)),
                        exchange_timestamp: (!exchange_timestamp.is_null(row))
                            .then(|| TimestampMilliseconds(exchange_timestamp.value(row))),
                        received_at: TimestampMilliseconds(received_at.value(row)),
                        payload: payload.value(row).to_string(),
                    },
                    state_epoch: state_epoch.value(row),
                };
                if routing_key.value(row) != decoded.routing_key().value() {
                    return Err(Self::invalid_value(
                        "routing_key",
                        routing_key.value(row),
                        "does not match the event's trading pair",
                    ));
                }
                Ok(decoded)
            })
            .collect()
    }

    fn decode_candles(batch: &RecordBatch) -> BuildResult<Vec<ClosedCandle>> {
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
        let close_watermark = Self::i64_column(batch, "close_watermark")?;
        let source_offset = Self::u64_column(batch, "source_offset")?;
        let state_epoch = Self::u64_column(batch, "state_epoch")?;
        let revision_id = Self::string_column(batch, "revision_id")?;
        let revision = Self::string_column(batch, "revision")?;
        let is_complete = Self::boolean_column(batch, "is_complete")?;
        let routing_key = Self::u64_column(batch, "routing_key")?;

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
                let interval = AlignedWindow::try_from(interval.value(row))
                    .map_err(|error| Self::invalid_value("interval", interval.value(row), error))?;
                let start_time_millis =
                    Self::timestamp(start_time_millis_inc.value(row), "start_time_millis_inc")?;
                let end_time_millis =
                    Self::timestamp(end_time_millis_exc.value(row), "end_time_millis_exc")?;
                if end_time_millis < start_time_millis {
                    return Err(Self::invalid_value(
                        "end_time_millis_exc",
                        end_time_millis,
                        "end time precedes start time",
                    ));
                }
                let duration = duration_millis.value(row);
                if duration
                    != i64::try_from(end_time_millis - start_time_millis)
                        .map_err(|error| Self::invalid_value("duration_millis", duration, error))?
                {
                    return Err(Self::invalid_value(
                        "duration_millis",
                        duration,
                        "does not match the candle window",
                    ));
                }
                let ohlcv = MarketOhlcv::new(
                    open.value(row),
                    high.value(row),
                    low.value(row),
                    close.value(row),
                    volume.value(row),
                    quote_volume.value(row),
                )
                .map_err(|error| Self::invalid_value("ohlcv", "candle row", error))?;
                if ohlcv.vwap() != vwap.value(row) {
                    return Err(Self::invalid_value(
                        "vwap",
                        vwap.value(row),
                        format!("does not match the OHLCV-derived VWAP {}", ohlcv.vwap()),
                    ));
                }
                let candle = Candle {
                    key: CandleKey::new(exchange, TradingPair::new(base, quote), interval),
                    start_time_millis_inc: start_time_millis,
                    end_time_millis_exc: end_time_millis,
                    is_candle_closed: is_candle_closed.value(row),
                    ohlcv,
                    trade_count: trade_count.value(row),
                    is_complete: is_complete.value(row),
                };
                let decoded_revision = CandleRevision::decode(revision.value(row).as_bytes())?;
                if decoded_revision.key != candle.key
                    || decoded_revision.window_start != candle.start_time()
                    || decoded_revision.window_end != candle.end_time()
                {
                    return Err(Self::invalid_value(
                        "revision",
                        revision.value(row),
                        "revision identity does not match the candle row",
                    ));
                }
                // The explicit columns are the canonical provenance. The JSON
                // revision may be retained for readers that need the full key,
                // but it must agree with those columns before the row is used.
                let decoded = ClosedCandle::new(
                    candle,
                    TimestampMilliseconds(close_watermark.value(row)),
                    source_offset.value(row),
                    state_epoch.value(row),
                );
                if decoded.message_id() != revision_id.value(row) {
                    return Err(Self::invalid_value(
                        "revision_id",
                        revision_id.value(row),
                        format!("does not match candle revision {}", decoded.message_id()),
                    ));
                }
                let expected_routing_key = crate::routing_key::RoutingKey::for_pair(
                    decoded.candle().key.exchange,
                    &decoded.candle().key.trading_pair,
                );
                if routing_key.value(row) != expected_routing_key.value() {
                    return Err(Self::invalid_value(
                        "routing_key",
                        routing_key.value(row),
                        "does not match the candle's trading pair",
                    ));
                }
                Ok(decoded)
            })
            .collect()
    }

    fn array<'a>(batch: &'a RecordBatch, name: &str) -> BuildResult<&'a ArrayRef> {
        batch
            .column_by_name(name)
            .ok_or_else(|| Self::column_error(name))
    }

    fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> BuildResult<&'a StringArray> {
        Self::array(batch, name)?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Self::column_error(name))
    }

    fn i64_column<'a>(batch: &'a RecordBatch, name: &str) -> BuildResult<&'a Int64Array> {
        Self::array(batch, name)?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Self::column_error(name))
    }

    fn u64_column<'a>(batch: &'a RecordBatch, name: &str) -> BuildResult<&'a UInt64Array> {
        Self::array(batch, name)?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| Self::column_error(name))
    }

    fn f64_column<'a>(batch: &'a RecordBatch, name: &str) -> BuildResult<&'a Float64Array> {
        Self::array(batch, name)?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| Self::column_error(name))
    }

    fn boolean_column<'a>(batch: &'a RecordBatch, name: &str) -> BuildResult<&'a BooleanArray> {
        Self::array(batch, name)?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| Self::column_error(name))
    }

    fn parse<T>(value: &str, column: &str) -> BuildResult<T>
    where
        T: FromStr,
        T::Err: Display,
    {
        value
            .parse()
            .map_err(|error| Self::invalid_value(column, value, error))
    }

    fn timestamp(value: i64, column: &str) -> BuildResult<u64> {
        u64::try_from(value).map_err(|error| Self::invalid_value(column, value, error))
    }

    fn invalid_value(column: &str, value: impl Display, reason: impl Display) -> BuildError {
        BuildError::io(
            "decode archived parquet",
            column.to_string(),
            std::io::Error::new(
                ErrorKind::InvalidData,
                format!("column {column} has invalid value {value}: {reason}"),
            ),
        )
    }

    fn column_error(name: &str) -> BuildError {
        BuildError::io(
            "decode archived parquet",
            name.to_string(),
            std::io::Error::new(
                ErrorKind::InvalidData,
                format!("column {name} is missing or has an unexpected type"),
            ),
        )
    }
}
