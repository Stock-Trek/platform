use crate::{
    candle::{AlignedWindow, key::CandleKey},
    decoders::Trade,
};
use platform_errors::{PlatformError, PlatformResult};
use platform_types::TimestampMilliseconds;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use stock_trek::markets::MarketOhlcv;
use stock_trek::markets::TimestampMillis;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{field} value {value} cannot be represented as a finite f64")]
struct DecimalConversionError {
    field: &'static str,
    value: Decimal,
}

#[derive(Debug, Error)]
enum CandleTimestampError {
    #[error("candle timestamp {0} is outside the supported range")]
    OutOfRange(u64),
    #[error("candle end timestamp {end} precedes start timestamp {start}")]
    EndBeforeStart { start: u64, end: u64 },
}

#[derive(Deserialize)]
struct CandleWire {
    key: CandleKey,
    start_time_millis_inc: u64,
    end_time_millis_exc: u64,
    is_candle_closed: bool,
    ohlcv: MarketOhlcv,
    trade_count: u64,
    is_complete: bool,
}

/// An aggregated candle together with its exchange and interval identity.
///
/// The candle carries stock-trek's [`MarketOhlcv`] body while remaining mutable,
/// because aggregation folds trades into an open candle in place.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "CandleWire")]
pub struct Candle {
    pub key: CandleKey,
    pub start_time_millis_inc: u64,
    pub end_time_millis_exc: u64,
    pub is_candle_closed: bool,
    pub ohlcv: MarketOhlcv,
    pub trade_count: u64,
    /// Whether every source event that belongs to this window was observed.
    ///
    /// Candles start complete. A gap marker that covers the window marks it
    /// incomplete before it closes, and the flag is carried by the published
    /// closed candle. While the candle remains in the correction contract,
    /// observing every missing source sequence restores the flag and
    /// republishes the corrected candle. A gap that is never backfilled remains
    /// permanently incomplete.
    pub is_complete: bool,
}

impl TryFrom<CandleWire> for Candle {
    type Error = CandleTimestampError;

    fn try_from(wire: CandleWire) -> Result<Self, Self::Error> {
        let start = wire.start_time_millis_inc;
        let end = wire.end_time_millis_exc;
        Self::validate_timestamp(start)?;
        Self::validate_timestamp(end)?;
        if end < start {
            return Err(CandleTimestampError::EndBeforeStart { start, end });
        }

        Ok(Self {
            key: wire.key,
            start_time_millis_inc: start,
            end_time_millis_exc: end,
            is_candle_closed: wire.is_candle_closed,
            ohlcv: wire.ohlcv,
            trade_count: wire.trade_count,
            is_complete: wire.is_complete,
        })
    }
}

impl Candle {
    pub fn open(trade: &Trade, interval: AlignedWindow) -> PlatformResult<Self> {
        let price = Self::to_f64("price", trade.price)?;
        let quantity = Self::to_f64("quantity", trade.quantity)?;
        let quote_volume = price * quantity;
        let bounds = Self::window_bounds(trade.timestamp, interval)?;

        Ok(Self {
            key: CandleKey::new(trade.exchange, trade.trading_pair, interval),
            start_time_millis_inc: bounds.0,
            end_time_millis_exc: bounds.1,
            is_candle_closed: false,
            ohlcv: MarketOhlcv::new(price, price, price, price, quantity, quote_volume)
                .map_err(|error| PlatformError::candle_conversion("ohlcv", error))?,
            trade_count: 1,
            is_complete: true,
        })
    }

    /// Whether every source event that belongs to this candle was observed.
    pub fn is_complete(&self) -> bool {
        self.is_complete
    }

    /// Whether a source gap affected this candle.
    pub fn is_incomplete(&self) -> bool {
        !self.is_complete
    }

    /// Marks this candle as affected by a source gap.
    pub fn mark_incomplete(&mut self) {
        self.is_complete = false;
    }

    /// Restores the completeness flag after a gap has been fully resolved.
    pub fn mark_complete(&mut self) {
        self.is_complete = true;
    }

    pub fn apply_trade(&mut self, trade: &Trade) -> PlatformResult<()> {
        let price = Self::to_f64("price", trade.price)?;
        let quantity = Self::to_f64("quantity", trade.quantity)?;

        self.ohlcv
            .apply_trade(price, quantity)
            .map_err(|error| PlatformError::candle_conversion("trade", error))?;
        self.trade_count = self.trade_count.saturating_add(1);
        Ok(())
    }

    pub fn close(&mut self) {
        self.is_candle_closed = true;
    }

    pub fn start_time(&self) -> TimestampMilliseconds {
        Self::to_timestamp(self.start_time_millis_inc)
    }

    pub fn end_time(&self) -> TimestampMilliseconds {
        Self::to_timestamp(self.end_time_millis_exc)
    }

    pub fn duration_millis(&self) -> u64 {
        self.end_time_millis_exc
            .saturating_sub(self.start_time_millis_inc)
    }

    fn window_bounds(
        timestamp: TimestampMilliseconds,
        interval: AlignedWindow,
    ) -> PlatformResult<(u64, u64)> {
        let timestamp = TimestampMillis::try_from(timestamp.0)
            .map_err(|error| PlatformError::candle_conversion("timestamp", error))?;
        let bounds = interval
            .bounds_for(timestamp)
            .map_err(|error| PlatformError::candle_conversion("timestamp", error))?;
        let start_millis = bounds.lower().as_millis();
        let end_millis = bounds.upper().as_millis();
        Self::validate_timestamp(start_millis)
            .and_then(|_| Self::validate_timestamp(end_millis))
            .map_err(|error| PlatformError::candle_conversion("timestamp", error))?;
        Ok((start_millis, end_millis))
    }

    fn to_f64(field: &'static str, value: Decimal) -> PlatformResult<f64> {
        value
            .to_f64()
            .filter(|value| value.is_finite())
            .ok_or_else(|| {
                PlatformError::candle_conversion(field, DecimalConversionError { field, value })
            })
    }

    fn validate_timestamp(value: u64) -> Result<(), CandleTimestampError> {
        i64::try_from(value)
            .map(|_| ())
            .map_err(|_| CandleTimestampError::OutOfRange(value))
    }

    fn to_timestamp(value: u64) -> TimestampMilliseconds {
        TimestampMilliseconds(
            i64::try_from(value).expect("validated candle timestamps must fit in i64"),
        )
    }
}
