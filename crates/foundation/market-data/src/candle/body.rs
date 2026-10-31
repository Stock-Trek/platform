use super::interval::CandleInterval;
use super::key::CandleKey;
use crate::decoders::trade::Trade;
use platform_errors::{BpError, BpResult};
use platform_types::new_types::TimestampMilliseconds;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use stock_trek::markets::{MarketCandle, MarketOhlcv};
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
    market_candle: MarketCandle,
    is_complete: bool,
}

/// An aggregated candle together with its exchange and interval identity.
///
/// The candle body reuses the stock-trek market data DTOs so downstream
/// consumers receive the same representation as other market data producers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "CandleWire")]
pub struct Candle {
    pub key: CandleKey,
    pub market_candle: MarketCandle,
    /// Whether every source event that belongs to this window was observed.
    ///
    /// Candles start complete. A gap marker that covers the window marks it
    /// incomplete before it closes, and the flag is carried by the published
    /// closed candle.
    pub is_complete: bool,
}

impl TryFrom<CandleWire> for Candle {
    type Error = CandleTimestampError;

    fn try_from(wire: CandleWire) -> Result<Self, Self::Error> {
        let start = wire.market_candle.start_time_millis_inc;
        let end = wire.market_candle.end_time_millis_exc;
        Self::validate_timestamp(start)?;
        Self::validate_timestamp(end)?;
        if end < start {
            return Err(CandleTimestampError::EndBeforeStart { start, end });
        }

        Ok(Self {
            key: wire.key,
            market_candle: wire.market_candle,
            is_complete: wire.is_complete,
        })
    }
}

impl Candle {
    pub fn open(trade: &Trade, interval: CandleInterval) -> BpResult<Self> {
        let price = Self::to_f64("price", trade.price)?;
        let quantity = Self::to_f64("quantity", trade.quantity)?;
        let quote_volume = price * quantity;
        let start_time = interval
            .start_for(trade.timestamp)
            .map_err(|error| BpError::candle_conversion("timestamp", error))?;
        let end_time = interval
            .end_for(trade.timestamp)
            .map_err(|error| BpError::candle_conversion("timestamp", error))?;
        let start_millis = Self::to_millis(start_time.0)?;
        let end_millis = Self::to_millis(end_time.0)?;
        let duration_millis = end_millis - start_millis;

        Ok(Self {
            key: CandleKey::new(trade.exchange, trade.trading_pair, interval),
            market_candle: MarketCandle::new(
                start_millis,
                end_millis,
                duration_millis,
                false,
                MarketOhlcv::new(price, price, price, price, quantity, quote_volume, price),
                1,
            ),
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

    pub fn apply_trade(&mut self, trade: &Trade) -> BpResult<()> {
        let price = Self::to_f64("price", trade.price)?;
        let quantity = Self::to_f64("quantity", trade.quantity)?;
        let ohlcv = &mut self.market_candle.ohlcv;

        ohlcv.high = ohlcv.high.max(price);
        ohlcv.low = ohlcv.low.min(price);
        ohlcv.close = price;
        ohlcv.volume += quantity;
        ohlcv.quote_volume += price * quantity;
        ohlcv.vwap = if ohlcv.volume == 0.0 {
            0.0
        } else {
            ohlcv.quote_volume / ohlcv.volume
        };
        self.market_candle.trade_count = self.market_candle.trade_count.saturating_add(1);
        Ok(())
    }

    pub fn close(&mut self) {
        self.market_candle.is_candle_closed = true;
    }

    pub fn start_time(&self) -> TimestampMilliseconds {
        Self::to_timestamp(self.market_candle.start_time_millis_inc)
    }

    pub fn end_time(&self) -> TimestampMilliseconds {
        Self::to_timestamp(self.market_candle.end_time_millis_exc)
    }

    fn to_f64(field: &'static str, value: Decimal) -> BpResult<f64> {
        value
            .to_f64()
            .filter(|value| value.is_finite())
            .ok_or_else(|| {
                BpError::candle_conversion(field, DecimalConversionError { field, value })
            })
    }

    fn to_millis(value: i64) -> BpResult<u64> {
        u64::try_from(value).map_err(|error| BpError::candle_conversion("timestamp", error))
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
