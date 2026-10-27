use platform_market_data::decoders::trade::Trade;
use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::fmt;
use stock_trek::markets::{MarketCandle, MarketOhlcv};
use stock_trek_types::cex::cex_id::CexId;
use thiserror::Error;

const MILLIS_PER_SECOND: i64 = 1_000;
const MILLIS_PER_MINUTE: i64 = 60 * MILLIS_PER_SECOND;
const MILLIS_PER_HOUR: i64 = 60 * MILLIS_PER_MINUTE;
const MILLIS_PER_DAY: i64 = 24 * MILLIS_PER_HOUR;

/// A candle interval such as `1s`, `5m`, or `1h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CandleInterval {
    duration: DurationMilliseconds,
}

impl CandleInterval {
    pub fn from_label(label: &str) -> Result<Self, CandleIntervalError> {
        let label = label.trim().to_ascii_lowercase();
        let split_at = label
            .find(|character: char| !character.is_ascii_digit())
            .ok_or_else(|| CandleIntervalError::InvalidLabel(label.clone()))?;
        let (amount, unit) = label.split_at(split_at);
        let amount: i64 = amount
            .parse()
            .map_err(|_| CandleIntervalError::InvalidLabel(label.clone()))?;
        let multiplier = match unit {
            "ms" => 1,
            "s" => MILLIS_PER_SECOND,
            "m" => MILLIS_PER_MINUTE,
            "h" => MILLIS_PER_HOUR,
            "d" => MILLIS_PER_DAY,
            _ => return Err(CandleIntervalError::InvalidLabel(label)),
        };
        let duration = amount
            .checked_mul(multiplier)
            .filter(|duration| *duration > 0)
            .ok_or(CandleIntervalError::ZeroOrOverflow)?;
        Ok(Self {
            duration: DurationMilliseconds(duration),
        })
    }

    pub fn label(self) -> String {
        let duration = self.duration.0;
        if duration % MILLIS_PER_DAY == 0 {
            format!("{}d", duration / MILLIS_PER_DAY)
        } else if duration % MILLIS_PER_HOUR == 0 {
            format!("{}h", duration / MILLIS_PER_HOUR)
        } else if duration % MILLIS_PER_MINUTE == 0 {
            format!("{}m", duration / MILLIS_PER_MINUTE)
        } else if duration % MILLIS_PER_SECOND == 0 {
            format!("{}s", duration / MILLIS_PER_SECOND)
        } else {
            format!("{duration}ms")
        }
    }

    pub fn duration(self) -> DurationMilliseconds {
        self.duration
    }

    pub fn start_for(self, timestamp: TimestampMilliseconds) -> TimestampMilliseconds {
        TimestampMilliseconds(timestamp.0.div_euclid(self.duration.0) * self.duration.0)
    }

    pub fn end_for(self, timestamp: TimestampMilliseconds) -> TimestampMilliseconds {
        self.start_for(timestamp) + self.duration
    }
}

impl fmt::Display for CandleInterval {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.label())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CandleIntervalError {
    #[error("invalid candle interval {0:?}; expected a value such as 1s, 5m, or 1h")]
    InvalidLabel(String),
    #[error("candle interval must be greater than zero and representable in milliseconds")]
    ZeroOrOverflow,
}

/// Identity of an open candle. One candle is maintained per exchange, symbol,
/// and interval.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CandleKey {
    pub exchange: CexId,
    pub symbol: String,
    pub interval: CandleInterval,
}

impl CandleKey {
    pub fn new(exchange: CexId, symbol: impl Into<String>, interval: CandleInterval) -> Self {
        Self {
            exchange,
            symbol: symbol.into().to_ascii_lowercase(),
            interval,
        }
    }

    /// The subject/key segment used when publishing this candle.
    pub fn subject_suffix(&self) -> String {
        format!("{}.{}.{}", self.exchange, self.symbol, self.interval)
    }
}

impl fmt::Display for CandleKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.subject_suffix())
    }
}

/// An aggregated candle together with its exchange and interval identity.
///
/// The candle body reuses the stock-trek market data DTOs so downstream
/// consumers receive the same representation as other market data producers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    pub key: CandleKey,
    pub market_candle: MarketCandle,
}

impl Candle {
    pub fn open(trade: &Trade, interval: CandleInterval) -> Self {
        let price = trade.price.to_f64().unwrap_or_default();
        let quantity = trade.quantity.to_f64().unwrap_or_default();
        let quote_volume = price * quantity;
        let start_time = interval.start_for(trade.timestamp);
        let end_time = interval.end_for(trade.timestamp);

        Self {
            key: CandleKey::new(trade.exchange, trade.symbol.clone(), interval),
            market_candle: MarketCandle::new(
                to_millis(start_time.0),
                to_millis(end_time.0),
                to_millis(interval.duration().0),
                false,
                MarketOhlcv::new(price, price, price, price, quantity, quote_volume, price),
                1,
            ),
        }
    }

    pub fn apply_trade(&mut self, trade: &Trade) {
        let price = trade.price.to_f64().unwrap_or_default();
        let quantity = trade.quantity.to_f64().unwrap_or_default();
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
    }

    pub fn close(&mut self) {
        self.market_candle.is_candle_closed = true;
    }

    pub fn start_time(&self) -> TimestampMilliseconds {
        to_timestamp(self.market_candle.start_time_millis_inc)
    }

    pub fn end_time(&self) -> TimestampMilliseconds {
        to_timestamp(self.market_candle.end_time_millis_exc)
    }
}

fn to_millis(value: i64) -> u64 {
    u64::try_from(value).unwrap_or_default()
}

fn to_timestamp(value: u64) -> TimestampMilliseconds {
    TimestampMilliseconds(i64::try_from(value).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str_exact(value).unwrap()
    }

    fn trade(price: &str, quantity: &str, timestamp: i64) -> Trade {
        Trade {
            exchange: CexId::Binance,
            symbol: "BTCUSDT".to_string(),
            price: decimal(price),
            quantity: decimal(quantity),
            timestamp: TimestampMilliseconds(timestamp),
        }
    }

    #[test]
    fn intervals_are_parsed_and_canonicalized() {
        let interval = CandleInterval::from_label("60s").unwrap();
        assert_eq!(interval.label(), "1m");
        assert_eq!(interval.duration().0, 60_000);
        assert_eq!(interval.start_for(TimestampMilliseconds(61_234)).0, 60_000);
        assert_eq!(interval.end_for(TimestampMilliseconds(61_234)).0, 120_000);
    }

    #[test]
    fn invalid_intervals_are_rejected() {
        assert!(CandleInterval::from_label("").is_err());
        assert!(CandleInterval::from_label("0s").is_err());
        assert!(CandleInterval::from_label("1w").is_err());
        assert!(CandleInterval::from_label("abc").is_err());
    }

    #[test]
    fn applying_trades_updates_ohlcv() {
        let interval = CandleInterval::from_label("1m").unwrap();
        let mut candle = Candle::open(&trade("100", "2", 61_000), interval);
        candle.apply_trade(&trade("110", "1", 62_000));
        candle.apply_trade(&trade("90", "3", 63_000));

        let ohlcv = &candle.market_candle.ohlcv;
        assert_eq!(ohlcv.open, 100.0);
        assert_eq!(ohlcv.high, 110.0);
        assert_eq!(ohlcv.low, 90.0);
        assert_eq!(ohlcv.close, 90.0);
        assert_eq!(ohlcv.volume, 6.0);
        assert_eq!(ohlcv.quote_volume, 580.0);
        assert!((ohlcv.vwap - 96.666_666_666_666_67).abs() < f64::EPSILON);
        assert_eq!(candle.market_candle.trade_count, 3);
    }

    #[test]
    fn key_uses_lowercase_symbol_and_canonical_interval() {
        let key = CandleKey::new(
            CexId::Binance,
            "BTCUSDT",
            CandleInterval::from_label("60s").unwrap(),
        );
        assert_eq!(key.subject_suffix(), "binance.btcusdt.1m");
    }
}
