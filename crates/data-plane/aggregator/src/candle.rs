use crate::trade::Trade;
use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Ohlcv {
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub quote_volume: Decimal,
    pub vwap: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Candle {
    pub key: CandleKey,
    pub start_time: TimestampMilliseconds,
    pub end_time: TimestampMilliseconds,
    pub duration: DurationMilliseconds,
    pub is_candle_closed: bool,
    pub ohlcv: Ohlcv,
    pub trade_count: u64,
}

impl Candle {
    pub fn open(trade: &Trade, interval: CandleInterval) -> Self {
        let quote_volume = trade.price * trade.quantity;
        Self {
            key: CandleKey::new(trade.exchange, trade.symbol.clone(), interval),
            start_time: interval.start_for(trade.timestamp),
            end_time: interval.end_for(trade.timestamp),
            duration: interval.duration(),
            is_candle_closed: false,
            ohlcv: Ohlcv {
                open: trade.price,
                high: trade.price,
                low: trade.price,
                close: trade.price,
                volume: trade.quantity,
                quote_volume,
                vwap: trade.price,
            },
            trade_count: 1,
        }
    }

    pub fn apply_trade(&mut self, trade: &Trade) {
        self.ohlcv.high = self.ohlcv.high.max(trade.price);
        self.ohlcv.low = self.ohlcv.low.min(trade.price);
        self.ohlcv.close = trade.price;
        self.ohlcv.volume += trade.quantity;
        self.ohlcv.quote_volume += trade.price * trade.quantity;
        self.ohlcv.vwap = self
            .ohlcv
            .quote_volume
            .checked_div(self.ohlcv.volume)
            .unwrap_or(Decimal::ZERO);
        self.trade_count = self.trade_count.saturating_add(1);
    }

    pub fn close(&mut self) {
        self.is_candle_closed = true;
    }
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

        assert_eq!(candle.ohlcv.open, decimal("100"));
        assert_eq!(candle.ohlcv.high, decimal("110"));
        assert_eq!(candle.ohlcv.low, decimal("90"));
        assert_eq!(candle.ohlcv.close, decimal("90"));
        assert_eq!(candle.ohlcv.volume, decimal("6"));
        assert_eq!(candle.ohlcv.quote_volume, decimal("580"));
        assert_eq!(candle.ohlcv.vwap, decimal("96.66666666666666666666666667"));
        assert_eq!(candle.trade_count, 3);
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
