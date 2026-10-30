use crate::decoders::trade::Trade;
use platform_errors::{BpError, BpResult};
use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use std::fmt;
use stock_trek::markets::{MarketCandle, MarketOhlcv};
use stock_trek_types::cex::{cex_id::CexId, trading_pair::TradingPair};
use thiserror::Error;

const MILLIS_PER_SECOND: i64 = 1_000;
const MILLIS_PER_MINUTE: i64 = 60 * MILLIS_PER_SECOND;
const MILLIS_PER_HOUR: i64 = 60 * MILLIS_PER_MINUTE;
const MILLIS_PER_DAY: i64 = 24 * MILLIS_PER_HOUR;

#[derive(Debug, Error)]
#[error("{field} value {value} cannot be represented as a finite f64")]
struct DecimalConversionError {
    field: &'static str,
    value: Decimal,
}

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

/// Identity of an open candle. One candle is maintained per exchange, trading
/// pair, and interval.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CandleKey {
    pub exchange: CexId,
    pub trading_pair: TradingPair,
    pub interval: CandleInterval,
}

impl CandleKey {
    pub fn new(exchange: CexId, trading_pair: TradingPair, interval: CandleInterval) -> Self {
        Self {
            exchange,
            trading_pair,
            interval,
        }
    }

    /// The subject/key segment used when publishing this candle.
    pub fn subject_suffix(&self) -> String {
        format!(
            "{}.{}.{}.{}",
            self.exchange, self.trading_pair.base, self.trading_pair.quote, self.interval
        )
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
    /// Whether every source event that belongs to this window was observed.
    ///
    /// Candles start complete. A gap marker that covers the window marks it
    /// incomplete before it closes, and the flag is carried by the published
    /// closed candle.
    pub is_complete: bool,
}

/// Deterministic revision of a closed candle.
///
/// A revision is a pure function of the candle identity and window, the source
/// watermark that closed the candle, the source offset that advanced that
/// watermark, and the shard state epoch. Replaying the same input log
/// therefore recreates the same revision; there is no process-local or global
/// revision counter to race across replicas.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CandleRevision {
    /// Candle identity this revision belongs to.
    pub key: CandleKey,
    /// Inclusive window start.
    pub window_start: TimestampMilliseconds,
    /// Exclusive window end.
    pub window_end: TimestampMilliseconds,
    /// Event-time watermark at which the candle was closed.
    pub close_watermark: TimestampMilliseconds,
    /// Source offset that advanced the watermark to `close_watermark`.
    pub source_offset: u64,
    /// State epoch that produced this revision.
    pub state_epoch: u64,
}

impl PartialOrd for CandleRevision {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CandleRevision {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key
            .subject_suffix()
            .cmp(&other.key.subject_suffix())
            .then_with(|| self.window_start.cmp(&other.window_start))
            .then_with(|| self.window_end.cmp(&other.window_end))
            .then_with(|| self.close_watermark.cmp(&other.close_watermark))
            .then_with(|| self.source_offset.cmp(&other.source_offset))
            .then_with(|| self.state_epoch.cmp(&other.state_epoch))
    }
}

impl CandleRevision {
    /// Derives the revision for `candle` from the closing watermark and source
    /// offset that are part of the shard state.
    pub fn for_candle(
        candle: &Candle,
        close_watermark: TimestampMilliseconds,
        source_offset: u64,
        state_epoch: u64,
    ) -> Self {
        Self {
            key: candle.key.clone(),
            window_start: candle.start_time(),
            window_end: candle.end_time(),
            close_watermark,
            source_offset,
            state_epoch,
        }
    }

    /// Stable id for the published closed-candle message.
    ///
    /// The id covers the candle key, its window, and the state epoch. Revision
    /// provenance is included as well, so a replay of the same processing step
    /// publishes the same id while a genuinely new revision is not hidden by
    /// JetStream de-duplication.
    pub fn message_id(&self) -> String {
        format!(
            "{}@{}:{}.epoch={}.watermark={}.offset={}",
            self.key.subject_suffix(),
            self.window_start.0,
            self.window_end.0,
            self.state_epoch,
            self.close_watermark.0,
            self.source_offset,
        )
    }
}

/// A candle that has left its aggregation window and is ready to be archived.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosedCandle {
    /// Deterministic revision assigned when the candle closed.
    pub revision: CandleRevision,
    pub candle: Candle,
}

impl ClosedCandle {
    /// Builds a closed candle and derives its deterministic revision.
    pub fn new(
        candle: Candle,
        close_watermark: TimestampMilliseconds,
        source_offset: u64,
        state_epoch: u64,
    ) -> Self {
        let revision =
            CandleRevision::for_candle(&candle, close_watermark, source_offset, state_epoch);
        Self { revision, candle }
    }

    /// Stable id used when publishing this candle.
    pub fn message_id(&self) -> String {
        self.revision.message_id()
    }

    /// Whether every source event that belongs to this candle was observed.
    pub fn is_complete(&self) -> bool {
        self.candle.is_complete
    }

    /// Whether a source gap affected this candle.
    pub fn is_incomplete(&self) -> bool {
        !self.is_complete()
    }

    /// Marks this candle as affected by a source gap.
    pub fn mark_incomplete(&mut self) {
        self.candle.mark_incomplete();
    }
}

impl std::ops::Deref for ClosedCandle {
    type Target = Candle;

    fn deref(&self) -> &Self::Target {
        &self.candle
    }
}

impl std::ops::DerefMut for ClosedCandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.candle
    }
}

impl Candle {
    pub fn open(trade: &Trade, interval: CandleInterval) -> BpResult<Self> {
        let price = Self::to_f64("price", trade.price)?;
        let quantity = Self::to_f64("quantity", trade.quantity)?;
        let quote_volume = price * quantity;
        let start_time = interval.start_for(trade.timestamp);
        let end_time = interval.end_for(trade.timestamp);

        Ok(Self {
            key: CandleKey::new(trade.exchange, trade.trading_pair, interval),
            market_candle: MarketCandle::new(
                Self::to_millis(start_time.0),
                Self::to_millis(end_time.0),
                Self::to_millis(interval.duration().0),
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

    fn to_millis(value: i64) -> u64 {
        u64::try_from(value).unwrap_or_default()
    }

    fn to_timestamp(value: u64) -> TimestampMilliseconds {
        TimestampMilliseconds(i64::try_from(value).unwrap_or(i64::MAX))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use stock_trek_types::cex::asset_id::AssetId;

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str_exact(value).unwrap()
    }

    fn trade(price: &str, quantity: &str, timestamp: i64) -> Trade {
        Trade {
            exchange: CexId::Binance,
            trading_pair: TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
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
        let mut candle = Candle::open(&trade("100", "2", 61_000), interval).unwrap();
        candle.apply_trade(&trade("110", "1", 62_000)).unwrap();
        candle.apply_trade(&trade("90", "3", 63_000)).unwrap();

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
    fn candles_start_complete_and_can_be_marked_incomplete() {
        let interval = CandleInterval::from_label("1m").unwrap();
        let mut candle = Candle::open(&trade("100", "2", 61_000), interval).unwrap();
        assert!(candle.is_complete());
        assert!(!candle.is_incomplete());

        candle.mark_incomplete();
        assert!(candle.is_incomplete());

        candle.mark_complete();
        assert!(candle.is_complete());
    }

    #[test]
    fn key_uses_asset_ids_and_canonical_interval() {
        let key = CandleKey::new(
            CexId::Binance,
            TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            CandleInterval::from_label("60s").unwrap(),
        );
        assert_eq!(key.subject_suffix(), "binance.bitcoin.tether_usd.1m");
    }

    fn closed_candle(offset: u64, epoch: u64) -> ClosedCandle {
        let interval = CandleInterval::from_label("1m").unwrap();
        let mut candle = Candle::open(&trade("100", "2", 61_000), interval).unwrap();
        candle.close();
        ClosedCandle::new(candle, TimestampMilliseconds(121_000), offset, epoch)
    }

    #[test]
    fn closed_candle_revisions_are_deterministic() {
        let first = closed_candle(7, 0);
        let replay = closed_candle(7, 0);

        assert_eq!(first.revision, replay.revision);
        assert_eq!(first.message_id(), replay.message_id());
        assert_eq!(first.revision.window_start, TimestampMilliseconds(60_000));
        assert_eq!(first.revision.window_end, TimestampMilliseconds(120_000));
        assert_eq!(
            first.revision.close_watermark,
            TimestampMilliseconds(121_000)
        );
    }

    #[test]
    fn closed_candle_ids_cover_key_window_epoch_and_revision() {
        let base = closed_candle(7, 0);

        let other_offset = closed_candle(8, 0);
        let other_epoch = closed_candle(7, 1);
        let mut other_key = closed_candle(7, 0);
        let other_interval = CandleInterval::from_label("5m").unwrap();
        other_key.candle.key.interval = other_interval;
        other_key.revision.key.interval = other_interval;

        assert_ne!(base.message_id(), other_offset.message_id());
        assert_ne!(base.message_id(), other_epoch.message_id());
        assert_ne!(base.message_id(), other_key.message_id());
    }
}
