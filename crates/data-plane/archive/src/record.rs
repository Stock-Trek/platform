use platform_market_data::{
    candle::{CandleKey, ClosedCandle},
    event::MarketEvent,
};
use platform_types::new_types::TimestampMilliseconds;
use serde::{Deserialize, Serialize};
use stock_trek_types::cex::cex_id::CexId;

/// The kind of market data stored in an archive file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArchiveKind {
    Events,
    Candles,
}

impl ArchiveKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Events => "events",
            Self::Candles => "candles",
        }
    }

    pub fn from_segment(segment: &str) -> Option<Self> {
        match segment.strip_prefix("kind=").unwrap_or(segment) {
            "events" => Some(Self::Events),
            "candles" => Some(Self::Candles),
            _ => None,
        }
    }
}

/// A record waiting to be written to, or read from, the archive.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "record_type", rename_all = "snake_case")]
pub enum ArchiveRecord {
    MarketEvent(MarketEvent),
    ClosedCandle(ClosedCandle),
}

impl ArchiveRecord {
    pub fn kind(&self) -> ArchiveKind {
        match self {
            Self::MarketEvent(_) => ArchiveKind::Events,
            Self::ClosedCandle(_) => ArchiveKind::Candles,
        }
    }

    /// The exchange timestamp that determines the archive bucket.
    pub fn timestamp(&self) -> TimestampMilliseconds {
        match self {
            Self::MarketEvent(event) => event.exchange_timestamp.unwrap_or(event.received_at),
            Self::ClosedCandle(closed) => closed.candle.start_time(),
        }
    }

    /// Orders records within a compacted partition.
    pub fn sort_key(&self) -> (i64, u64) {
        match self {
            Self::MarketEvent(event) => (
                event.exchange_timestamp.unwrap_or(event.received_at).0,
                event.sequence.unwrap_or_default(),
            ),
            Self::ClosedCandle(closed) => (closed.candle.start_time().0, closed.revision),
        }
    }

    /// The output partition for a compacted record.
    pub fn partition(&self) -> ArchivePartition {
        match self {
            Self::MarketEvent(event) => ArchivePartition::Events {
                exchange: event.exchange,
                stream: event.stream.clone(),
            },
            Self::ClosedCandle(closed) => ArchivePartition::Candles {
                candle: closed.candle.key.clone(),
            },
        }
    }

    /// The identity used to de-duplicate a record.
    pub fn key(&self) -> ArchiveRecordKey {
        match self {
            Self::MarketEvent(event) => ArchiveRecordKey::MarketEvent {
                exchange: event.exchange,
                stream: event.stream.clone(),
                sequence: event.sequence,
                payload: event.payload.clone(),
            },
            Self::ClosedCandle(closed) => ArchiveRecordKey::ClosedCandle {
                candle: closed.candle.key.clone(),
                start_time: closed.candle.start_time(),
                revision: closed.revision,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ArchiveRecordKey {
    MarketEvent {
        exchange: CexId,
        stream: String,
        sequence: Option<u64>,
        payload: String,
    },
    ClosedCandle {
        candle: CandleKey,
        start_time: TimestampMilliseconds,
        revision: u64,
    },
}

/// The output partition for a compacted record.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ArchivePartition {
    Events { exchange: CexId, stream: String },
    Candles { candle: CandleKey },
}

impl ArchivePartition {
    pub fn path(&self) -> String {
        match self {
            Self::Events { exchange, stream } => format!(
                "kind=events/exchange={}/stream={}",
                Self::segment(&exchange.to_string()),
                Self::segment(stream)
            ),
            Self::Candles { candle } => format!(
                "kind=candles/exchange={}/base={}/quote={}/interval={}",
                Self::segment(&candle.exchange.to_string()),
                Self::segment(&candle.trading_pair.base.to_string()),
                Self::segment(&candle.trading_pair.quote.to_string()),
                Self::segment(&candle.interval.label())
            ),
        }
    }

    fn segment(value: &str) -> String {
        let value = value.trim().replace(['/', '\\'], "_");
        if value.is_empty() {
            "unknown".to_string()
        } else {
            value
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use platform_market_data::candle::CandleInterval;
    use stock_trek::markets::{MarketCandle, MarketOhlcv};
    use stock_trek_types::cex::{asset_id::AssetId, trading_pair::TradingPair};

    fn event() -> ArchiveRecord {
        ArchiveRecord::MarketEvent(MarketEvent {
            exchange: CexId::Binance,
            stream: "btc/usdt@trade".to_string(),
            sequence: Some(3),
            exchange_timestamp: Some(TimestampMilliseconds(120_000)),
            received_at: TimestampMilliseconds(130_000),
            payload: "payload".to_string(),
        })
    }

    fn closed_candle() -> ArchiveRecord {
        ArchiveRecord::ClosedCandle(ClosedCandle {
            revision: 4,
            candle: platform_market_data::candle::Candle {
                key: CandleKey::new(
                    CexId::Binance,
                    TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
                    CandleInterval::from_label("1m").unwrap(),
                ),
                market_candle: MarketCandle::new(
                    60_000,
                    120_000,
                    60_000,
                    true,
                    MarketOhlcv::new(1.0, 2.0, 0.5, 1.5, 10.0, 15.0, 1.5),
                    5,
                ),
            },
        })
    }

    #[test]
    fn events_and_candles_use_their_own_timestamps_and_kinds() {
        let event = event();
        assert_eq!(event.kind(), ArchiveKind::Events);
        assert_eq!(event.timestamp(), TimestampMilliseconds(120_000));
        assert_eq!(event.sort_key(), (120_000, 3));

        let candle = closed_candle();
        assert_eq!(candle.kind(), ArchiveKind::Candles);
        assert_eq!(candle.timestamp(), TimestampMilliseconds(60_000));
        assert_eq!(candle.sort_key(), (60_000, 4));
    }

    #[test]
    fn partitions_sanitize_path_segments() {
        let partition = event().partition();
        assert_eq!(
            partition.path(),
            "kind=events/exchange=binance/stream=btc_usdt@trade"
        );

        let partition = closed_candle().partition();
        assert_eq!(
            partition.path(),
            "kind=candles/exchange=binance/base=bitcoin/quote=tether_usd/interval=1m"
        );
    }
}
