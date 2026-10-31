use crate::routing::{RoutingBucket, RoutingKey};
use platform_market_data::{
    candle::{CandleKey, ClosedCandle},
    event::MarketEvent,
};
use platform_types::new_types::TimestampMilliseconds;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, ops::Deref};
use stock_trek_types::cex::cex_id::CexId;

/// Header used by event producers to tag raw events with their state epoch.
pub const STATE_EPOCH_HEADER: &str = "Market-Data-State-Epoch";

/// The kind of market data stored in an archive file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

/// A raw market event together with the state epoch that processed it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArchivedMarketEvent {
    pub event: MarketEvent,
    pub state_epoch: u64,
}

impl ArchivedMarketEvent {
    pub fn new(event: MarketEvent, state_epoch: u64) -> Self {
        Self { event, state_epoch }
    }

    pub fn event(&self) -> &MarketEvent {
        &self.event
    }

    pub fn state_epoch(&self) -> u64 {
        self.state_epoch
    }

    /// The stable archive routing key for this event's trading pair.
    pub fn routing_key(&self) -> RoutingKey {
        RoutingKey::for_event(&self.event)
    }
}

impl From<(MarketEvent, u64)> for ArchivedMarketEvent {
    fn from((event, state_epoch): (MarketEvent, u64)) -> Self {
        Self::new(event, state_epoch)
    }
}

impl From<MarketEvent> for ArchivedMarketEvent {
    fn from(event: MarketEvent) -> Self {
        Self::new(event, 0)
    }
}

impl Deref for ArchivedMarketEvent {
    type Target = MarketEvent;

    fn deref(&self) -> &Self::Target {
        &self.event
    }
}

/// A record waiting to be written to, or read from, the archive.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "record_type", rename_all = "snake_case")]
pub enum ArchiveRecord {
    MarketEvent(ArchivedMarketEvent),
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
    ///
    /// Closed candles are ordered by their window and then by every field of
    /// their deterministic revision. Compaction therefore keeps revisions of
    /// the same candle in a stable order instead of collapsing them just because
    /// they share a start time and source offset.
    pub fn sort_key(&self) -> ArchiveSortKey {
        match self {
            Self::MarketEvent(event) => ArchiveSortKey::MarketEvent {
                timestamp: event.exchange_timestamp.unwrap_or(event.received_at).0,
                sequence: event.sequence.unwrap_or_default(),
                state_epoch: event.state_epoch,
            },
            Self::ClosedCandle(closed) => ArchiveSortKey::ClosedCandle {
                start_time: closed.candle.start_time().0,
                close_watermark: closed.revision.close_watermark.0,
                source_offset: closed.revision.source_offset,
                state_epoch: closed.revision.state_epoch,
            },
        }
    }

    /// The stable archive routing key for this record's trading pair.
    pub fn routing_key(&self) -> RoutingKey {
        match self {
            Self::MarketEvent(event) => event.routing_key(),
            Self::ClosedCandle(closed) => {
                RoutingKey::for_pair(closed.candle.key.exchange, &closed.candle.key.trading_pair)
            }
        }
    }

    /// The virtual archive bucket that contains this record.
    pub fn routing_bucket(&self) -> RoutingBucket {
        self.routing_key().bucket()
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
                state_epoch: event.state_epoch,
            },
            Self::ClosedCandle(closed) => ArchiveRecordKey::ClosedCandle {
                revision_id: closed.message_id(),
            },
        }
    }
}

/// Stable ordering for archive records inside one compacted partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ArchiveSortKey {
    MarketEvent {
        timestamp: i64,
        sequence: u64,
        state_epoch: u64,
    },
    ClosedCandle {
        start_time: i64,
        close_watermark: i64,
        source_offset: u64,
        state_epoch: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ArchiveRecordKey {
    MarketEvent {
        exchange: CexId,
        stream: String,
        sequence: Option<u64>,
        payload: String,
        state_epoch: u64,
    },
    /// Closed candles are de-duplicated by their deterministic revision id.
    ///
    /// The id covers the candle identity, window, state epoch, close watermark,
    /// and source offset. Keeping the revision id rather than a copied candle
    /// body also means a redelivered message cannot create a second archive row
    /// merely because its decoded body was materialized twice.
    ClosedCandle { revision_id: String },
}

impl PartialOrd for ArchiveRecordKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ArchiveRecordKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (
                Self::MarketEvent {
                    exchange,
                    stream,
                    sequence,
                    payload,
                    state_epoch,
                },
                Self::MarketEvent {
                    exchange: other_exchange,
                    stream: other_stream,
                    sequence: other_sequence,
                    payload: other_payload,
                    state_epoch: other_state_epoch,
                },
            ) => exchange
                .to_string()
                .cmp(&other_exchange.to_string())
                .then_with(|| stream.cmp(other_stream))
                .then_with(|| sequence.cmp(other_sequence))
                .then_with(|| payload.cmp(other_payload))
                .then_with(|| state_epoch.cmp(other_state_epoch)),
            (
                Self::ClosedCandle { revision_id },
                Self::ClosedCandle {
                    revision_id: other_revision_id,
                },
            ) => revision_id.cmp(other_revision_id),
            (Self::MarketEvent { .. }, Self::ClosedCandle { .. }) => Ordering::Less,
            (Self::ClosedCandle { .. }, Self::MarketEvent { .. }) => Ordering::Greater,
        }
    }
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
