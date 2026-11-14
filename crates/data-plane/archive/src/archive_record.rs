use crate::{
    archive_kind::ArchiveKind,
    archive_partition::ArchivePartition,
    archive_record_key::ArchiveRecordKey,
    archive_sort_key::ArchiveSortKey,
    archived_market_event::ArchivedMarketEvent,
    routing_key::{RoutingBucket, RoutingKey},
};
use platform_market_data::ClosedCandle;
use platform_types::TimestampMilliseconds;
use serde::{Deserialize, Serialize};

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

    pub fn timestamp(&self) -> TimestampMilliseconds {
        match self {
            Self::MarketEvent(event) => event.exchange_timestamp.unwrap_or(event.received_at),
            Self::ClosedCandle(closed) => closed.candle().start_time(),
        }
    }

    pub fn sort_key(&self) -> ArchiveSortKey {
        match self {
            Self::MarketEvent(event) => ArchiveSortKey::MarketEvent {
                timestamp: event.exchange_timestamp.unwrap_or(event.received_at).0,
                sequence: event.sequence.unwrap_or_default(),
                state_epoch: event.state_epoch,
            },
            Self::ClosedCandle(closed) => ArchiveSortKey::ClosedCandle {
                start_time: closed.candle().start_time().0,
                close_watermark: closed.revision().close_watermark.0,
                source_offset: closed.revision().source_offset,
                state_epoch: closed.revision().state_epoch,
            },
        }
    }

    pub fn routing_key(&self) -> RoutingKey {
        match self {
            Self::MarketEvent(event) => event.routing_key(),
            Self::ClosedCandle(closed) => RoutingKey::for_pair(
                closed.candle().key.exchange,
                &closed.candle().key.trading_pair,
            ),
        }
    }

    pub fn routing_bucket(&self) -> RoutingBucket {
        RoutingBucket::from_key(self.routing_key())
    }

    pub fn partition(&self) -> ArchivePartition {
        match self {
            Self::MarketEvent(event) => ArchivePartition::Events {
                exchange: event.exchange,
                stream: event.stream.clone(),
            },
            Self::ClosedCandle(closed) => ArchivePartition::Candles {
                candle: closed.candle().key.clone(),
            },
        }
    }

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
