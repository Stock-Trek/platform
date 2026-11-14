use std::cmp::Ordering;
use stock_trek_types::cex::cex_id::CexId;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ArchiveRecordKey {
    MarketEvent {
        exchange: CexId,
        stream: String,
        sequence: Option<u64>,
        payload: String,
        state_epoch: u64,
    },
    ClosedCandle {
        revision_id: String,
    },
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
