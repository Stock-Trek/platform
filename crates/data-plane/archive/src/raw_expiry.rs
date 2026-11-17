use platform_types::TimestampMilliseconds;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawDeletionDecision {
    Delete,
    Retain,
    Blocked,
    Quarantine,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawExpiryInput {
    pub now: TimestampMilliseconds,
    pub processed_at: TimestampMilliseconds,
    pub manifest_modified_at: TimestampMilliseconds,
    pub retention: Duration,
    pub compacted_durable: bool,
    pub manifest_readable: bool,
    pub raw_table_entry_removed: bool,
    pub earlier_stages_complete: bool,
}

impl RawExpiryInput {
    pub fn decide(self) -> RawDeletionDecision {
        if !self.manifest_readable {
            return RawDeletionDecision::Quarantine;
        }
        if !self.compacted_durable {
            return RawDeletionDecision::Blocked;
        }
        if !self.earlier_stages_complete || !self.raw_table_entry_removed {
            return RawDeletionDecision::Blocked;
        }
        let retention_millis = i64::try_from(self.retention.as_millis()).unwrap_or(i64::MAX);
        let ledger_ready = self.processed_at.0.saturating_add(retention_millis) <= self.now.0;
        let manifest_ready =
            self.manifest_modified_at.0.saturating_add(retention_millis) <= self.now.0;
        if ledger_ready && manifest_ready {
            RawDeletionDecision::Delete
        } else {
            RawDeletionDecision::Retain
        }
    }
}
