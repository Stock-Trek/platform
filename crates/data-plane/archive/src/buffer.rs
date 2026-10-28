use crate::record::{ArchiveRecord, ArchiveRecordKey};
use std::collections::{HashSet, VecDeque};

/// Buffers records for a periodic Parquet flush while suppressing redeliveries.
pub struct ArchiveBuffer {
    records: Vec<ArchiveRecord>,
    seen: HashSet<ArchiveRecordKey>,
    order: VecDeque<ArchiveRecordKey>,
    max_records: usize,
    dedupe_capacity: usize,
    duplicates: u64,
}

impl ArchiveBuffer {
    pub fn new(max_records: usize, dedupe_capacity: usize) -> Self {
        Self {
            records: Vec::new(),
            seen: HashSet::new(),
            order: VecDeque::new(),
            max_records,
            dedupe_capacity,
            duplicates: 0,
        }
    }

    /// Adds a record unless the same record is already buffered or was recently
    /// archived.
    pub fn push(&mut self, record: ArchiveRecord) -> bool {
        let key = record.key();
        if !self.seen.insert(key.clone()) {
            self.duplicates = self.duplicates.saturating_add(1);
            return false;
        }

        self.order.push_back(key);
        while self.order.len() > self.dedupe_capacity {
            if let Some(expired) = self.order.pop_front() {
                self.seen.remove(&expired);
            }
        }
        self.records.push(record);
        true
    }

    pub fn should_flush(&self) -> bool {
        self.records.len() >= self.max_records
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn records(&self) -> &[ArchiveRecord] {
        &self.records
    }

    /// Borrows the buffered records for an in-place sink write. The records
    /// remain buffered until the sink succeeds and the caller clears them.
    pub fn records_mut(&mut self) -> &mut [ArchiveRecord] {
        &mut self.records
    }

    pub fn clear_records(&mut self) {
        self.records.clear();
    }

    pub fn duplicates(&self) -> u64 {
        self.duplicates
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use platform_market_data::event::MarketEvent;
    use platform_types::new_types::TimestampMilliseconds;
    use stock_trek_types::cex::cex_id::CexId;

    fn event(sequence: u64) -> ArchiveRecord {
        ArchiveRecord::MarketEvent(MarketEvent {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            sequence: Some(sequence),
            exchange_timestamp: Some(TimestampMilliseconds(1_700_000_000_000)),
            received_at: TimestampMilliseconds(1_700_000_000_123),
            payload: format!("payload-{sequence}"),
        })
    }

    #[test]
    fn duplicate_records_are_suppressed() {
        let mut buffer = ArchiveBuffer::new(10, 10);

        assert!(buffer.push(event(1)));
        assert!(!buffer.push(event(1)));
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.duplicates(), 1);
    }

    #[test]
    fn dedupe_keys_expire_but_buffered_records_remain() {
        let mut buffer = ArchiveBuffer::new(10, 1);

        assert!(buffer.push(event(1)));
        assert!(buffer.push(event(2)));
        assert_eq!(buffer.len(), 2);
        assert!(buffer.push(event(1)));
        assert_eq!(buffer.len(), 3);
    }

    #[test]
    fn flush_is_requested_at_the_record_limit() {
        let mut buffer = ArchiveBuffer::new(2, 10);

        assert!(!buffer.should_flush());
        buffer.push(event(1));
        assert!(!buffer.should_flush());
        buffer.push(event(2));
        assert!(buffer.should_flush());
    }
}
