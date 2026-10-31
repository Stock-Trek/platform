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

    /// Returns `true` when [`Self::push`] would suppress the record.
    pub fn would_duplicate(&self, record: &ArchiveRecord) -> bool {
        self.seen.contains(&record.key())
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
