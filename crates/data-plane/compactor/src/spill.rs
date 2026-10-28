use platform_archive::{
    ArchiveBatch, ArchiveKind, ArchiveParquet, ArchiveParquetBatches, ArchiveParquetWriter,
    ArchiveRecord, ArchiveRecordKey,
};
use platform_errors::{BpError, BpResult};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    fs::File,
    path::{Path, PathBuf},
    vec::IntoIter,
};

/// De-duplicates records while keeping only a bounded number of them in memory.
///
/// Once the in-memory map reaches the configured limit it is sorted by record
/// key and spilled to a Parquet run. The runs are merged back in order at the
/// end, selecting the record from the latest run for duplicate keys.
pub struct DedupeSpill {
    kind: ArchiveKind,
    directory: PathBuf,
    limit: usize,
    records: HashMap<ArchiveRecordKey, ArchiveRecord>,
    runs: Vec<PathBuf>,
}

impl DedupeSpill {
    pub fn new(kind: ArchiveKind, directory: impl Into<PathBuf>, limit: usize) -> BpResult<Self> {
        let directory = directory.into();
        std::fs::create_dir_all(&directory).map_err(|error| {
            BpError::io(
                "create compaction spill directory",
                directory.display().to_string(),
                error,
            )
        })?;
        Ok(Self {
            kind,
            directory,
            limit: limit.max(1),
            records: HashMap::new(),
            runs: Vec::new(),
        })
    }

    pub fn push(&mut self, record: ArchiveRecord) -> BpResult<()> {
        self.records.insert(record.key(), record);
        if self.records.len() >= self.limit {
            self.spill()?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> BpResult<DedupedRecords> {
        if self.runs.is_empty() {
            let mut entries = self.records.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let records = entries
                .into_iter()
                .map(|(_, record)| record)
                .collect::<Vec<_>>();
            return Ok(DedupedRecords::Memory(records.into_iter()));
        }

        self.spill()?;
        let readers = self
            .runs
            .iter()
            .map(|path| RunReader::open(self.kind, path))
            .collect::<BpResult<Vec<_>>>()?;
        Ok(DedupedRecords::Runs(DedupeMerge::new(readers)?))
    }

    fn spill(&mut self) -> BpResult<()> {
        if self.records.is_empty() {
            return Ok(());
        }

        let mut entries = self.records.drain().collect::<Vec<_>>();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        let records = entries
            .into_iter()
            .map(|(_, record)| record)
            .collect::<Vec<_>>();
        let path = self
            .directory
            .join(format!("dedupe-run-{}.parquet", self.runs.len()));
        SpillRun::write(&path, &ArchiveBatch::new(self.kind, records)?)?;
        self.runs.push(path);
        Ok(())
    }
}

pub enum DedupedRecords {
    Memory(IntoIter<ArchiveRecord>),
    Runs(DedupeMerge),
}

impl DedupedRecords {
    pub fn next_record(&mut self) -> BpResult<Option<ArchiveRecord>> {
        match self {
            Self::Memory(records) => Ok(records.next()),
            Self::Runs(merge) => merge.next_record(),
        }
    }
}

struct RunReader {
    batches: ArchiveParquetBatches,
    buffer: IntoIter<ArchiveRecord>,
}

impl RunReader {
    fn open(kind: ArchiveKind, path: &Path) -> BpResult<Self> {
        let file = File::open(path).map_err(|error| {
            BpError::io(
                "open compaction spill run",
                path.display().to_string(),
                error,
            )
        })?;
        Ok(Self {
            batches: ArchiveParquet::decode_batches(kind, file)?,
            buffer: Vec::new().into_iter(),
        })
    }

    fn next_record(&mut self) -> BpResult<Option<ArchiveRecord>> {
        loop {
            if let Some(record) = self.buffer.next() {
                return Ok(Some(record));
            }
            match self.batches.next() {
                Some(batch) => self.buffer = batch?.into_records().into_iter(),
                None => return Ok(None),
            }
        }
    }
}

struct DedupeEntry {
    key: ArchiveRecordKey,
    run: usize,
    record: ArchiveRecord,
}

impl PartialEq for DedupeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for DedupeEntry {}

impl PartialOrd for DedupeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DedupeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // `BinaryHeap` is a max-heap, so reverse the key ordering to pop the
        // smallest key first. For equal keys, prefer the latest run.
        other
            .key
            .cmp(&self.key)
            .then_with(|| self.run.cmp(&other.run))
    }
}

pub struct DedupeMerge {
    runs: Vec<RunReader>,
    heap: BinaryHeap<DedupeEntry>,
}

impl DedupeMerge {
    fn new(mut runs: Vec<RunReader>) -> BpResult<Self> {
        let mut heap = BinaryHeap::with_capacity(runs.len());
        for (index, run) in runs.iter_mut().enumerate() {
            if let Some(record) = run.next_record()? {
                heap.push(DedupeEntry {
                    key: record.key(),
                    run: index,
                    record,
                });
            }
        }
        Ok(Self { runs, heap })
    }

    fn advance(&mut self, run: usize) -> BpResult<()> {
        if let Some(record) = self.runs[run].next_record()? {
            self.heap.push(DedupeEntry {
                key: record.key(),
                run,
                record,
            });
        }
        Ok(())
    }

    fn next_record(&mut self) -> BpResult<Option<ArchiveRecord>> {
        let Some(mut best) = self.heap.pop() else {
            return Ok(None);
        };
        self.advance(best.run)?;

        while self
            .heap
            .peek()
            .is_some_and(|candidate| candidate.key == best.key)
        {
            let candidate = self.heap.pop().expect("peeked heap entry");
            self.advance(candidate.run)?;
            if candidate.run > best.run {
                best = candidate;
            }
        }

        Ok(Some(best.record))
    }
}

/// A record plus the cached values used to order its compaction output.
pub struct PartitionItem {
    pub partition: String,
    pub sort_key: (i64, u64),
    pub record: ArchiveRecord,
}

impl PartitionItem {
    fn new(record: ArchiveRecord) -> Self {
        Self {
            partition: record.partition().path(),
            sort_key: record.sort_key(),
            record,
        }
    }

    fn compare(&self, other: &Self) -> Ordering {
        self.partition
            .cmp(&other.partition)
            .then_with(|| self.sort_key.cmp(&other.sort_key))
    }
}

/// Sorts and spills records by output partition, keeping the in-memory working
/// set bounded by the configured limit.
pub struct PartitionSpill {
    kind: ArchiveKind,
    directory: PathBuf,
    limit: usize,
    records: Vec<PartitionItem>,
    runs: Vec<PathBuf>,
}

impl PartitionSpill {
    pub fn new(kind: ArchiveKind, directory: impl Into<PathBuf>, limit: usize) -> BpResult<Self> {
        let directory = directory.into();
        std::fs::create_dir_all(&directory).map_err(|error| {
            BpError::io(
                "create compaction spill directory",
                directory.display().to_string(),
                error,
            )
        })?;
        Ok(Self {
            kind,
            directory,
            limit: limit.max(1),
            records: Vec::new(),
            runs: Vec::new(),
        })
    }

    pub fn push(&mut self, record: ArchiveRecord) -> BpResult<()> {
        self.records.push(PartitionItem::new(record));
        if self.records.len() >= self.limit {
            self.spill()?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> BpResult<PartitionRecords> {
        if self.runs.is_empty() {
            self.records.sort_by(PartitionItem::compare);
            return Ok(PartitionRecords::Memory(self.records.into_iter()));
        }

        self.spill()?;
        let readers = self
            .runs
            .iter()
            .map(|path| PartitionRunReader::open(self.kind, path))
            .collect::<BpResult<Vec<_>>>()?;
        Ok(PartitionRecords::Runs(PartitionMerge::new(readers)?))
    }

    fn spill(&mut self) -> BpResult<()> {
        if self.records.is_empty() {
            return Ok(());
        }

        let mut items = std::mem::take(&mut self.records);
        items.sort_by(PartitionItem::compare);
        let records = items
            .into_iter()
            .map(|item| item.record)
            .collect::<Vec<_>>();
        let path = self
            .directory
            .join(format!("partition-run-{}.parquet", self.runs.len()));
        SpillRun::write(&path, &ArchiveBatch::new(self.kind, records)?)?;
        self.runs.push(path);
        Ok(())
    }
}

pub enum PartitionRecords {
    Memory(IntoIter<PartitionItem>),
    Runs(PartitionMerge),
}

impl PartitionRecords {
    pub fn next_item(&mut self) -> BpResult<Option<PartitionItem>> {
        match self {
            Self::Memory(items) => Ok(items.next()),
            Self::Runs(merge) => merge.next_item(),
        }
    }
}

struct PartitionRunReader {
    batches: ArchiveParquetBatches,
    buffer: IntoIter<ArchiveRecord>,
}

impl PartitionRunReader {
    fn open(kind: ArchiveKind, path: &Path) -> BpResult<Self> {
        let file = File::open(path).map_err(|error| {
            BpError::io(
                "open compaction spill run",
                path.display().to_string(),
                error,
            )
        })?;
        Ok(Self {
            batches: ArchiveParquet::decode_batches(kind, file)?,
            buffer: Vec::new().into_iter(),
        })
    }

    fn next_item(&mut self) -> BpResult<Option<PartitionItem>> {
        loop {
            if let Some(record) = self.buffer.next() {
                return Ok(Some(PartitionItem::new(record)));
            }
            match self.batches.next() {
                Some(batch) => self.buffer = batch?.into_records().into_iter(),
                None => return Ok(None),
            }
        }
    }
}

struct PartitionEntry {
    item: PartitionItem,
    run: usize,
}

impl PartialEq for PartitionEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for PartitionEntry {}

impl PartialOrd for PartitionEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PartitionEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .item
            .partition
            .cmp(&self.item.partition)
            .then_with(|| other.item.sort_key.cmp(&self.item.sort_key))
            .then_with(|| self.run.cmp(&other.run))
    }
}

pub struct PartitionMerge {
    runs: Vec<PartitionRunReader>,
    heap: BinaryHeap<PartitionEntry>,
}

impl PartitionMerge {
    fn new(mut runs: Vec<PartitionRunReader>) -> BpResult<Self> {
        let mut heap = BinaryHeap::with_capacity(runs.len());
        for (index, run) in runs.iter_mut().enumerate() {
            if let Some(item) = run.next_item()? {
                heap.push(PartitionEntry { item, run: index });
            }
        }
        Ok(Self { runs, heap })
    }

    fn advance(&mut self, run: usize) -> BpResult<()> {
        if let Some(item) = self.runs[run].next_item()? {
            self.heap.push(PartitionEntry { item, run });
        }
        Ok(())
    }

    fn next_item(&mut self) -> BpResult<Option<PartitionItem>> {
        let Some(entry) = self.heap.pop() else {
            return Ok(None);
        };
        self.advance(entry.run)?;
        Ok(Some(entry.item))
    }
}

struct SpillRun;

impl SpillRun {
    fn write(path: &Path, batch: &ArchiveBatch) -> BpResult<()> {
        let file = File::create(path).map_err(|error| {
            BpError::io(
                "create compaction spill run",
                path.display().to_string(),
                error,
            )
        })?;
        let mut writer = ArchiveParquetWriter::try_new(batch.kind(), file)?;
        writer.write(batch)?;
        writer.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use platform_market_data::event::MarketEvent;
    use platform_types::new_types::TimestampMilliseconds;
    use stock_trek_types::cex::cex_id::CexId;

    fn event(stream: &str, sequence: u64, timestamp: i64, received_at: i64) -> ArchiveRecord {
        ArchiveRecord::MarketEvent(MarketEvent {
            exchange: CexId::Binance,
            stream: stream.to_string(),
            sequence: Some(sequence),
            exchange_timestamp: Some(TimestampMilliseconds(timestamp)),
            received_at: TimestampMilliseconds(received_at),
            payload: format!("payload-{sequence}"),
        })
    }

    #[test]
    fn dedupe_spill_prefers_records_from_later_runs() {
        let directory = tempfile::tempdir().unwrap();
        let mut spill = DedupeSpill::new(ArchiveKind::Events, directory.path(), 2).unwrap();

        spill.push(event("trades", 1, 1_000, 1_000)).unwrap();
        spill.push(event("trades", 2, 2_000, 2_000)).unwrap();
        spill.push(event("trades", 1, 1_500, 1_500)).unwrap();

        let mut deduped = spill.finish().unwrap();
        let first = deduped.next_record().unwrap().unwrap();
        let second = deduped.next_record().unwrap().unwrap();
        assert!(deduped.next_record().unwrap().is_none());

        let ArchiveRecord::MarketEvent(first) = first else {
            panic!("expected a market event");
        };
        let ArchiveRecord::MarketEvent(second) = second else {
            panic!("expected a market event");
        };
        assert_eq!(first.sequence, Some(1));
        assert_eq!(first.received_at, TimestampMilliseconds(1_500));
        assert_eq!(second.sequence, Some(2));
    }

    #[test]
    fn partition_spill_merges_runs_in_partition_and_sort_order() {
        let directory = tempfile::tempdir().unwrap();
        let mut spill = PartitionSpill::new(ArchiveKind::Events, directory.path(), 1).unwrap();

        spill.push(event("b", 1, 20_000, 20_000)).unwrap();
        spill.push(event("a", 2, 30_000, 30_000)).unwrap();
        spill.push(event("a", 3, 10_000, 10_000)).unwrap();

        let mut records = spill.finish().unwrap();
        let mut partitions = Vec::new();
        let mut timestamps = Vec::new();
        while let Some(item) = records.next_item().unwrap() {
            partitions.push(item.partition);
            timestamps.push(item.sort_key.0);
        }

        assert_eq!(
            partitions,
            vec!["kind=events/exchange=binance/stream=a"; 2]
                .into_iter()
                .chain(std::iter::once("kind=events/exchange=binance/stream=b"))
                .map(str::to_string)
                .collect::<Vec<_>>()
        );
        assert_eq!(timestamps, vec![10_000, 30_000, 20_000]);
    }
}
