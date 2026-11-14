use platform_archive::{
    ArchiveBatch, ArchiveKind, ArchivePath, ArchiveRecord, ArchiveSortKey, SpillRun, SpillRunReader,
};
use platform_errors::{PlatformError, PlatformResult};
use std::{cmp::Ordering, collections::BinaryHeap, path::PathBuf, vec::IntoIter};

/// A record plus the cached values used to order its compaction output.
pub struct PartitionItem {
    pub partition: String,
    pub sort_key: ArchiveSortKey,
    pub record: ArchiveRecord,
}

impl PartitionItem {
    fn new(record: ArchiveRecord) -> Self {
        let partition =
            ArchivePath::compacted_partition(&record.partition(), record.routing_bucket());
        Self {
            partition,
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
    pub fn new(
        kind: ArchiveKind,
        directory: impl Into<PathBuf>,
        limit: usize,
    ) -> PlatformResult<Self> {
        let directory = directory.into();
        std::fs::create_dir_all(&directory).map_err(|error| {
            PlatformError::io(
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

    pub fn push(&mut self, record: ArchiveRecord) -> PlatformResult<()> {
        self.records.push(PartitionItem::new(record));
        if self.records.len() >= self.limit {
            self.spill()?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> PlatformResult<PartitionRecords> {
        if self.runs.is_empty() {
            self.records.sort_by(PartitionItem::compare);
            return Ok(PartitionRecords::Memory(self.records.into_iter()));
        }

        self.spill()?;
        let readers = self
            .runs
            .iter()
            .map(|path| SpillRunReader::open(self.kind, path))
            .collect::<PlatformResult<Vec<_>>>()?;
        Ok(PartitionRecords::Runs(PartitionMerge::new(readers)?))
    }

    fn spill(&mut self) -> PlatformResult<()> {
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
    pub fn next_item(&mut self) -> PlatformResult<Option<PartitionItem>> {
        match self {
            Self::Memory(items) => Ok(items.next()),
            Self::Runs(merge) => merge.next_item(),
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
    runs: Vec<SpillRunReader>,
    heap: BinaryHeap<PartitionEntry>,
}

impl PartitionMerge {
    fn new(mut readers: Vec<SpillRunReader>) -> PlatformResult<Self> {
        let mut heap = BinaryHeap::with_capacity(readers.len());
        for (index, run) in readers.iter_mut().enumerate() {
            if let Some(record) = run.next_record()? {
                heap.push(PartitionEntry {
                    item: PartitionItem::new(record),
                    run: index,
                });
            }
        }
        Ok(Self {
            runs: readers,
            heap,
        })
    }

    fn advance(&mut self, run: usize) -> PlatformResult<()> {
        if let Some(record) = self.runs[run].next_record()? {
            self.heap.push(PartitionEntry {
                item: PartitionItem::new(record),
                run,
            });
        }
        Ok(())
    }

    fn next_item(&mut self) -> PlatformResult<Option<PartitionItem>> {
        let Some(entry) = self.heap.pop() else {
            return Ok(None);
        };
        self.advance(entry.run)?;
        Ok(Some(entry.item))
    }
}
