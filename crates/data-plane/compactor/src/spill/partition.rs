use super::run::SpillRun;
use platform_archive::{
    ArchiveBatch, ArchiveKind, ArchiveParquet, ArchiveParquetBatches, ArchiveRecord,
};
use platform_errors::{BpError, BpResult};
use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    fs::File,
    path::{Path, PathBuf},
    vec::IntoIter,
};

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
