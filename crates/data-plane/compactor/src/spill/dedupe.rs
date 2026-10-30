use super::run::SpillRun;
use platform_archive::{
    ArchiveBatch, ArchiveKind, ArchiveParquet, ArchiveParquetBatches, ArchiveRecord,
    ArchiveRecordKey,
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
            let Some(candidate) = self.heap.pop() else {
                break;
            };
            self.advance(candidate.run)?;
            if candidate.run > best.run {
                best = candidate;
            }
        }

        Ok(Some(best.record))
    }
}
