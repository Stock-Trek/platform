use crate::rebuild::{SpillUsage, spill::SpillMerge};
use platform_errors::{BpError, BpResult};
use platform_market_data::{decoders::trade::Trade, event::MarketEvent};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    fs::File,
    io::{BufRead, BufWriter, Write},
    path::{Path, PathBuf},
    vec::IntoIter,
};

/// The decoded trading pair that owns a replay record.
pub type PairKey = (String, String, String);

/// One archived event together with the trades that belong to one pair.
///
/// An event can decode to trades for more than one pair, so the replay stages
/// one record per pair and replays each pair's records through a shared
/// processor, mirroring how the live router groups streams by pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayRecord {
    pub pair: PairKey,
    pub event: MarketEvent,
    pub trades: Vec<Trade>,
}

/// The replay order of a record: pair first, then the deterministic event
/// order the rebuild has always used inside one pair.
pub type ReplayKey = (PairKey, i64, u64, String, String);

impl ReplayRecord {
    pub fn key(&self) -> ReplayKey {
        (
            self.pair.clone(),
            Self::event_timestamp(&self.event).0,
            self.event.sequence.unwrap_or_default(),
            self.event.stream.clone(),
            self.event.payload.clone(),
        )
    }

    pub fn event_timestamp(
        event: &MarketEvent,
    ) -> platform_types::new_types::TimestampMilliseconds {
        event.exchange_timestamp.unwrap_or(event.received_at)
    }
}

/// Bounded external sorter for replay records.
///
/// Once the in-memory entries reach the configured record limit they are
/// sorted by replay key and appended to a spill run. `finish` merges the runs
/// back in replay order, so the caller consumes one pair group at a time
/// without ever holding the whole run in memory.
pub struct ReplaySpill {
    directory: PathBuf,
    limit: usize,
    entries: Vec<(ReplayKey, ReplayRecord)>,
    runs: Vec<PathBuf>,
    generated: usize,
    spilled: usize,
    usage: SpillUsage,
}

impl ReplaySpill {
    pub fn new(directory: impl Into<PathBuf>, limit: usize) -> BpResult<Self> {
        let directory = directory.into();
        std::fs::create_dir_all(&directory).map_err(|error| {
            BpError::io(
                "create rebuild spill directory",
                directory.display().to_string(),
                error,
            )
        })?;
        Ok(Self {
            directory,
            limit: limit.max(1),
            entries: Vec::new(),
            runs: Vec::new(),
            generated: 0,
            spilled: 0,
            usage: SpillUsage::default(),
        })
    }

    pub fn push(&mut self, record: ReplayRecord) -> BpResult<()> {
        let key = record.key();
        self.entries.push((key, record));
        if self.entries.len() >= self.limit {
            self.spill()?;
        }
        Ok(())
    }

    pub fn spilled_records(&self) -> usize {
        self.spilled
    }

    pub fn usage(&self) -> SpillUsage {
        self.usage
    }

    pub fn finish(&mut self) -> BpResult<ReplayRecords> {
        if self.runs.is_empty() {
            self.entries
                .sort_by(|(left, _), (right, _)| left.cmp(right));
            let records = std::mem::take(&mut self.entries)
                .into_iter()
                .map(|(_, record)| record)
                .collect::<Vec<_>>();
            return Ok(ReplayRecords::Memory(records.into_iter()));
        }

        self.spill()?;
        self.reduce_runs(SpillMerge::fanin(self.limit))?;
        let readers = self
            .runs
            .iter()
            .map(|path| ReplayRunReader::open(path))
            .collect::<BpResult<Vec<_>>>()?;
        Ok(ReplayRecords::Runs(ReplayMerge::new(readers)?))
    }

    fn spill(&mut self) -> BpResult<()> {
        if self.entries.is_empty() {
            return Ok(());
        }

        self.entries
            .sort_by(|(left, _), (right, _)| left.cmp(right));
        let path = self.next_run_path("event-run");
        self.write_run(&path)?;
        self.spilled += self.entries.len();
        self.runs.push(path);
        self.entries.clear();
        Ok(())
    }

    fn reduce_runs(&mut self, fan_in: usize) -> BpResult<()> {
        while self.runs.len() > fan_in {
            let sources = std::mem::take(&mut self.runs);
            let mut merged = Vec::new();
            for chunk in sources.chunks(fan_in) {
                if chunk.len() == 1 {
                    merged.push(chunk[0].clone());
                    continue;
                }
                let path = self.next_run_path("event-merge");
                self.merge_runs(chunk, &path)?;
                for source in chunk {
                    std::fs::remove_file(source).map_err(|error| {
                        BpError::io(
                            "remove merged rebuild spill run",
                            source.display().to_string(),
                            error,
                        )
                    })?;
                }
                merged.push(path);
            }
            self.runs = merged;
        }
        Ok(())
    }

    fn next_run_path(&mut self, label: &str) -> PathBuf {
        let path = self
            .directory
            .join(format!("{label}-{}.jsonl", self.generated));
        self.generated += 1;
        path
    }

    fn merge_runs(&mut self, runs: &[PathBuf], path: &Path) -> BpResult<()> {
        let readers = runs
            .iter()
            .map(|run| ReplayRunReader::open(run))
            .collect::<BpResult<Vec<_>>>()?;
        let mut merge = ReplayMerge::new(readers)?;
        let file = File::create(path).map_err(|error| {
            BpError::io(
                "create rebuild spill run",
                path.display().to_string(),
                error,
            )
        })?;
        let mut writer = BufWriter::new(file);
        while let Some(record) = merge.next_record()? {
            serde_json::to_writer(&mut writer, &record).map_err(BpError::external)?;
            writer.write_all(b"\n").map_err(|error| {
                BpError::io("write rebuild spill run", path.display().to_string(), error)
            })?;
        }
        writer.flush().map_err(|error| {
            BpError::io("flush rebuild spill run", path.display().to_string(), error)
        })?;
        self.usage.record(path)
    }

    fn write_run(&mut self, path: &Path) -> BpResult<()> {
        let file = File::create(path).map_err(|error| {
            BpError::io(
                "create rebuild spill run",
                path.display().to_string(),
                error,
            )
        })?;
        let mut writer = BufWriter::new(file);
        for (_, record) in &self.entries {
            serde_json::to_writer(&mut writer, record).map_err(BpError::external)?;
            writer.write_all(b"\n").map_err(|error| {
                BpError::io("write rebuild spill run", path.display().to_string(), error)
            })?;
        }
        writer.flush().map_err(|error| {
            BpError::io("flush rebuild spill run", path.display().to_string(), error)
        })?;
        self.usage.record(path)
    }
}

pub enum ReplayRecords {
    Memory(IntoIter<ReplayRecord>),
    Runs(ReplayMerge),
}

impl ReplayRecords {
    pub fn next_record(&mut self) -> BpResult<Option<ReplayRecord>> {
        match self {
            Self::Memory(records) => Ok(records.next()),
            Self::Runs(merge) => merge.next_record(),
        }
    }
}

struct ReplayRunReader {
    lines: std::io::Lines<std::io::BufReader<File>>,
}

impl ReplayRunReader {
    fn open(path: &Path) -> BpResult<Self> {
        let file = File::open(path).map_err(|error| {
            BpError::io("open rebuild spill run", path.display().to_string(), error)
        })?;
        Ok(Self {
            lines: std::io::BufReader::new(file).lines(),
        })
    }

    fn next_record(&mut self) -> BpResult<Option<ReplayRecord>> {
        loop {
            let Some(line) = self.lines.next() else {
                return Ok(None);
            };
            let line =
                line.map_err(|error| BpError::io("read rebuild spill run", String::new(), error))?;
            if line.is_empty() {
                continue;
            }
            return serde_json::from_str(&line)
                .map(Some)
                .map_err(BpError::external);
        }
    }
}

struct ReplayEntry {
    key: ReplayKey,
    run: usize,
    record: ReplayRecord,
}

impl PartialEq for ReplayEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for ReplayEntry {}

impl PartialOrd for ReplayEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReplayEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // `BinaryHeap` is a max-heap, so reverse the key ordering to pop the
        // smallest key first. For equal keys, prefer the earliest run so
        // duplicates are emitted in read order, exactly like the stable sort
        // of an in-memory group.
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.run.cmp(&self.run))
    }
}

pub struct ReplayMerge {
    runs: Vec<ReplayRunReader>,
    heap: BinaryHeap<ReplayEntry>,
}

impl ReplayMerge {
    fn new(mut runs: Vec<ReplayRunReader>) -> BpResult<Self> {
        let mut heap = BinaryHeap::with_capacity(runs.len());
        for (index, run) in runs.iter_mut().enumerate() {
            if let Some(record) = run.next_record()? {
                heap.push(ReplayEntry {
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
            self.heap.push(ReplayEntry {
                key: record.key(),
                run,
                record,
            });
        }
        Ok(())
    }

    fn next_record(&mut self) -> BpResult<Option<ReplayRecord>> {
        let Some(entry) = self.heap.pop() else {
            return Ok(None);
        };
        self.advance(entry.run)?;
        Ok(Some(entry.record))
    }
}
