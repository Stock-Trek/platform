use super::{
    SpillUsage,
    spill_codec::{SpillCodec, SpillReader, SpillWriter},
    spill_merge::SpillRecords,
    spill_sorter::SpillSorter,
};
use platform_errors::{BuildError, BuildResult};
use platform_market_data::{decoders::trade::Trade, event::MarketEvent};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{BufRead, BufWriter, Write},
    path::{Path, PathBuf},
};

pub type PairKey = (String, String, String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayRecord {
    pub pair: PairKey,
    pub event: MarketEvent,
    pub trades: Vec<Trade>,
}

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

#[derive(Clone)]
struct ReplayCodec;

impl SpillCodec for ReplayCodec {
    type Record = ReplayRecord;
    type Key = ReplayKey;
    type Reader = ReplayRunReader;
    type Writer = ReplayRunWriter;

    fn key(&self, record: &Self::Record) -> Self::Key {
        record.key()
    }

    fn label(&self) -> &'static str {
        "event"
    }

    fn extension(&self) -> &'static str {
        "jsonl"
    }

    fn dedupe_merges(&self) -> bool {
        false
    }

    fn prefer_latest_run(&self) -> bool {
        false
    }

    fn open(&self, path: &Path, _batch: usize) -> BuildResult<Self::Reader> {
        ReplayRunReader::open(path)
    }

    fn create(&self, path: &Path, _batch: usize) -> BuildResult<Self::Writer> {
        ReplayRunWriter::create(path)
    }
}

struct ReplayRunReader {
    lines: std::io::Lines<std::io::BufReader<File>>,
}

impl ReplayRunReader {
    fn open(path: &Path) -> BuildResult<Self> {
        let file = File::open(path).map_err(|error| {
            BuildError::io("open rebuild spill run", path.display().to_string(), error)
        })?;
        Ok(Self {
            lines: std::io::BufReader::new(file).lines(),
        })
    }
}

impl SpillReader for ReplayRunReader {
    type Record = ReplayRecord;

    fn read(&mut self) -> BuildResult<Option<Self::Record>> {
        loop {
            let Some(line) = self.lines.next() else {
                return Ok(None);
            };
            let line = line
                .map_err(|error| BuildError::io("read rebuild spill run", String::new(), error))?;
            if line.is_empty() {
                continue;
            }
            return serde_json::from_str(&line)
                .map(Some)
                .map_err(BuildError::external);
        }
    }
}

struct ReplayRunWriter {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl ReplayRunWriter {
    fn create(path: &Path) -> BuildResult<Self> {
        let file = File::create(path).map_err(|error| {
            BuildError::io(
                "create rebuild spill run",
                path.display().to_string(),
                error,
            )
        })?;
        Ok(Self {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
        })
    }
}

impl SpillWriter for ReplayRunWriter {
    type Record = ReplayRecord;

    fn write(&mut self, record: Self::Record) -> BuildResult<()> {
        serde_json::to_writer(&mut self.writer, &record).map_err(BuildError::external)?;
        self.writer.write_all(b"\n").map_err(|error| {
            BuildError::io(
                "write rebuild spill run",
                self.path.display().to_string(),
                error,
            )
        })
    }

    fn finish(mut self) -> BuildResult<()> {
        self.writer.flush().map_err(|error| {
            BuildError::io(
                "flush rebuild spill run",
                self.path.display().to_string(),
                error,
            )
        })
    }
}

pub struct ReplaySpill {
    sorter: SpillSorter<ReplayCodec>,
}

impl ReplaySpill {
    pub fn new(directory: impl Into<PathBuf>, limit: usize) -> BuildResult<Self> {
        Ok(Self {
            sorter: SpillSorter::with_codec(directory, limit, ReplayCodec)?,
        })
    }

    pub fn push(&mut self, record: ReplayRecord) -> BuildResult<()> {
        self.sorter.push_record(record)
    }

    pub fn spilled_records(&self) -> usize {
        self.sorter.spilled_records()
    }

    pub fn usage(&self) -> SpillUsage {
        self.sorter.usage()
    }

    pub fn finish(&mut self) -> BuildResult<ReplayRecords> {
        Ok(ReplayRecords {
            records: self.sorter.finish()?,
        })
    }
}

pub struct ReplayRecords {
    records: SpillRecords<ReplayCodec>,
}

impl ReplayRecords {
    pub fn next_record(&mut self) -> BuildResult<Option<ReplayRecord>> {
        self.records.next_record()
    }
}
