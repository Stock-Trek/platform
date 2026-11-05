use platform_archive::{
    ArchiveBatch, ArchiveBucket, ArchiveKind, ArchiveRecord, ArchiveRecordKey, ArchiveSortKey,
    ArchiveTableLayer, RoutingBucket,
    spill::{SpillRun, SpillRunReader},
};
use platform_errors::{BpError, BpResult};
use platform_market_data::candle::ClosedCandle;
use std::{cmp::Ordering, collections::BinaryHeap, path::PathBuf, vec::IntoIter};

/// The sink's object order: archive bucket, routing bucket, compacted
/// partition, record sort key, and record identity. Two candle records
/// compare equal exactly when they carry the same deterministic revision id.
type CandleOrderKey = (
    ArchiveBucket,
    RoutingBucket,
    Option<String>,
    ArchiveSortKey,
    ArchiveRecordKey,
);

fn order_key(
    record: &ArchiveRecord,
    bucket_millis: i64,
    layer: ArchiveTableLayer,
) -> CandleOrderKey {
    (
        ArchiveBucket::new(record, bucket_millis),
        record.routing_bucket(),
        match layer {
            ArchiveTableLayer::Raw => None,
            ArchiveTableLayer::Compacted => Some(record.partition().path()),
        },
        record.sort_key(),
        record.key(),
    )
}

/// Bounded external sorter for regenerated candles.
///
/// Once the in-memory entries reach the configured record limit they are
/// sorted by the sink's object order and spilled to a Parquet run. `finish`
/// merges the runs back in that order, suppressing duplicate revisions, so
/// candles stream to the sink without ever being fully materialized.
pub struct CandleSpill {
    directory: PathBuf,
    limit: usize,
    bucket_millis: i64,
    layer: ArchiveTableLayer,
    entries: Vec<(CandleOrderKey, ArchiveRecord)>,
    runs: Vec<PathBuf>,
    spilled: usize,
}

impl CandleSpill {
    pub fn new(
        directory: impl Into<PathBuf>,
        bucket_millis: i64,
        layer: ArchiveTableLayer,
        limit: usize,
    ) -> BpResult<Self> {
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
            bucket_millis,
            layer,
            entries: Vec::new(),
            runs: Vec::new(),
            spilled: 0,
        })
    }

    pub fn push(&mut self, candle: ClosedCandle) -> BpResult<()> {
        let record = ArchiveRecord::ClosedCandle(candle);
        let key = order_key(&record, self.bucket_millis, self.layer);
        self.entries.push((key, record));
        if self.entries.len() >= self.limit {
            self.spill()?;
        }
        Ok(())
    }

    pub fn spilled_records(&self) -> usize {
        self.spilled
    }

    pub fn finish(mut self) -> BpResult<CandleRecords> {
        if self.runs.is_empty() {
            self.entries
                .sort_by(|(left, _), (right, _)| left.cmp(right));
            let records = keep_latest(self.entries.into_iter().map(|(_, record)| record));
            return Ok(CandleRecords::Memory(records.into_iter()));
        }

        self.spill()?;
        let readers = self
            .runs
            .iter()
            .map(|path| SpillRunReader::open(ArchiveKind::Candles, path))
            .collect::<BpResult<Vec<_>>>()?;
        Ok(CandleRecords::Runs(CandleMerge::new(
            readers,
            self.bucket_millis,
            self.layer,
        )?))
    }

    fn spill(&mut self) -> BpResult<()> {
        if self.entries.is_empty() {
            return Ok(());
        }

        self.entries
            .sort_by(|(left, _), (right, _)| left.cmp(right));
        let records = keep_latest(self.entries.drain(..).map(|(_, record)| record));
        self.spilled += records.len();
        let path = self
            .directory
            .join(format!("candle-run-{}.parquet", self.runs.len()));
        SpillRun::write(&path, &ArchiveBatch::new(ArchiveKind::Candles, records)?)?;
        self.runs.push(path);
        Ok(())
    }
}

/// Drops every record whose revision id equals its successor's, keeping the
/// latest generated revision of each candle.
fn keep_latest(records: impl Iterator<Item = ArchiveRecord>) -> Vec<ArchiveRecord> {
    let mut kept = Vec::new();
    let mut pending: Option<ArchiveRecord> = None;
    for record in records {
        match pending.take() {
            Some(previous) if message_id(&previous) == message_id(&record) => {}
            Some(previous) => kept.push(previous),
            None => {}
        }
        pending = Some(record);
    }
    kept.extend(pending);
    kept
}

fn message_id(record: &ArchiveRecord) -> String {
    match record {
        ArchiveRecord::ClosedCandle(candle) => candle.message_id(),
        ArchiveRecord::MarketEvent(_) => String::new(),
    }
}

/// Candle records in sink order, de-duplicated by revision id.
pub enum CandleRecords {
    Memory(IntoIter<ArchiveRecord>),
    Runs(CandleMerge),
}

impl CandleRecords {
    pub fn next_candle(&mut self) -> BpResult<Option<ClosedCandle>> {
        let Some(record) = self.next_record()? else {
            return Ok(None);
        };
        match record {
            ArchiveRecord::ClosedCandle(candle) => Ok(Some(candle)),
            ArchiveRecord::MarketEvent(_) => Err(BpError::io(
                "read rebuilt candles",
                "rebuild spill merge",
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "event record in candle spill merge",
                ),
            )),
        }
    }

    fn next_record(&mut self) -> BpResult<Option<ArchiveRecord>> {
        match self {
            Self::Memory(records) => Ok(records.next()),
            Self::Runs(merge) => merge.next_record(),
        }
    }
}

struct CandleEntry {
    key: CandleOrderKey,
    run: usize,
    record: ArchiveRecord,
}

impl PartialEq for CandleEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for CandleEntry {}

impl PartialOrd for CandleEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CandleEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // `BinaryHeap` is a max-heap, so reverse the key ordering to pop the
        // smallest key first. For equal keys, prefer the latest run.
        other
            .key
            .cmp(&self.key)
            .then_with(|| self.run.cmp(&other.run))
    }
}

pub struct CandleMerge {
    runs: Vec<SpillRunReader>,
    heap: BinaryHeap<CandleEntry>,
    bucket_millis: i64,
    layer: ArchiveTableLayer,
}

impl CandleMerge {
    fn new(
        mut runs: Vec<SpillRunReader>,
        bucket_millis: i64,
        layer: ArchiveTableLayer,
    ) -> BpResult<Self> {
        let mut heap = BinaryHeap::with_capacity(runs.len());
        for (index, run) in runs.iter_mut().enumerate() {
            if let Some(record) = run.next_record()? {
                heap.push(CandleEntry {
                    key: order_key(&record, bucket_millis, layer),
                    run: index,
                    record,
                });
            }
        }
        Ok(Self {
            runs,
            heap,
            bucket_millis,
            layer,
        })
    }

    fn advance(&mut self, run: usize) -> BpResult<()> {
        if let Some(record) = self.runs[run].next_record()? {
            self.heap.push(CandleEntry {
                key: order_key(&record, self.bucket_millis, self.layer),
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
