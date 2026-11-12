use crate::rebuild::{
    SpillUsage,
    spill_codec::{SpillCodec, SpillReader, SpillWriter},
    spill_merge::SpillRecords,
    spill_sorter::SpillSorter,
};
use platform_archive::{
    ArchiveBucket, ArchiveKind, ArchiveRecord, ArchiveRecordKey, ArchiveSortKey, ArchiveTableLayer,
    RoutingBucket,
    spill::{SpillRunReader, SpillRunWriter},
};
use platform_errors::{BuildError, BuildResult};
use platform_market_data::candle::ClosedCandle;
use std::path::PathBuf;

type CandleOrderKey = (
    ArchiveBucket,
    RoutingBucket,
    Option<String>,
    ArchiveSortKey,
    ArchiveRecordKey,
);

#[derive(Clone)]
struct CandleCodec {
    bucket_millis: i64,
    layer: ArchiveTableLayer,
}

impl CandleCodec {
    fn order_key(&self, record: &ArchiveRecord) -> CandleOrderKey {
        (
            ArchiveBucket::new(record, self.bucket_millis),
            record.routing_bucket(),
            match self.layer {
                ArchiveTableLayer::Raw => None,
                ArchiveTableLayer::Compacted => Some(record.partition().path()),
            },
            record.sort_key(),
            record.key(),
        )
    }
}

impl SpillCodec for CandleCodec {
    type Record = ArchiveRecord;
    type Key = CandleOrderKey;
    type Reader = SpillRunReader;
    type Writer = SpillRunWriter;

    fn key(&self, record: &Self::Record) -> Self::Key {
        self.order_key(record)
    }

    fn prepare(&self, entries: Vec<(Self::Key, Self::Record)>) -> Vec<Self::Record> {
        let mut kept = Vec::new();
        let mut pending: Option<(Self::Key, Self::Record)> = None;
        for (key, record) in entries {
            match pending.take() {
                Some((previous_key, _)) if previous_key == key => {}
                Some((_, previous)) => kept.push(previous),
                None => {}
            }
            pending = Some((key, record));
        }
        if let Some((_, record)) = pending {
            kept.push(record);
        }
        kept
    }

    fn label(&self) -> &'static str {
        "candle"
    }

    fn extension(&self) -> &'static str {
        "parquet"
    }

    fn dedupe_merges(&self) -> bool {
        true
    }

    fn prefer_latest_run(&self) -> bool {
        true
    }

    fn open(&self, path: &std::path::Path, batch: usize) -> BuildResult<Self::Reader> {
        SpillRunReader::open_with_batch_size(ArchiveKind::Candles, path, batch)
    }

    fn create(&self, path: &std::path::Path, batch: usize) -> BuildResult<Self::Writer> {
        SpillRunWriter::create_with_batch_size(path, ArchiveKind::Candles, batch)
    }
}

impl SpillReader for SpillRunReader {
    type Record = ArchiveRecord;

    fn read(&mut self) -> BuildResult<Option<Self::Record>> {
        SpillRunReader::next_record(self)
    }
}

impl SpillWriter for SpillRunWriter {
    type Record = ArchiveRecord;

    fn write(&mut self, record: Self::Record) -> BuildResult<()> {
        SpillRunWriter::push(self, record)
    }

    fn finish(self) -> BuildResult<()> {
        SpillRunWriter::close(self)
    }
}

pub struct CandleSpill {
    sorter: SpillSorter<CandleCodec>,
}

impl CandleSpill {
    pub fn new(
        directory: impl Into<PathBuf>,
        bucket_millis: i64,
        layer: ArchiveTableLayer,
        limit: usize,
    ) -> BuildResult<Self> {
        Ok(Self {
            sorter: SpillSorter::with_codec(
                directory,
                limit,
                CandleCodec {
                    bucket_millis,
                    layer,
                },
            )?,
        })
    }

    pub fn push(&mut self, candle: ClosedCandle) -> BuildResult<()> {
        self.sorter.push_record(ArchiveRecord::ClosedCandle(candle))
    }

    pub fn spilled_records(&self) -> usize {
        self.sorter.spilled_records()
    }

    pub fn usage(&self) -> SpillUsage {
        self.sorter.usage()
    }

    pub fn finish(&mut self) -> BuildResult<CandleRecords> {
        Ok(CandleRecords {
            records: self.sorter.finish()?,
        })
    }
}

pub struct CandleRecords {
    records: SpillRecords<CandleCodec>,
}

impl CandleRecords {
    pub fn next_candle(&mut self) -> BuildResult<Option<ClosedCandle>> {
        let Some(record) = self.records.next_record()? else {
            return Ok(None);
        };
        match record {
            ArchiveRecord::ClosedCandle(candle) => Ok(Some(candle)),
            ArchiveRecord::MarketEvent(_) => Err(BuildError::io(
                "read rebuilt candles",
                "rebuild spill merge",
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "event record in candle spill merge",
                ),
            )),
        }
    }
}
