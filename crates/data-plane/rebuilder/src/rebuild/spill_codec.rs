use platform_errors::PlatformResult;
use std::path::Path;

pub trait SpillReader {
    type Record;

    fn read(&mut self) -> PlatformResult<Option<Self::Record>>;
}

pub trait SpillWriter {
    type Record;

    fn write(&mut self, record: Self::Record) -> PlatformResult<()>;

    fn finish(self) -> PlatformResult<()>;
}

pub trait SpillCodec: Clone {
    type Record;
    type Key: Ord;
    type Reader: SpillReader<Record = Self::Record>;
    type Writer: SpillWriter<Record = Self::Record>;

    fn key(&self, record: &Self::Record) -> Self::Key;

    fn prepare(&self, entries: Vec<(Self::Key, Self::Record)>) -> Vec<Self::Record> {
        entries.into_iter().map(|(_, record)| record).collect()
    }

    fn label(&self) -> &'static str;

    fn extension(&self) -> &'static str;

    fn dedupe_merges(&self) -> bool;

    fn prefer_latest_run(&self) -> bool;

    fn open(&self, path: &Path, batch: usize) -> PlatformResult<Self::Reader>;

    fn create(&self, path: &Path, batch: usize) -> PlatformResult<Self::Writer>;
}
