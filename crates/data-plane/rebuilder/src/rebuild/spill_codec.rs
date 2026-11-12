use platform_errors::BpResult;
use std::path::Path;

pub(crate) trait SpillReader {
    type Record;

    fn read(&mut self) -> BpResult<Option<Self::Record>>;
}

pub(crate) trait SpillWriter {
    type Record;

    fn write(&mut self, record: Self::Record) -> BpResult<()>;

    fn finish(self) -> BpResult<()>;
}

pub(crate) trait SpillCodec: Clone {
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

    fn open(&self, path: &Path, batch: usize) -> BpResult<Self::Reader>;

    fn create(&self, path: &Path, batch: usize) -> BpResult<Self::Writer>;
}
