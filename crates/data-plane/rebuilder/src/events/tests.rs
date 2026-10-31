use super::*;
use crate::config::RebuildSourceLayer;
use async_trait::async_trait;
use platform_archive::{ArchiveCatalog, ArchiveIcebergTable, IcebergDataFile};
use platform_errors::BpResult;
use std::collections::HashSet;

#[derive(Default)]
struct FakeCatalog {
    raw: Vec<IcebergDataFile>,
    compacted: Vec<IcebergDataFile>,
    processed: HashSet<String>,
}

#[async_trait]
impl ArchiveCatalog for FakeCatalog {
    async fn register_files(
        &self,
        _table: ArchiveIcebergTable,
        _files: &[IcebergDataFile],
        _source_files: Option<&[String]>,
    ) -> BpResult<()> {
        Ok(())
    }

    async fn registered_data_files(
        &self,
        _table: ArchiveIcebergTable,
    ) -> BpResult<HashSet<String>> {
        Ok(HashSet::new())
    }

    async fn data_files(&self, table: ArchiveIcebergTable) -> BpResult<Vec<IcebergDataFile>> {
        let files = match table.layer() {
            platform_archive::ArchiveTableLayer::Raw => &self.raw,
            platform_archive::ArchiveTableLayer::Compacted => &self.compacted,
        };
        Ok(files.clone())
    }

    async fn processed_source_files(
        &self,
        table: ArchiveIcebergTable,
    ) -> BpResult<HashSet<String>> {
        if table.layer() == platform_archive::ArchiveTableLayer::Compacted {
            Ok(self.processed.clone())
        } else {
            Ok(HashSet::new())
        }
    }
}

fn file(path: &str) -> IcebergDataFile {
    IcebergDataFile::new(path, 10, 1)
}

#[tokio::test]
async fn auto_reads_compacted_files_and_only_the_uncompacted_raw_tail() {
    let covered = "s3://archive/market-data/raw/events/covered.parquet";
    let tail = "s3://archive/market-data/raw/events/tail.parquet";
    let compacted = "s3://archive/market-data/compacted/events/a.parquet";
    let catalog = FakeCatalog {
        raw: vec![file(covered), file(tail)],
        compacted: vec![file(compacted)],
        processed: HashSet::from([covered.to_string()]),
    };

    let (layer, handoff) = IcebergEventReader::discover(&catalog, RebuildSourceLayer::Auto)
        .await
        .unwrap();

    assert_eq!(layer, RebuildSourceLayer::Auto);
    assert_eq!(handoff.compacted, vec![file(compacted)]);
    assert_eq!(handoff.raw_tail, vec![file(tail)]);
}

#[tokio::test]
async fn explicit_layers_do_not_mix_sources() {
    let raw = file("s3://archive/market-data/raw/events/a.parquet");
    let compacted = file("s3://archive/market-data/compacted/events/a.parquet");
    let catalog = FakeCatalog {
        raw: vec![raw.clone()],
        compacted: vec![compacted.clone()],
        processed: HashSet::new(),
    };

    let (_, raw_handoff) = IcebergEventReader::discover(&catalog, RebuildSourceLayer::Raw)
        .await
        .unwrap();
    let (_, compacted_handoff) =
        IcebergEventReader::discover(&catalog, RebuildSourceLayer::Compacted)
            .await
            .unwrap();

    assert_eq!(raw_handoff.raw_tail, vec![raw]);
    assert!(raw_handoff.compacted.is_empty());
    assert_eq!(compacted_handoff.compacted, vec![compacted]);
    assert!(compacted_handoff.raw_tail.is_empty());
}
