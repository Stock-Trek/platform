use super::*;
use async_trait::async_trait;
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
            crate::ArchiveTableLayer::Raw => &self.raw,
            crate::ArchiveTableLayer::Compacted => &self.compacted,
        };
        Ok(files.clone())
    }

    async fn processed_source_files(
        &self,
        table: ArchiveIcebergTable,
    ) -> BpResult<HashSet<String>> {
        if table.layer() == crate::ArchiveTableLayer::Compacted {
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
async fn compacted_files_and_the_uncompact_raw_tail_do_not_overlap() {
    let covered = "s3://archive/market-data/raw/events/covered.parquet";
    let tail = "s3://archive/market-data/raw/events/tail.parquet";
    let catalog = FakeCatalog {
        raw: vec![file(tail), file(covered)],
        compacted: vec![file("s3://archive/market-data/compacted/events/a.parquet")],
        processed: HashSet::from([covered.to_string()]),
    };

    let handoff = ArchiveHandoff::discover(&catalog, ArchiveKind::Events)
        .await
        .unwrap();

    assert_eq!(handoff.compacted.len(), 1);
    assert_eq!(handoff.raw_tail, vec![file(tail)]);
    assert_eq!(handoff.len(), 2);
    assert_eq!(handoff.files().count(), 2);
}

#[tokio::test]
async fn a_missing_compacted_layer_leaves_the_entire_raw_history_in_the_tail() {
    let first = "s3://archive/market-data/raw/events/a.parquet";
    let second = "s3://archive/market-data/raw/events/b.parquet";
    let catalog = FakeCatalog {
        raw: vec![file(second), file(first)],
        ..FakeCatalog::default()
    };

    let handoff = ArchiveHandoff::discover(&catalog, ArchiveKind::Events)
        .await
        .unwrap();

    assert!(handoff.compacted.is_empty());
    assert_eq!(handoff.raw_tail, vec![file(first), file(second)]);
    assert!(!handoff.is_empty());
}

#[tokio::test]
async fn duplicate_file_registrations_are_replayed_once() {
    let path = "s3://archive/market-data/raw/events/a.parquet";
    let catalog = FakeCatalog {
        raw: vec![file(path), file(path)],
        ..FakeCatalog::default()
    };

    let handoff = ArchiveHandoff::discover(&catalog, ArchiveKind::Events)
        .await
        .unwrap();

    assert_eq!(handoff.raw_tail, vec![file(path)]);
}
