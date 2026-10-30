//! The explicit boundary between the compacted and raw archive layers.
//!
//! The raw archive is the append-only source of truth. Compaction copies
//! completed raw objects into de-duplicated, sorted partitions and records the
//! source objects in the compacted Iceberg snapshot. That snapshot is the
//! handoff contract: every raw object named by the snapshot is fully represented
//! by the compacted files in the same snapshot, and every other raw object is
//! still part of the raw tail.
//!
//! A recovery reader can therefore replay the compacted files followed by the
//! raw tail and see every archived record exactly once. It never has to choose
//! between a partially compacted table (which would miss the tail) and the full
//! raw history (which would duplicate the compacted prefix).

use crate::{ArchiveCatalog, ArchiveIcebergTable, ArchiveKind, IcebergDataFile};
use platform_errors::BpResult;

/// The compacted archive and the raw objects that are not compacted yet.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArchiveHandoff {
    /// Live data files from the compacted table.
    pub compacted: Vec<IcebergDataFile>,
    /// Raw data files that are not represented by the compacted snapshot.
    pub raw_tail: Vec<IcebergDataFile>,
}

impl ArchiveHandoff {
    /// Discovers the compacted/raw split for one archive kind.
    ///
    /// Compacted files and their processed source-object set are read from the
    /// same Iceberg snapshot, so a concurrent compaction commit cannot make a
    /// raw object look processed before its compacted output is visible.
    pub async fn discover(catalog: &dyn ArchiveCatalog, kind: ArchiveKind) -> BpResult<Self> {
        let (compacted, processed) = catalog
            .compacted_snapshot(ArchiveIcebergTable::compacted(kind))
            .await?;
        let raw = catalog.data_files(ArchiveIcebergTable::raw(kind)).await?;

        let mut compacted = deduplicated(compacted);
        compacted.sort_by(|left, right| left.file_path.cmp(&right.file_path));

        let mut raw_tail = deduplicated(
            raw.into_iter()
                .filter(|file| !processed.contains(&file.file_path))
                .collect(),
        );
        raw_tail.sort_by(|left, right| left.file_path.cmp(&right.file_path));

        Ok(Self {
            compacted,
            raw_tail,
        })
    }

    /// Every file that must be replayed, compacted first and then the raw tail.
    pub fn files(&self) -> impl Iterator<Item = &IcebergDataFile> {
        self.compacted.iter().chain(self.raw_tail.iter())
    }

    pub fn len(&self) -> usize {
        self.compacted.len().saturating_add(self.raw_tail.len())
    }

    pub fn is_empty(&self) -> bool {
        self.compacted.is_empty() && self.raw_tail.is_empty()
    }
}

fn deduplicated(mut files: Vec<IcebergDataFile>) -> Vec<IcebergDataFile> {
    files.sort_by(|left, right| left.file_path.cmp(&right.file_path));
    files.dedup_by(|left, right| left.file_path == right.file_path);
    files
}

#[cfg(test)]
mod tests {
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
}
