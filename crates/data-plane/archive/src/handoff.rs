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

        let mut compacted = Self::deduplicated(compacted);
        compacted.sort_by(|left, right| left.file_path.cmp(&right.file_path));

        let mut raw_tail = Self::deduplicated(
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

impl ArchiveHandoff {
    fn deduplicated(mut files: Vec<IcebergDataFile>) -> Vec<IcebergDataFile> {
        files.sort_by(|left, right| left.file_path.cmp(&right.file_path));
        files.dedup_by(|left, right| left.file_path == right.file_path);
        files
    }
}

#[cfg(test)]
mod tests;
