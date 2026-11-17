use crate::{
    ArchiveCatalog, ArchiveIcebergTable, ArchiveKind, ArchiveRegistration, CommitContext,
    CompactionId, IcebergDataFile, IcebergFilePartition, LedgerEntry, RecoveryHandoff,
    RegisterOutcome,
};
use async_trait::async_trait;
use platform_errors::{PlatformError, PlatformResult};
use platform_types::TimestampMilliseconds;
use std::collections::HashSet;

const HOUR: i64 = 1_718_467_200_000;

struct FakeCatalog {
    raw_files: Vec<IcebergDataFile>,
    compacted_files: Vec<IcebergDataFile>,
    processed: HashSet<String>,
    ledger: Vec<LedgerEntry>,
    ledger_sources: HashSet<String>,
}

impl FakeCatalog {
    fn file(name: &str) -> IcebergDataFile {
        IcebergDataFile::partitioned(
            format!("s3://archive/{name}"),
            1024,
            1,
            IcebergFilePartition::new(ArchiveKind::Events, HOUR),
        )
    }

    fn entry(processed_at: i64) -> LedgerEntry {
        LedgerEntry::new(
            CompactionId::for_batch(
                ArchiveKind::Events,
                TimestampMilliseconds(HOUR),
                &"a".repeat(64),
            )
            .unwrap(),
            ArchiveKind::Events,
            TimestampMilliseconds(HOUR),
            "s3://archive/manifests/a.parquet".to_string(),
            1,
            TimestampMilliseconds(processed_at),
            0,
        )
        .unwrap()
    }
}

#[async_trait]
impl ArchiveCatalog for FakeCatalog {
    async fn register_files(
        &self,
        _table: ArchiveIcebergTable,
        _registration: &ArchiveRegistration,
        _context: &CommitContext,
    ) -> PlatformResult<RegisterOutcome> {
        Err(unsupported())
    }

    async fn registered_data_files(
        &self,
        _table: ArchiveIcebergTable,
    ) -> PlatformResult<HashSet<String>> {
        Ok(HashSet::new())
    }

    async fn processed_source_files(
        &self,
        _table: ArchiveIcebergTable,
    ) -> PlatformResult<HashSet<String>> {
        Ok(self.processed.clone())
    }

    async fn data_files(&self, table: ArchiveIcebergTable) -> PlatformResult<Vec<IcebergDataFile>> {
        Ok(match table {
            ArchiveIcebergTable::RawEvents => self.raw_files.clone(),
            ArchiveIcebergTable::CompactedEvents => self.compacted_files.clone(),
            _ => Vec::new(),
        })
    }

    async fn ledger_entries(&self, _kind: ArchiveKind) -> PlatformResult<Vec<LedgerEntry>> {
        Ok(self.ledger.clone())
    }

    async fn ledger_source_files(&self, _kind: ArchiveKind) -> PlatformResult<HashSet<String>> {
        Ok(self.ledger_sources.clone())
    }
}

fn unsupported() -> PlatformError {
    PlatformError::external(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "unsupported",
    ))
}

#[tokio::test]
async fn selects_the_dual_layer_before_raw_expiry() -> PlatformResult<()> {
    let catalog = FakeCatalog {
        raw_files: vec![FakeCatalog::file("raw/events/old.parquet")],
        compacted_files: vec![FakeCatalog::file("compacted/events/a.parquet")],
        processed: HashSet::from(["s3://archive/raw/events/other.parquet".to_string()]),
        ledger: vec![FakeCatalog::entry(1_700_000_000_000)],
        ledger_sources: HashSet::new(),
    };

    let recovery = RecoveryHandoff::discover(&catalog, ArchiveKind::Events).await?;

    assert_eq!(recovery.layer_label, "dual_layer");
    assert!(!recovery.raw_expired);
    assert_eq!(
        recovery.ledger_watermark,
        Some(TimestampMilliseconds(1_700_000_000_000))
    );
    assert_eq!(recovery.handoff.compacted.len(), 1);
    assert_eq!(recovery.handoff.raw_tail.len(), 1);
    Ok(())
}

#[tokio::test]
async fn selects_the_ledger_layer_after_raw_expiry() -> PlatformResult<()> {
    let catalog = FakeCatalog {
        raw_files: Vec::new(),
        compacted_files: vec![FakeCatalog::file("compacted/events/a.parquet")],
        processed: HashSet::new(),
        ledger: vec![FakeCatalog::entry(1_700_000_000_000)],
        ledger_sources: HashSet::from(["s3://archive/raw/events/gone.parquet".to_string()]),
    };

    let recovery = RecoveryHandoff::discover(&catalog, ArchiveKind::Events).await?;

    assert_eq!(recovery.layer_label, "compacted_plus_ledger");
    assert!(recovery.raw_expired);
    assert!(recovery.handoff.raw_tail.is_empty());
    assert_eq!(recovery.handoff.compacted.len(), 1);
    Ok(())
}

#[tokio::test]
async fn quarantines_recovery_when_the_manifest_is_unreadable() {
    struct Unreadable;

    #[async_trait]
    impl ArchiveCatalog for Unreadable {
        async fn register_files(
            &self,
            _table: ArchiveIcebergTable,
            _registration: &ArchiveRegistration,
            _context: &CommitContext,
        ) -> PlatformResult<RegisterOutcome> {
            Err(unsupported())
        }

        async fn registered_data_files(
            &self,
            _table: ArchiveIcebergTable,
        ) -> PlatformResult<HashSet<String>> {
            Ok(HashSet::new())
        }

        async fn processed_source_files(
            &self,
            _table: ArchiveIcebergTable,
        ) -> PlatformResult<HashSet<String>> {
            Ok(HashSet::new())
        }

        async fn data_files(
            &self,
            _table: ArchiveIcebergTable,
        ) -> PlatformResult<Vec<IcebergDataFile>> {
            Ok(Vec::new())
        }

        async fn ledger_source_files(&self, _kind: ArchiveKind) -> PlatformResult<HashSet<String>> {
            Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "manifest is corrupt",
            )))
        }
    }

    let error = match RecoveryHandoff::discover(&Unreadable, ArchiveKind::Events).await {
        Err(error) => error,
        Ok(_) => panic!("recovery must quarantine unreadable manifests"),
    };
    assert!(error.to_string().contains("quarantined"));
}
