use crate::{
    compactor::{BatchCompactor, CompactionStats},
    compactor_runner::CompactorRunner,
    source::ArchiveBatchSource,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_archive::{ArchiveKind, SourceBatch};
use platform_errors::PlatformResult;
use platform_types::TimestampMilliseconds;
use std::sync::Arc;

struct FakeSource {
    batches: Vec<SourceBatch>,
}

#[async_trait]
impl ArchiveBatchSource for FakeSource {
    async fn pending_batches(&self) -> PlatformResult<Vec<SourceBatch>> {
        Ok(self.batches.clone())
    }
}

#[derive(Default, Clone)]
struct FakeCompactor {
    batches: Arc<Mutex<usize>>,
}

#[async_trait]
impl BatchCompactor for FakeCompactor {
    async fn compact(&self, _batch: &SourceBatch) -> PlatformResult<CompactionStats> {
        *self.batches.lock() += 1;
        Ok(CompactionStats::default())
    }
}

#[tokio::test]
async fn runs_a_cycle_with_injected_source_and_compactor() {
    let source = FakeSource {
        batches: vec![SourceBatch {
            kind: ArchiveKind::Events,
            prefix: "raw/kind=events".to_string(),
            bucket_start: TimestampMilliseconds(0),
            files: vec!["raw/kind=events/data.parquet".to_string()],
        }],
    };
    let compactor = FakeCompactor::default();
    let observed = compactor.clone();

    CompactorRunner::run_cycle(&source, &compactor)
        .await
        .unwrap();

    assert_eq!(*observed.batches.lock(), 1);
}
