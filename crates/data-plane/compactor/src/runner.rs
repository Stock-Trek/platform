use crate::{
    compactor::BatchCompactor,
    config::CompactorConfig,
    source::{ArchiveBatchSource, ProcessedManifest},
};
use platform_errors::BpResult;
use tokio::time::{self, MissedTickBehavior};
use tracing::{info, warn};

pub struct CompactorRunner<S, C> {
    config: CompactorConfig,
    source: S,
    compactor: C,
}

impl<S, C> CompactorRunner<S, C>
where
    S: ArchiveBatchSource + ProcessedManifest,
    C: BatchCompactor,
{
    pub fn new(config: CompactorConfig, source: S, compactor: C) -> Self {
        Self {
            config,
            source,
            compactor,
        }
    }

    pub async fn run(self) -> BpResult<()> {
        let Self {
            config,
            source,
            compactor,
        } = self;
        let mut ticker = time::interval(config.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        info!(
            s3_bucket = %config.s3_bucket,
            s3_prefix = %config.s3_prefix,
            output_prefix = %config.output_prefix,
            "compactor started"
        );

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(error) = Self::run_cycle(&source, &compactor).await {
                        warn!(%error, "compaction cycle failed");
                    }
                }
                signal = platform_runtime::Runtime::shutdown_signal() => {
                    signal?;
                    break;
                }
            }
        }
        Ok(())
    }

    async fn run_cycle(source: &S, compactor: &C) -> BpResult<()> {
        let batches = source.pending_batches().await?;
        info!(batches = batches.len(), "scanning archive for compaction");
        for batch in batches {
            let stats = compactor.compact(&batch).await?;
            info!(
                kind = batch.kind.label(),
                prefix = %batch.prefix,
                source_files = stats.source_files,
                records_read = stats.records_read,
                records_written = stats.records_written,
                output_files = stats.output_files,
                "compacted archive batch"
            );
            source.mark_processed(&batch.files).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compactor::CompactionStats;
    use async_trait::async_trait;
    use platform_archive::{ArchiveKind, SourceBatch};
    use platform_types::new_types::TimestampMilliseconds;
    use std::sync::{Arc, Mutex};

    struct FakeSource {
        batches: Vec<SourceBatch>,
        processed: Arc<Mutex<Vec<Vec<String>>>>,
    }

    #[async_trait]
    impl ArchiveBatchSource for FakeSource {
        async fn pending_batches(&self) -> BpResult<Vec<SourceBatch>> {
            Ok(self.batches.clone())
        }
    }

    #[async_trait]
    impl crate::source::ProcessedManifest for FakeSource {
        async fn processed_files(&self) -> BpResult<std::collections::HashSet<String>> {
            Ok(self
                .processed
                .lock()
                .expect("processed files lock")
                .iter()
                .flatten()
                .cloned()
                .collect())
        }

        async fn mark_processed(&self, files: &[String]) -> BpResult<()> {
            self.processed
                .lock()
                .expect("processed files lock")
                .push(files.to_vec());
            Ok(())
        }
    }

    #[derive(Default, Clone)]
    struct FakeCompactor {
        batches: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl BatchCompactor for FakeCompactor {
        async fn compact(&self, _batch: &SourceBatch) -> BpResult<CompactionStats> {
            *self.batches.lock().expect("compacted batches lock") += 1;
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
            processed: Arc::default(),
        };
        let compactor = FakeCompactor::default();
        let observed = compactor.clone();

        CompactorRunner::run_cycle(&source, &compactor)
            .await
            .unwrap();

        assert_eq!(*observed.batches.lock().unwrap(), 1);
        assert_eq!(source.processed.lock().unwrap().len(), 1);
    }
}
