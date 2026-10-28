use crate::{compactor::Compactor, config::CompactorConfig, source::S3ArchiveSource};
use platform_errors::BpResult;
use tokio::time::{self, MissedTickBehavior};
use tracing::{info, warn};

pub struct CompactorRunner {
    config: CompactorConfig,
}

impl CompactorRunner {
    pub fn new(config: CompactorConfig) -> Self {
        Self { config }
    }

    pub async fn run(self) -> BpResult<()> {
        let source = S3ArchiveSource::connect(&self.config).await?;
        let compactor = Compactor::connect(&self.config).await?;
        let mut ticker = time::interval(self.config.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        info!(
            s3_bucket = %self.config.s3_bucket,
            s3_prefix = %self.config.s3_prefix,
            output_prefix = %self.config.output_prefix,
            "compactor started"
        );

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(error) = self.run_cycle(&source, &compactor).await {
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

    async fn run_cycle(&self, source: &S3ArchiveSource, compactor: &Compactor) -> BpResult<()> {
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
        }
        Ok(())
    }
}
