use crate::{compactor::BatchCompactor, config::CompactorConfig, source::ArchiveBatchSource};
use platform_errors::BpResult;
use platform_runtime::Runtime;
use tokio::time::{self, MissedTickBehavior};
use tracing::{info, warn};

pub struct CompactorRunner<S, C> {
    config: CompactorConfig,
    source: S,
    compactor: C,
}

impl<S, C> CompactorRunner<S, C>
where
    S: ArchiveBatchSource,
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

        Runtime::run_until_shutdown(Self::run_cycles(&source, &compactor, &mut ticker)).await
    }

    async fn run_cycles(source: &S, compactor: &C, ticker: &mut time::Interval) -> BpResult<()> {
        loop {
            ticker.tick().await;
            if let Err(error) = Self::run_cycle(source, compactor).await {
                warn!(%error, "compaction cycle failed");
            }
        }
    }

    pub(crate) async fn run_cycle(source: &S, compactor: &C) -> BpResult<()> {
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
