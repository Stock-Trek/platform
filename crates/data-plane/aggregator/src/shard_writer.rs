use crate::supervisor::Supervisor;
use platform_aggregator::{
    AggregatorConfig, ArchivedEventSource, LeaseStore, NatsCandleSink, NatsShardSource,
    ProcessorRunner, RestoreOptions, S3ArchiveRecovery, ShardId, StateStore, WriterLease,
};
use platform_archive::ICEBERG_CATALOG_URI;
use platform_errors::{BuildError, BuildResult};
use platform_nats::JetStream;
use platform_runtime::Runtime;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct ShardWriter {
    config: AggregatorConfig,
    archive_recovery: Option<Arc<S3ArchiveRecovery>>,
    checkpoints: Arc<dyn StateStore>,
    leases: Arc<dyn LeaseStore>,
    sink: Arc<NatsCandleSink>,
    shard: ShardId,
}

impl ShardWriter {
    pub fn new(
        config: AggregatorConfig,
        archive_recovery: Option<Arc<S3ArchiveRecovery>>,
        checkpoints: Arc<dyn StateStore>,
        leases: Arc<dyn LeaseStore>,
        sink: Arc<NatsCandleSink>,
        shard: ShardId,
    ) -> Self {
        Self {
            config,
            archive_recovery,
            checkpoints,
            leases,
            sink,
            shard,
        }
    }

    pub async fn run(self) -> BuildResult<()> {
        let source_stream = JetStream::stream_name(&self.config.shard_subject_prefix);
        Supervisor::supervise("shard", move || {
            let writer = self.clone();
            let source_stream = source_stream.clone();
            async move { writer.run_once(source_stream).await }
        })
        .await
    }

    async fn run_once(&self, source_stream: String) -> BuildResult<()> {
        let writer_id = WriterLease::new_writer_id();
        let Some(mut lease) = self.acquire_lease(&writer_id).await? else {
            return Ok(());
        };

        let result = async {
            let restore = async {
                let options = RestoreOptions::from_config(&self.config, self.shard, &source_stream);
                let restored = options
                    .restore(self.checkpoints.as_ref(), || async {
                        match &self.archive_recovery {
                            Some(archive_recovery) => Ok(Box::new(
                                archive_recovery
                                    .connect(self.shard, self.config.shard_count)
                                    .await?,
                            )
                                as Box<dyn ArchivedEventSource>),
                            None => Err(BuildError::config_missing(ICEBERG_CATALOG_URI)),
                        }
                    })
                    .await?;
                let replay_watermark = restored.replay_watermark();
                let source =
                    NatsShardSource::connect(&self.config, self.shard, restored.start).await?;
                Ok::<_, BuildError>((restored.state, replay_watermark, source))
            };
            tokio::pin!(restore);

            let mut renew_ticker =
                tokio::time::interval((self.config.lease_ttl / 3).max(Duration::from_millis(10)));
            renew_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let (state, replay_watermark, source) = loop {
                tokio::select! {
                    result = &mut restore => break result?,
                    _ = renew_ticker.tick() => {
                        let Some(renewed) =
                            self.leases.renew(&lease, self.config.lease_ttl).await?
                        else {
                            return Err(BuildError::bad_value(
                                "aggregator lease",
                                self.shard.to_string(),
                                format!("writer {writer_id} lost the shard lease while preparing"),
                            ));
                        };
                        lease = renewed;
                    }
                }
            };

            let runner = ProcessorRunner::new_shard(
                self.config.clone(),
                source,
                self.sink.clone(),
                self.checkpoints.clone(),
                self.leases.clone(),
                self.shard,
            )
            .with_lease(lease.clone())
            .with_writer_id(writer_id.clone())
            .with_state(state)
            .with_replay_watermark(replay_watermark);
            runner.run().await
        }
        .await;

        let release_result = self.leases.release(&lease).await;
        result?;
        release_result?;
        Ok(())
    }

    async fn acquire_lease(&self, writer_id: &str) -> BuildResult<Option<WriterLease>> {
        tokio::select! {
            result = WriterLease::wait_for_lease(
                self.leases.as_ref(),
                self.shard,
                writer_id,
                self.config.lease_ttl,
            ) => Ok(Some(result?)),
            signal = Runtime::shutdown_signal() => {
                signal?;
                Ok(None)
            }
        }
    }
}
