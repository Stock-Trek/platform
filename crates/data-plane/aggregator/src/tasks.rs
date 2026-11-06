use crate::shard_writer::ShardWriter;
use crate::supervisor::Supervisor;
use platform_aggregator::{
    AggregatorConfig, LeaseStore, NatsCandleSink, NatsLeaseStore, NatsMarketSource, NatsRouter,
    NatsStateStore, S3ArchiveRecovery, StateStore,
};
use platform_errors::{BpError, BpResult};
use platform_nats::NatsPublisher;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::{error, info};

pub struct Tasks;

impl Tasks {
    pub async fn run(
        config: AggregatorConfig,
        archive_recovery: Option<Arc<S3ArchiveRecovery>>,
    ) -> BpResult<()> {
        let shards = config.shards();
        info!(
            shard_subject_prefix = %config.shard_subject_prefix,
            shard_count = config.shard_count,
            shards = ?shards,
            "starting aggregator router and shard writers"
        );

        // The router owns the reshard stream. It must exist before shard
        // consumers connect, and it deliberately lives outside the market subject
        // tree so its subjects cannot overlap the ingester stream.
        let publisher =
            Arc::new(NatsPublisher::connect(&config.nats_url, "platform-aggregator-router").await?);
        publisher
            .ensure_stream(&config.shard_subject_prefix)
            .await?;

        let checkpoints: Arc<dyn StateStore> = Arc::new(NatsStateStore::connect(&config).await?);
        let leases: Arc<dyn LeaseStore> = Arc::new(NatsLeaseStore::connect(&config).await?);
        let sink = Arc::new(NatsCandleSink::connect(&config).await?);

        let mut tasks = JoinSet::new();
        let router_config = config.clone();
        let router_publisher = publisher.clone();
        tasks.spawn(async move {
            Supervisor::supervise("router", move || {
                let config = router_config.clone();
                let publisher = router_publisher.clone();
                async move { Tasks::run_router(config, publisher).await }
            })
            .await
        });

        for shard in shards {
            tasks.spawn(
                ShardWriter::new(
                    config.clone(),
                    archive_recovery.clone(),
                    checkpoints.clone(),
                    leases.clone(),
                    sink.clone(),
                    shard,
                )
                .run(),
            );
        }

        let mut first_error = None;
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    error!(%error, "aggregator task stopped");
                    if first_error.is_none() {
                        first_error = Some(error);
                        // A permanently failed role must stop its siblings and the
                        // process. The external supervisor then owns the restart.
                        tasks.shutdown().await;
                    }
                }
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    error!(%error, "aggregator task panicked");
                    if first_error.is_none() {
                        first_error = Some(BpError::external(error));
                        tasks.shutdown().await;
                    }
                }
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn run_router(config: AggregatorConfig, publisher: Arc<NatsPublisher>) -> BpResult<()> {
        let source = NatsMarketSource::connect(&config).await?;
        NatsRouter::new(config, source, publisher).run().await
    }
}
