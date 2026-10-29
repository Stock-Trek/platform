use platform_aggregator::{
    AggregatorConfig, ArchiveRecoveryConfig, ArchivedEventSource, EmptyArchivedEventSource,
    LeaseStore, NatsCandleSink, NatsLeaseStore, NatsMarketSource, NatsPartitionSource, NatsRouter,
    NatsStateStore, ProcessorRunner, RestoreOptions, S3ArchivedEventSource, StateStore,
    restore_partition,
};
use platform_errors::BpResult;
use platform_nats::{JetStream, NatsPublisher};
use platform_runtime::Service;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::{error, info};

const SERVICE: Service = Service::new("platform-aggregator");

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = AggregatorConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;

    let partitions = config.partitions();
    info!(
        shard_subject_prefix = %config.shard_subject_prefix,
        shard_count = config.shard_count,
        partitions = ?partitions,
        "starting aggregator router and partition writers"
    );

    // The router owns the repartition stream. It must exist before partition
    // consumers connect, and it deliberately lives outside the market subject
    // tree so its subjects cannot overlap the ingester stream.
    let publisher = NatsPublisher::connect(&config.nats_url, "platform-aggregator-router").await?;
    publisher
        .ensure_stream(&config.shard_subject_prefix)
        .await?;
    let router_source = NatsMarketSource::connect(&config).await?;
    let router = NatsRouter::new(config.clone(), router_source, publisher);

    let checkpoints: Arc<dyn StateStore> = Arc::new(NatsStateStore::connect(&config).await?);
    let leases: Arc<dyn LeaseStore> = Arc::new(NatsLeaseStore::connect(&config).await?);
    let sink = Arc::new(NatsCandleSink::connect(&config).await?);
    let archive_config = ArchiveRecoveryConfig::from_env()?;
    let source_stream = JetStream::stream_name(&config.shard_subject_prefix);

    let mut tasks = JoinSet::new();
    tasks.spawn(async move { router.run().await });

    for partition in partitions {
        let mut archive: Box<dyn ArchivedEventSource> = match &archive_config {
            Some(archive_config) => Box::new(
                S3ArchivedEventSource::connect(archive_config, partition, config.shard_count)
                    .await?,
            ),
            None => Box::new(EmptyArchivedEventSource),
        };
        let options = RestoreOptions::from_config(&config, partition, &source_stream);
        let restored = restore_partition(checkpoints.as_ref(), archive.as_mut(), &options).await?;
        let start = restored.start;
        let replay_watermark = restored.replay_watermark();
        let source = NatsPartitionSource::connect(&config, partition, start).await?;
        let runner = ProcessorRunner::new_partition(
            config.clone(),
            source,
            sink.clone(),
            checkpoints.clone(),
            partition,
        )
        .with_lease_store(leases.clone())
        .with_state(restored.state)
        .with_replay_watermark(replay_watermark);
        tasks.spawn(async move { runner.run().await });
    }

    let mut first_error = None;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                error!(%error, "aggregator task stopped");
                first_error = Some(error);
                tasks.abort_all();
                break;
            }
            Err(error) if error.is_cancelled() => {}
            Err(error) => {
                error!(%error, "aggregator task panicked");
                first_error = Some(platform_errors::BpError::external(error));
                tasks.abort_all();
                break;
            }
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
