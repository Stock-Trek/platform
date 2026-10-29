use platform_aggregator::{
    AggregatorConfig, AggregatorRunner, CheckpointStore, NatsCandleSink, NatsCheckpointStore,
    NatsMarketSource, NatsPartitionSource, NatsRouter,
};
use platform_errors::BpResult;
use platform_nats::NatsPublisher;
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

    let checkpoints = Arc::new(NatsCheckpointStore::connect(&config).await?);
    let sink = Arc::new(NatsCandleSink::connect(&config).await?);

    let mut tasks = JoinSet::new();
    tasks.spawn(async move { router.run().await });

    for partition in partitions {
        let checkpoint = checkpoints.load(partition).await?;
        let start_sequence = checkpoint.map(|checkpoint| checkpoint.resume_sequence());
        let source = NatsPartitionSource::connect(&config, partition, start_sequence).await?;
        let runner = AggregatorRunner::new_partition(
            config.clone(),
            source,
            sink.clone(),
            checkpoints.clone(),
            partition,
        );
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
