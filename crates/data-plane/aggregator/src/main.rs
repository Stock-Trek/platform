use platform_aggregator::{
    AggregatorConfig, ArchiveRecoveryConfig, ArchivedEventSource, EmptyArchivedEventSource,
    LeaseStore, NatsCandleSink, NatsLeaseStore, NatsMarketSource, NatsPartitionSource, NatsRouter,
    NatsStateStore, PartitionId, ProcessorRunner, RestoreOptions, S3ArchiveRecovery, StateStore,
    WriterLease, new_writer_id, restore_partition, wait_for_lease,
};
use platform_errors::{BpError, BpResult};
use platform_nats::{JetStream, NatsPublisher};
use platform_runtime::{Runtime, Service};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::{error, info};

const SERVICE: Service = Service::new("platform-aggregator");
const INITIAL_RESTART_DELAY: Duration = Duration::from_secs(1);
const MAX_RESTART_DELAY: Duration = Duration::from_secs(30);

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
    let publisher =
        Arc::new(NatsPublisher::connect(&config.nats_url, "platform-aggregator-router").await?);
    publisher
        .ensure_stream(&config.shard_subject_prefix)
        .await?;

    let checkpoints: Arc<dyn StateStore> = Arc::new(NatsStateStore::connect(&config).await?);
    let leases: Arc<dyn LeaseStore> = Arc::new(NatsLeaseStore::connect(&config).await?);
    let sink = Arc::new(NatsCandleSink::connect(&config).await?);
    let archive_recovery = ArchiveRecoveryConfig::from_env()?
        .map(S3ArchiveRecovery::new)
        .map(Arc::new);

    let mut tasks = JoinSet::new();
    let router_config = config.clone();
    let router_publisher = publisher.clone();
    tasks.spawn(async move {
        supervise("router", move || {
            let config = router_config.clone();
            let publisher = router_publisher.clone();
            async move { run_router(config, publisher).await }
        })
        .await
    });

    for partition in partitions {
        tasks.spawn(run_partition(
            config.clone(),
            archive_recovery.clone(),
            checkpoints.clone(),
            leases.clone(),
            sink.clone(),
            partition,
        ));
    }

    let mut first_error = None;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                error!(%error, "aggregator supervisor stopped");
                first_error.get_or_insert(error);
            }
            Err(error) if error.is_cancelled() => {}
            Err(error) => {
                error!(%error, "aggregator supervisor panicked");
                first_error.get_or_insert(BpError::external(error));
            }
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn supervise<F, Fut>(name: &'static str, mut run: F) -> BpResult<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = BpResult<()>>,
{
    let mut restart_delay = INITIAL_RESTART_DELAY;
    let shutdown = Runtime::shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        match run().await {
            Ok(()) => return Ok(()),
            Err(error) => {
                error!(task = name, %error, "aggregator task stopped; restarting");
                tokio::select! {
                    _ = tokio::time::sleep(restart_delay) => {}
                    result = &mut shutdown => {
                        result?;
                        return Ok(());
                    }
                }
                restart_delay = (restart_delay * 2).min(MAX_RESTART_DELAY);
            }
        }
    }
}

async fn run_router(config: AggregatorConfig, publisher: Arc<NatsPublisher>) -> BpResult<()> {
    let source = NatsMarketSource::connect(&config).await?;
    NatsRouter::new(config, source, publisher).run().await
}

async fn run_partition(
    config: AggregatorConfig,
    archive_recovery: Option<Arc<S3ArchiveRecovery>>,
    checkpoints: Arc<dyn StateStore>,
    leases: Arc<dyn LeaseStore>,
    sink: Arc<NatsCandleSink>,
    partition: PartitionId,
) -> BpResult<()> {
    let source_stream = JetStream::stream_name(&config.shard_subject_prefix);
    supervise("partition", move || {
        let config = config.clone();
        let archive_recovery = archive_recovery.clone();
        let checkpoints = checkpoints.clone();
        let leases = leases.clone();
        let sink = sink.clone();
        let source_stream = source_stream.clone();
        async move {
            run_partition_once(
                config,
                archive_recovery,
                checkpoints,
                leases,
                sink,
                partition,
                source_stream,
            )
            .await
        }
    })
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_partition_once(
    config: AggregatorConfig,
    archive_recovery: Option<Arc<S3ArchiveRecovery>>,
    checkpoints: Arc<dyn StateStore>,
    leases: Arc<dyn LeaseStore>,
    sink: Arc<NatsCandleSink>,
    partition: PartitionId,
    source_stream: String,
) -> BpResult<()> {
    let writer_id = new_writer_id();
    let Some(mut lease) =
        acquire_lease_or_shutdown(leases.as_ref(), partition, &writer_id, config.lease_ttl).await?
    else {
        return Ok(());
    };

    let result = async {
        let restore = async {
            let options = RestoreOptions::from_config(&config, partition, &source_stream);
            let restored = restore_partition(
                checkpoints.as_ref(),
                || async {
                    match &archive_recovery {
                        Some(archive_recovery) => Ok(Box::new(
                            archive_recovery
                                .connect(partition, config.shard_count)
                                .await?,
                        )
                            as Box<dyn ArchivedEventSource>),
                        None => {
                            Ok(Box::new(EmptyArchivedEventSource) as Box<dyn ArchivedEventSource>)
                        }
                    }
                },
                &options,
            )
            .await?;
            let replay_watermark = restored.replay_watermark();
            let source = NatsPartitionSource::connect(&config, partition, restored.start).await?;
            Ok::<_, BpError>((restored.state, replay_watermark, source))
        };
        tokio::pin!(restore);

        let mut renew_ticker =
            tokio::time::interval((config.lease_ttl / 3).max(Duration::from_millis(10)));
        renew_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let (state, replay_watermark, source) = loop {
            tokio::select! {
                result = &mut restore => break result?,
                _ = renew_ticker.tick() => {
                    let Some(renewed) = leases.renew(&lease, config.lease_ttl).await? else {
                        return Err(BpError::bad_value(
                            "aggregator lease",
                            partition.to_string(),
                            format!(
                                "writer {writer_id} lost the partition lease while preparing"
                            ),
                        ));
                    };
                    lease = renewed;
                }
            }
        };

        let runner = ProcessorRunner::new_partition(
            config.clone(),
            source,
            sink,
            checkpoints.clone(),
            partition,
        )
        .with_lease_store(leases.clone())
        .with_lease(lease.clone())
        .with_writer_id(writer_id.clone())
        .with_state(state)
        .with_replay_watermark(replay_watermark);
        runner.run().await
    }
    .await;

    let release_result = leases.release(&lease).await;
    result?;
    release_result?;
    Ok(())
}

async fn acquire_lease_or_shutdown(
    leases: &dyn LeaseStore,
    partition: PartitionId,
    owner_id: &str,
    ttl: Duration,
) -> BpResult<Option<WriterLease>> {
    tokio::select! {
        result = wait_for_lease(leases, partition, owner_id, ttl) => Ok(Some(result?)),
        signal = Runtime::shutdown_signal() => {
            signal?;
            Ok(None)
        }
    }
}
