use platform_aggregator::{
    AggregatorConfig, ArchiveRecoveryConfig, ArchivedEventSource, EmptyArchivedEventSource,
    LeaseStore, NatsCandleSink, NatsLeaseStore, NatsMarketSource, NatsRouter, NatsShardSource,
    NatsStateStore, ProcessorRunner, RestoreOptions, S3ArchiveRecovery, ShardId, StateStore,
    WriterLease, new_writer_id, restore_shard, wait_for_lease,
};
use platform_errors::{BpError, BpResult};
use platform_nats::{JetStream, NatsPublisher};
use platform_runtime::{Runtime, Service};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tracing::{error, info, warn};

const SERVICE: Service = Service::new("platform-aggregator");
const INITIAL_RESTART_DELAY: Duration = Duration::from_secs(1);
const MAX_RESTART_DELAY: Duration = Duration::from_secs(30);
const HEALTHY_RUN_THRESHOLD: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = AggregatorConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;

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

    for shard in shards {
        tasks.spawn(run_shard(
            config.clone(),
            archive_recovery.clone(),
            checkpoints.clone(),
            leases.clone(),
            sink.clone(),
            shard,
        ));
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

async fn supervise<F, Fut>(name: &'static str, mut run: F) -> BpResult<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = BpResult<()>>,
{
    let mut restart_delay = INITIAL_RESTART_DELAY;
    let shutdown = Runtime::shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        let started_at = Instant::now();
        match run().await {
            Ok(()) => {
                warn!(
                    task = name,
                    "aggregator task exited without a shutdown request; restarting"
                );
            }
            Err(error) if error.retryability().is_retryable() => {
                error!(task = name, %error, "aggregator task stopped; restarting");
            }
            Err(error) => {
                error!(
                    task = name,
                    %error,
                    "aggregator task failed permanently; not restarting"
                );
                return Err(error);
            }
        }

        if started_at.elapsed() >= HEALTHY_RUN_THRESHOLD {
            restart_delay = INITIAL_RESTART_DELAY;
        }
        let delay = jittered_delay(restart_delay);
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            result = &mut shutdown => {
                result?;
                return Ok(());
            }
        }
        restart_delay = next_restart_delay(restart_delay);
    }
}

fn next_restart_delay(current: Duration) -> Duration {
    (current * 2).min(MAX_RESTART_DELAY)
}

fn jittered_delay(base: Duration) -> Duration {
    // Jitter the upper half of the delay so replicas that failed together do
    // not all reconnect in lockstep when a shared dependency recovers.
    let jitter = 0.5 + rand::random::<f64>() * 0.5;
    base.mul_f64(jitter).min(MAX_RESTART_DELAY)
}

async fn run_router(config: AggregatorConfig, publisher: Arc<NatsPublisher>) -> BpResult<()> {
    let source = NatsMarketSource::connect(&config).await?;
    NatsRouter::new(config, source, publisher).run().await
}

async fn run_shard(
    config: AggregatorConfig,
    archive_recovery: Option<Arc<S3ArchiveRecovery>>,
    checkpoints: Arc<dyn StateStore>,
    leases: Arc<dyn LeaseStore>,
    sink: Arc<NatsCandleSink>,
    shard: ShardId,
) -> BpResult<()> {
    let source_stream = JetStream::stream_name(&config.shard_subject_prefix);
    supervise("shard", move || {
        let config = config.clone();
        let archive_recovery = archive_recovery.clone();
        let checkpoints = checkpoints.clone();
        let leases = leases.clone();
        let sink = sink.clone();
        let source_stream = source_stream.clone();
        async move {
            run_shard_once(
                config,
                archive_recovery,
                checkpoints,
                leases,
                sink,
                shard,
                source_stream,
            )
            .await
        }
    })
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_shard_once(
    config: AggregatorConfig,
    archive_recovery: Option<Arc<S3ArchiveRecovery>>,
    checkpoints: Arc<dyn StateStore>,
    leases: Arc<dyn LeaseStore>,
    sink: Arc<NatsCandleSink>,
    shard: ShardId,
    source_stream: String,
) -> BpResult<()> {
    let writer_id = new_writer_id();
    let Some(mut lease) =
        acquire_lease_or_shutdown(leases.as_ref(), shard, &writer_id, config.lease_ttl).await?
    else {
        return Ok(());
    };

    let result = async {
        let restore = async {
            let options = RestoreOptions::from_config(&config, shard, &source_stream);
            let restored = restore_shard(
                checkpoints.as_ref(),
                || async {
                    match &archive_recovery {
                        Some(archive_recovery) => Ok(Box::new(
                            archive_recovery.connect(shard, config.shard_count).await?,
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
            let source = NatsShardSource::connect(&config, shard, restored.start).await?;
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
                            shard.to_string(),
                            format!(
                                "writer {writer_id} lost the shard lease while preparing"
                            ),
                        ));
                    };
                    lease = renewed;
                }
            }
        };

        let runner =
            ProcessorRunner::new_shard(config.clone(), source, sink, checkpoints.clone(), shard)
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
    shard: ShardId,
    owner_id: &str,
    ttl: Duration,
) -> BpResult<Option<WriterLease>> {
    tokio::select! {
        result = wait_for_lease(leases, shard, owner_id, ttl) => Ok(Some(result?)),
        signal = Runtime::shutdown_signal() => {
            signal?;
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_delay_doubles_until_it_reaches_the_cap() {
        assert_eq!(
            next_restart_delay(INITIAL_RESTART_DELAY),
            Duration::from_secs(2)
        );
        assert_eq!(next_restart_delay(MAX_RESTART_DELAY), MAX_RESTART_DELAY);
    }

    #[test]
    fn jitter_keeps_the_delay_within_the_upper_half_of_the_base() {
        let base = Duration::from_secs(8);
        for _ in 0..32 {
            let delay = jittered_delay(base);
            assert!(delay >= base / 2);
            assert!(delay <= base);
        }
    }

    #[tokio::test]
    async fn permanent_task_errors_are_not_restarted() {
        let error = supervise("test", || async {
            Err(BpError::config_missing("aggregator test setting"))
        })
        .await
        .unwrap_err();

        assert!(error.retryability().is_permanent());
    }
}
