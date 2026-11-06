use crate::supervisor::supervise;
use platform_aggregator::{
    AGGREGATOR_ARCHIVE_S3_BUCKET, AggregatorConfig, ArchivedEventSource, LeaseStore,
    NatsCandleSink, NatsShardSource, ProcessorRunner, RestoreOptions, S3ArchiveRecovery, ShardId,
    StateStore, WriterLease,
};
use platform_errors::{BpError, BpResult};
use platform_nats::JetStream;
use platform_runtime::Runtime;
use std::sync::Arc;
use std::time::Duration;

pub async fn run_shard(
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
    let writer_id = WriterLease::new_writer_id();
    let Some(mut lease) =
        acquire_lease_or_shutdown(leases.as_ref(), shard, &writer_id, config.lease_ttl).await?
    else {
        return Ok(());
    };

    let result = async {
        let restore = async {
            let options = RestoreOptions::from_config(&config, shard, &source_stream);
            let restored = options
                .restore(checkpoints.as_ref(), || async {
                    match &archive_recovery {
                        Some(archive_recovery) => Ok(Box::new(
                            archive_recovery.connect(shard, config.shard_count).await?,
                        )
                            as Box<dyn ArchivedEventSource>),
                        None => Err(BpError::config_missing(AGGREGATOR_ARCHIVE_S3_BUCKET)),
                    }
                })
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

        let runner = ProcessorRunner::new_shard(
            config.clone(),
            source,
            sink,
            checkpoints.clone(),
            leases.clone(),
            shard,
        )
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
        result = WriterLease::wait_for_lease(leases, shard, owner_id, ttl) => Ok(Some(result?)),
        signal = Runtime::shutdown_signal() => {
            signal?;
            Ok(None)
        }
    }
}
