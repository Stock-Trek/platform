use crate::{
    compacter::Compactor,
    constants::{LOCK_KEY, common_trading_pairs},
    env_config::EnvConfig,
    feed::Feed,
    granularity::Granularity,
    job::{Job, JobOutcome},
    lock::LockGuard,
    path::MarketDataPath,
    s3::{Bucket, ObjectKey, S3},
    updater::KlinesUpdater,
};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3_transfer_manager::Client as TransferClient;
use platform_errors::{BpError, BpResult, DataError, TimeoutError};
use platform_metrics::market_data::MarketDataMetrics;
use platform_time::deadline::Deadline;
use tracing::{error, info, warn};

pub struct JobExecutor;

impl JobExecutor {
    /// Acquire the global lock and execute one explicitly selected job.
    pub async fn run_job(
        env_config: &EnvConfig,
        job: &Job,
        deadline: Deadline,
    ) -> BpResult<JobOutcome> {
        job.validate()?;
        info!("get s3 and transfer clients");
        let (s3_client, transfer_client) = S3::clients().await;
        let bucket = &env_config.s3_bucket;
        let lock_key = ObjectKey::new(LOCK_KEY);
        let mut lock =
            match LockGuard::acquire_with_deadline(&s3_client, bucket, &lock_key, deadline).await {
                Ok(lock) => lock,
                Err(error) if matches!(job, Job::Update) && error.is_skippable() => {
                    MarketDataMetrics::record_lock_skip();
                    warn!("skipping update because another invocation holds the lock: {error}");
                    return Ok(JobOutcome::default());
                }
                Err(error) => return Err(error),
            };
        let result = if deadline.has_remaining() {
            let remaining = deadline.remaining_or_zero();
            tokio::select! {
                biased;
                _ = lock.wait_until_lost() => {
                    warn!("lock {} was lost; cancelling job", lock_key);
                    Err(BpError::Data(DataError::LockLost {
                        key: lock_key.as_str().to_string(),
                    }))
                }
                result = tokio::time::timeout(
                    remaining,
                    Self::process_job(
                        &s3_client,
                        &transfer_client,
                        env_config,
                        job,
                        deadline,
                    ),
                ) => match result {
                    Ok(result) => result,
                    Err(_) => Err(BpError::Timeout(TimeoutError::DeadlineExceeded)),
                },
            }
        } else {
            Err(BpError::Timeout(TimeoutError::DeadlineExceeded))
        };
        if let Err(release_error) = lock.release().await {
            error!("failed to release lock: {release_error}");
        }
        result
    }

    async fn process_job(
        s3_client: &S3Client,
        transfer_client: &TransferClient,
        env_config: &EnvConfig,
        job: &Job,
        deadline: Deadline,
    ) -> BpResult<JobOutcome> {
        let bucket = &env_config.s3_bucket;
        match job {
            Job::Update => {
                Self::process_scheduled(s3_client, transfer_client, env_config, deadline).await
            }
            Job::Backfill(backfill) => {
                info!("running targeted backfill for {:?}", backfill.pair);
                let updater = KlinesUpdater::try_new(env_config)?;
                let windows_ingested = updater
                    .backfill(s3_client, transfer_client, bucket, backfill, deadline)
                    .await?;
                Ok(JobOutcome {
                    windows_ingested: windows_ingested.windows_ingested,
                    windows_compacted: windows_ingested.windows_compacted,
                    windows_incomplete: windows_ingested.windows_incomplete,
                    windows_deadline: windows_ingested.windows_deadline,
                    compaction_targets: 0,
                    incomplete: windows_ingested.windows_incomplete > 0
                        || windows_ingested.windows_deadline > 0,
                })
            }
            Job::Compact(compact) => {
                let prefix = compact.prefix()?;
                info!("running targeted compaction for {}", prefix);
                Compactor::prefix(s3_client, transfer_client, bucket, &prefix, deadline).await?;
                Ok(JobOutcome {
                    windows_ingested: 0,
                    windows_compacted: 0,
                    windows_incomplete: 0,
                    windows_deadline: 0,
                    compaction_targets: 1,
                    incomplete: false,
                })
            }
        }
    }

    async fn process_scheduled(
        s3_client: &S3Client,
        transfer_client: &TransferClient,
        env_config: &EnvConfig,
        deadline: Deadline,
    ) -> BpResult<JobOutcome> {
        let bucket = &env_config.s3_bucket;
        let updater = KlinesUpdater::try_new(env_config)?;
        let update_result = updater
            .update(
                s3_client,
                transfer_client,
                bucket,
                Some(env_config.backfill_start),
                deadline,
            )
            .await;
        if let Err(error) = &update_result {
            error!("update failed: {error}");
        }
        if !deadline.has_remaining() {
            return update_result.map(|update| JobOutcome {
                windows_ingested: update.windows_ingested,
                windows_compacted: update.windows_compacted,
                windows_incomplete: update.windows_incomplete,
                windows_deadline: update.windows_deadline,
                compaction_targets: 0,
                incomplete: true,
            });
        }
        let compact_result = Self::compact_all(s3_client, transfer_client, bucket, deadline).await;
        if let Err(error) = &compact_result {
            error!("compaction failed: {error}");
        }
        match (update_result, compact_result) {
            (Ok(update), Ok(compaction_targets)) => Ok(JobOutcome {
                windows_ingested: update.windows_ingested,
                windows_compacted: update.windows_compacted,
                windows_incomplete: update.windows_incomplete,
                windows_deadline: update.windows_deadline,
                compaction_targets,
                incomplete: update.windows_incomplete > 0 || update.windows_deadline > 0,
            }),
            (Err(error), Ok(_)) | (Ok(_), Err(error)) => Err(error),
            (Err(update_error), Err(compact_error)) => {
                Err(BpError::multiple([update_error, compact_error]))
            }
        }
    }

    async fn compact_all(
        s3_client: &S3Client,
        transfer_client: &TransferClient,
        bucket: &Bucket,
        deadline: Deadline,
    ) -> BpResult<usize> {
        info!("compact all");
        let mut pairs = common_trading_pairs()
            .into_iter()
            .flat_map(|(cex_id, trading_mode, trading_pairs)| {
                trading_pairs.into_iter().map(move |trading_pair| {
                    MarketDataPath::trading_pair(
                        trading_mode,
                        cex_id,
                        trading_pair.base,
                        trading_pair.quote,
                    )
                })
            })
            .collect::<Vec<_>>();
        pairs.sort_unstable();
        pairs.dedup();
        let mut errors = Vec::new();
        let mut compaction_targets = 0;
        'pairs: for pair in pairs {
            for feed in Feed::ALL {
                for granularity in Granularity::ALL {
                    if !deadline.has_remaining() {
                        let error = BpError::Timeout(TimeoutError::DeadlineExceeded);
                        warn!("stopping compaction before deadline: {error}");
                        errors.push(error);
                        break 'pairs;
                    }
                    let prefix = MarketDataPath::tier_prefix(&pair, feed, granularity);
                    match Compactor::prefix(s3_client, transfer_client, bucket, &prefix, deadline)
                        .await
                    {
                        Ok(()) => compaction_targets += 1,
                        Err(error) => {
                            warn!("failed to compact folder {}: {error}", prefix);
                            errors.push(error);
                        }
                    }
                }
            }
        }
        if errors.is_empty() {
            Ok(compaction_targets)
        } else {
            Err(BpError::multiple(errors))
        }
    }
}
