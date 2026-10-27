use crate::{
    constants::{UPDATE_QUANTIZATION, common_trading_pairs},
    env_config::EnvConfig,
    feed::Feed,
    ingest::{Ingest, IngestOutcome, WindowIngest},
    job::BackfillJob,
    path::MarketDataPath,
    plan,
    quantized_time::QuantizedTime,
    s3::{Bucket, S3},
    updaters::{binance_updater::BinanceUpdater, exchange_updater::ExchangeUpdater},
};
use arrow_schema::SchemaRef;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3_transfer_manager::Client as TransferClient;
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult, ExchangeError, SystemError};
use platform_exchange_types::urls::TradingMode;
use platform_metrics::{
    descriptions::{INGEST_SKIP_REASON_COMPACTED, INGEST_SKIP_REASON_DEADLINE},
    market_data::MarketDataMetrics,
};
use platform_time::deadline::Deadline;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use stock_trek_types::cex::{cex_id::CexId, trading_pair::TradingPair};
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::{info, warn};

pub struct KlinesUpdater {
    binance: HashMap<TradingMode, Arc<dyn ExchangeUpdater>>,
}

/// Windows processed by an update or backfill.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UpdateOutcome {
    pub windows_ingested: usize,
    pub windows_compacted: usize,
    pub windows_incomplete: usize,
    pub windows_deadline: usize,
}

impl UpdateOutcome {
    fn record(&mut self, outcome: IngestOutcome) {
        match outcome {
            IngestOutcome::Ingested => self.windows_ingested += 1,
            IngestOutcome::SkippedIncomplete { .. } => self.windows_incomplete += 1,
        }
    }

    fn record_compacted(&mut self, windows: usize) {
        self.windows_compacted += windows;
        MarketDataMetrics::record_ingest_skipped(INGEST_SKIP_REASON_COMPACTED, windows);
    }

    /// Record a deadline cut-off. When the remaining windows are unknown, count
    /// the interrupted unit so the run is still reported as incomplete.
    fn record_deadline(&mut self, remaining_windows: usize) {
        let skipped = remaining_windows.max(1);
        self.windows_deadline += skipped;
        MarketDataMetrics::record_ingest_skipped(INGEST_SKIP_REASON_DEADLINE, skipped);
    }

    fn merge(&mut self, other: Self) {
        self.windows_ingested += other.windows_ingested;
        self.windows_compacted += other.windows_compacted;
        self.windows_incomplete += other.windows_incomplete;
        self.windows_deadline += other.windows_deadline;
    }
}

struct ExchangeUpdate {
    cex_id: CexId,
    trading_mode: TradingMode,
    trading_pairs: Vec<TradingPair>,
    schema: SchemaRef,
    end_time: DateTime<Utc>,
    backfill_start: Option<DateTime<Utc>>,
    updater: Arc<dyn ExchangeUpdater>,
}

struct TradingPairUpdate {
    cex_id: CexId,
    trading_mode: TradingMode,
    trading_pair: TradingPair,
    schema: SchemaRef,
    end_time: DateTime<Utc>,
    backfill_start: Option<DateTime<Utc>>,
}

impl KlinesUpdater {
    pub fn try_new(config: &EnvConfig) -> BpResult<Self> {
        let mut binance: HashMap<TradingMode, Arc<dyn ExchangeUpdater>> = HashMap::new();
        for (trading_mode, url) in [
            (TradingMode::Paper, config.binance_http_paper.clone()),
            (TradingMode::Real, config.binance_http_real.clone()),
        ] {
            binance.insert(trading_mode, Arc::new(BinanceUpdater::try_new(url)?));
        }
        Ok(Self { binance })
    }

    pub async fn update(
        &self,
        s3_client: &S3Client,
        transfer_client: &TransferClient,
        bucket: &Bucket,
        backfill_start: Option<DateTime<Utc>>,
        deadline: Deadline,
    ) -> BpResult<UpdateOutcome> {
        info!("update");
        let now = Utc::now();
        let schema = Ingest::schema();
        let end_time = QuantizedTime::timestamp(now, UPDATE_QUANTIZATION)?;
        let mut join_set = JoinSet::new();
        let mut outcome = UpdateOutcome::default();
        let mut errors = Vec::new();
        for (cex_id, trading_mode, trading_pairs) in common_trading_pairs() {
            if !deadline.has_remaining() {
                warn!("stopping update before deadline");
                outcome.record_deadline(0);
                break;
            }
            let s3_client = s3_client.clone();
            let transfer_client = transfer_client.clone();
            let bucket = bucket.clone();
            let schema = schema.clone();
            let updater = match self.exchange_updater(cex_id, trading_mode) {
                Ok(updater) => updater,
                Err(error) => {
                    warn!(
                        "skipping exchange {:?} in trading mode {:?}: {error}",
                        cex_id, trading_mode
                    );
                    continue;
                }
            };
            join_set.spawn(async move {
                let result = Self::update_exchange(
                    &s3_client,
                    &transfer_client,
                    &bucket,
                    ExchangeUpdate {
                        cex_id,
                        trading_mode,
                        trading_pairs,
                        schema,
                        end_time,
                        backfill_start,
                        updater,
                    },
                    deadline,
                )
                .await;
                if let Err(error) = &result {
                    warn!(
                        "failed to update exchange {:?} in trading mode {:?}: {error}",
                        cex_id, trading_mode
                    );
                }
                result
            });
        }
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(exchange_outcome)) => outcome.merge(exchange_outcome),
                Ok(Err(error)) if error.is_skippable() => {}
                Ok(Err(error)) => errors.push(error),
                Err(error) => errors.push(error.into()),
            }
        }
        if errors.is_empty() {
            Ok(outcome)
        } else {
            Err(BpError::multiple(errors))
        }
    }

    /// Re-ingest a specific pair and time range.
    ///
    /// Missing windows are always filled. With `force`, windows that already
    /// have a leaf file are fetched again and overwritten. Compacted periods are
    /// skipped because rebuilding their roll-ups belongs to compaction; compacted
    /// skips are counted and logged so an ignored correction is never silent.
    pub async fn backfill(
        &self,
        s3_client: &S3Client,
        transfer_client: &TransferClient,
        bucket: &Bucket,
        backfill: &BackfillJob,
        deadline: Deadline,
    ) -> BpResult<UpdateOutcome> {
        let resolved = backfill.resolve()?;
        let updater = self.exchange_updater(resolved.cex_id, resolved.trading_mode)?;
        let pair = MarketDataPath::trading_pair(
            resolved.trading_mode,
            resolved.cex_id,
            resolved.trading_pair.base,
            resolved.trading_pair.quote,
        );
        let candles_base_path = MarketDataPath::feed(&pair, backfill.feed);
        let base_path_prefix = MarketDataPath::feed_prefix(&pair, backfill.feed);
        info!(
            "backfill {:?} between {} and {} (force: {})",
            resolved.trading_pair, backfill.start, backfill.end, backfill.force
        );
        if !deadline.has_remaining() {
            warn!("stopping backfill before deadline");
            let mut outcome = UpdateOutcome::default();
            outcome.record_deadline(0);
            return Ok(outcome);
        }
        let existing = S3::list_files(s3_client, bucket, &base_path_prefix)
            .await?
            .into_iter()
            .filter_map(|key| {
                key.as_str()
                    .strip_prefix(base_path_prefix.as_str())
                    .map(str::to_string)
            })
            .collect::<HashSet<_>>();
        let plan = plan::Planner::plan_windows_with_overwrite(
            &existing,
            backfill.start,
            backfill.end,
            backfill.force,
        )?;
        let window_count = plan.windows.len() + plan.windows_compacted;
        let compacted_skips = plan.windows_compacted;
        let schema = Ingest::schema();
        let mut outcome = UpdateOutcome::default();
        outcome.record_compacted(plan.windows_compacted);
        for skip in &plan.skipped_compacted {
            if backfill.force {
                warn!(
                    "forced backfill {:?} skipped {} window(s) between {} and {} because {} is already compacted; compacted roll-ups cannot be overwritten by a backfill",
                    resolved.trading_pair, skip.windows, skip.start, skip.end, skip.compacted_file
                );
            } else {
                info!(
                    "backfill {:?} skipped {} already compacted window(s) between {} and {} ({})",
                    resolved.trading_pair, skip.windows, skip.start, skip.end, skip.compacted_file
                );
            }
        }
        for (index, window) in plan.windows.iter().enumerate() {
            if !deadline.has_remaining() {
                warn!("stopping backfill before deadline");
                outcome.record_deadline(plan.windows.len() - index);
                break;
            }
            let ingest_outcome = Ingest::ingest_window(
                transfer_client,
                bucket,
                updater.as_ref(),
                WindowIngest {
                    trading_pair: &resolved.trading_pair,
                    window: *window,
                    candles_base_path: &candles_base_path,
                    schema: &schema,
                },
            )
            .await?;
            outcome.record(ingest_outcome);
        }
        info!(
            "backfill {:?} completed {} window(s): {} ingested, {} incomplete, {} deadline-truncated; {} compacted window(s) skipped",
            resolved.trading_pair,
            window_count,
            outcome.windows_ingested,
            outcome.windows_incomplete,
            outcome.windows_deadline,
            compacted_skips
        );
        Ok(outcome)
    }

    fn exchange_updater(
        &self,
        cex_id: CexId,
        trading_mode: TradingMode,
    ) -> BpResult<Arc<dyn ExchangeUpdater>> {
        match cex_id {
            CexId::Binance => self
                .binance
                .get(&trading_mode)
                .cloned()
                .ok_or(BpError::Exchange(ExchangeError::UnsupportedTradingMode {
                    trading_mode: format!("{trading_mode:?}"),
                })),
            other => Err(BpError::Exchange(ExchangeError::UnsupportedExchange {
                exchange: format!("{other:?}"),
            })),
        }
    }

    async fn update_exchange(
        s3_client: &S3Client,
        transfer_client: &TransferClient,
        bucket: &Bucket,
        update: ExchangeUpdate,
        deadline: Deadline,
    ) -> BpResult<UpdateOutcome> {
        let ExchangeUpdate {
            cex_id,
            trading_mode,
            trading_pairs,
            schema,
            end_time,
            backfill_start,
            updater,
        } = update;
        info!("update exchange {:?} @ end time {}", cex_id, end_time);
        if let Some(backfill_start) = backfill_start {
            info!("backfill start set to {}", backfill_start);
        }
        let semaphore = Arc::new(Semaphore::new(10));
        let mut join_set = JoinSet::new();
        let mut outcome = UpdateOutcome::default();
        let mut errors = Vec::new();
        for trading_pair in trading_pairs {
            if !deadline.has_remaining() {
                warn!("stopping exchange update before deadline");
                outcome.record_deadline(0);
                break;
            }
            info!("update symbol {:?}", trading_pair);
            let bucket = bucket.clone();
            let schema = schema.clone();
            let semaphore = semaphore.clone();
            let s3_client = s3_client.clone();
            let transfer_client = transfer_client.clone();
            let updater_clone = updater.clone();
            let update = TradingPairUpdate {
                cex_id,
                trading_mode,
                trading_pair,
                schema,
                end_time,
                backfill_start,
            };
            join_set.spawn(async move {
                let result = Self::update_pair(
                    &s3_client,
                    &transfer_client,
                    &bucket,
                    &semaphore,
                    updater_clone.as_ref(),
                    &update,
                    deadline,
                )
                .await;
                if let Err(error) = &result {
                    if error.is_skippable() {
                        warn!(
                            "skipping unsupported trading pair {:?}: {error}",
                            update.trading_pair
                        );
                    } else {
                        warn!(
                            "failed to update trading pair {:?}: {error}",
                            update.trading_pair
                        );
                    }
                }
                result
            });
        }
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(pair_outcome)) => outcome.merge(pair_outcome),
                Ok(Err(error)) if error.is_skippable() => {}
                Ok(Err(error)) => errors.push(error),
                Err(error) => errors.push(error.into()),
            }
        }
        if errors.is_empty() {
            Ok(outcome)
        } else {
            Err(BpError::multiple(errors))
        }
    }

    async fn update_pair(
        s3_client: &S3Client,
        transfer_client: &TransferClient,
        bucket: &Bucket,
        semaphore: &Semaphore,
        updater: &dyn ExchangeUpdater,
        update: &TradingPairUpdate,
        deadline: Deadline,
    ) -> BpResult<UpdateOutcome> {
        let TradingPairUpdate {
            cex_id,
            trading_mode,
            trading_pair,
            schema,
            end_time,
            backfill_start,
        } = update;
        if !deadline.has_remaining() {
            warn!("stopping trading pair update before deadline");
            let mut outcome = UpdateOutcome::default();
            outcome.record_deadline(0);
            return Ok(outcome);
        }
        let remaining = deadline.remaining_or_zero();
        let _permit = match tokio::time::timeout(remaining, semaphore.acquire()).await {
            Ok(result) => result.map_err(|_| BpError::System(SystemError::SemaphoreClosed))?,
            Err(_) => {
                warn!("stopping trading pair update before deadline: deadline exceeded");
                let mut outcome = UpdateOutcome::default();
                outcome.record_deadline(0);
                return Ok(outcome);
            }
        };
        if !deadline.has_remaining() {
            warn!("stopping trading pair update before deadline");
            let mut outcome = UpdateOutcome::default();
            outcome.record_deadline(0);
            return Ok(outcome);
        }
        let pair = MarketDataPath::trading_pair(
            trading_mode,
            cex_id,
            trading_pair.base,
            trading_pair.quote,
        );
        let candles_base_path = MarketDataPath::feed(&pair, Feed::Trades);
        let base_path_prefix = MarketDataPath::feed_prefix(&pair, Feed::Trades);
        let existing = S3::list_files(s3_client, bucket, &base_path_prefix)
            .await?
            .into_iter()
            .filter_map(|key| {
                key.as_str()
                    .strip_prefix(base_path_prefix.as_str())
                    .map(str::to_string)
            })
            .collect::<HashSet<_>>();
        let plan = plan::Planner::plan_windows_with_skipped(&existing, *end_time, *backfill_start)?;
        let mut outcome = UpdateOutcome::default();
        outcome.record_compacted(plan.windows_compacted);
        for (index, window) in plan.windows.iter().enumerate() {
            if !deadline.has_remaining() {
                warn!("stopping update of {:?} before deadline", trading_pair);
                outcome.record_deadline(plan.windows.len() - index);
                break;
            }
            let ingest_outcome = Ingest::ingest_window(
                transfer_client,
                bucket,
                updater,
                WindowIngest {
                    trading_pair,
                    window: *window,
                    candles_base_path: &candles_base_path,
                    schema,
                },
            )
            .await?;
            outcome.record(ingest_outcome);
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_skip_reasons_are_counted_separately() {
        let mut outcome = UpdateOutcome::default();
        outcome.record(IngestOutcome::SkippedIncomplete {
            expected: 6,
            received: 4,
        });
        outcome.record_compacted(3);
        outcome.record_deadline(2);

        assert_eq!(outcome.windows_ingested, 0);
        assert_eq!(outcome.windows_compacted, 3);
        assert_eq!(outcome.windows_incomplete, 1);
        assert_eq!(outcome.windows_deadline, 2);
    }

    #[test]
    fn an_unknown_deadline_cut_off_is_not_reported_as_complete() {
        let mut outcome = UpdateOutcome::default();
        outcome.record_deadline(0);

        assert_eq!(outcome.windows_deadline, 1);
    }
}
