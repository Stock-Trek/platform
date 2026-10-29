//! Checkpoint restore and archive rebuild for aggregator partitions.
//!
//! Recovery prefers the latest compatible checkpoint because it contains the
//! exact keyed state and the next source sequence. If the checkpoint is
//! missing, stale, or from an incompatible schema, the partition state is
//! rebuilt from the raw event archive instead.

use crate::checkpoint::StateStore;
use crate::config::{AggregatorConfig, ReplayStartPolicy};
use crate::partition::{PartitionId, partition_for};
use crate::processor::{CandleProcessor, ProcessorConfig};
use crate::state::AggregatorState;
use async_trait::async_trait;
use platform_archive::{ArchiveKind, ArchiveParquet, ArchivePath, Bucket, ObjectKey, S3};
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult, ErrorType};
use platform_market_data::{
    candle::CandleInterval,
    decoders::trade::{SharedTradeDecoder, TradeDecoderFactory},
    event::MarketEvent,
};
use platform_metrics::AggregatorMetrics;
use platform_nats::ConsumerStart;
use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use stock_trek_types::cex::cex_id::CexId;
use tracing::{info, warn};

pub const AGGREGATOR_ARCHIVE_S3_BUCKET: &str = "AGGREGATOR_ARCHIVE_S3_BUCKET";
pub const AGGREGATOR_ARCHIVE_S3_PREFIX: &str = "AGGREGATOR_ARCHIVE_S3_PREFIX";

const DEFAULT_ARCHIVE_PREFIX: &str = "market-data/raw";

/// Optional raw-event archive used to rebuild partition state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveRecoveryConfig {
    pub bucket: String,
    pub prefix: String,
}

impl ArchiveRecoveryConfig {
    /// Reads archive recovery settings from the environment.
    ///
    /// Recovery is disabled when `AGGREGATOR_ARCHIVE_S3_BUCKET` is unset.
    pub fn from_env() -> BpResult<Option<Self>> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Option<Self>> {
        let Some(bucket) = vars.optional(AGGREGATOR_ARCHIVE_S3_BUCKET)? else {
            return Ok(None);
        };
        if bucket.trim().is_empty() {
            return Err(BpError::config_invalid(
                AGGREGATOR_ARCHIVE_S3_BUCKET,
                "must not be empty",
            ));
        }
        let prefix = vars
            .optional(AGGREGATOR_ARCHIVE_S3_PREFIX)?
            .unwrap_or_else(|| DEFAULT_ARCHIVE_PREFIX.to_string());
        Ok(Some(Self { bucket, prefix }))
    }
}

/// A stream of raw market events from the archive.
#[async_trait]
pub trait ArchivedEventSource: Send {
    async fn next_event(&mut self) -> BpResult<Option<MarketEvent>>;
}

/// Archive source used when recovery from archived events is not configured.
pub struct EmptyArchivedEventSource;

#[async_trait]
impl ArchivedEventSource for EmptyArchivedEventSource {
    async fn next_event(&mut self) -> BpResult<Option<MarketEvent>> {
        Ok(None)
    }
}

/// The state and source position a partition should start from.
#[derive(Debug, Clone)]
pub struct RestoredPartition {
    pub state: AggregatorState,
    /// Where the live source should begin after recovery.
    pub start: ConsumerStart,
    /// Which recovery path produced the state.
    pub restored_from: RestoreSource,
    /// Whether the state was rebuilt by replaying archived raw events.
    pub rebuilt_from_archive: bool,
}

/// Alias used by startup code.
pub type RestoreResult = RestoredPartition;

/// Recovery path that produced a partition's starting state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreSource {
    Checkpoint,
    Archive,
    Empty,
}

impl RestoreSource {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Checkpoint => "checkpoint",
            Self::Archive => "archive",
            Self::Empty => "empty",
        }
    }
}

impl RestoredPartition {
    pub fn replay_watermark(&self) -> Option<TimestampMilliseconds> {
        if self.rebuilt_from_archive {
            self.state.watermark
        } else {
            None
        }
    }

    pub fn start_sequence(&self) -> Option<u64> {
        match self.start {
            ConsumerStart::Sequence(sequence) => Some(sequence),
            ConsumerStart::Earliest | ConsumerStart::Latest => None,
        }
    }
}

/// Inputs that control checkpoint restore and archive rebuild for one writer.
#[derive(Debug, Clone)]
pub struct RestoreOptions {
    pub partition: PartitionId,
    pub source_stream: String,
    pub intervals: Vec<CandleInterval>,
    pub partition_count: usize,
    pub allowed_lateness: DurationMilliseconds,
    pub replay_start_policy: ReplayStartPolicy,
    pub expected_schema_version: u32,
}

impl RestoreOptions {
    pub fn from_config(
        config: &AggregatorConfig,
        partition: PartitionId,
        source_stream: impl Into<String>,
    ) -> Self {
        Self {
            partition,
            source_stream: source_stream.into(),
            intervals: config.intervals.clone(),
            partition_count: config.shard_count,
            allowed_lateness: config.processor_config().allowed_lateness,
            replay_start_policy: config.replay_start_policy,
            expected_schema_version: config.state_schema_version,
        }
    }
}

/// Restores a partition from its latest checkpoint, rebuilds it from archived
/// raw events, or starts with empty state according to the configured policy.
///
/// A checkpoint is accepted only when it belongs to this partition, names the
/// same source stream, and carries the expected state schema. Otherwise the
/// archive is replayed from the beginning and the caller starts the live source
/// from the beginning as well. Events at or before the rebuilt watermark are
/// then ignored by [`crate::runner::ProcessorRunner`] to avoid applying state
/// twice.
pub async fn restore_partition(
    store: &dyn StateStore,
    archive: &mut dyn ArchivedEventSource,
    options: &RestoreOptions,
) -> BpResult<RestoredPartition> {
    let started = Instant::now();
    let result = restore_partition_inner(store, archive, options).await;
    let outcome = match &result {
        Ok(restored) => restored.restored_from.label(),
        Err(_) => "error",
    };
    AggregatorMetrics::record_state_restore(options.partition.get(), outcome, started.elapsed());
    result
}

async fn restore_partition_inner(
    store: &dyn StateStore,
    archive: &mut dyn ArchivedEventSource,
    options: &RestoreOptions,
) -> BpResult<RestoredPartition> {
    let partition = options.partition;
    match options.replay_start_policy {
        ReplayStartPolicy::Earliest | ReplayStartPolicy::Latest => {
            let start = match options.replay_start_policy {
                ReplayStartPolicy::Latest => ConsumerStart::Latest,
                _ => ConsumerStart::Earliest,
            };
            info!(
                partition = %partition,
                policy = options.replay_start_policy.label(),
                "starting aggregator partition with empty state"
            );
            return Ok(RestoredPartition {
                state: AggregatorState::new(),
                start,
                restored_from: RestoreSource::Empty,
                rebuilt_from_archive: false,
            });
        }
        ReplayStartPolicy::Archive => {
            info!(
                partition = %partition,
                "archive replay policy selected; ignoring checkpoint"
            );
        }
        ReplayStartPolicy::Checkpoint => match store.load(partition).await {
            Ok(Some(checkpoint))
                if checkpoint.is_compatible_with_schema(
                    partition,
                    &options.source_stream,
                    options.expected_schema_version,
                ) =>
            {
                let age = checkpoint.age(TimestampMilliseconds::now());
                AggregatorMetrics::record_checkpoint_age(
                    partition.get(),
                    Duration::from_millis(age.0.max(0) as u64),
                );
                info!(
                    partition = %partition,
                    next_sequence = checkpoint.next_sequence,
                    watermark = ?checkpoint.watermark,
                    open_candles = checkpoint.state.open_candles_len(),
                    "restored aggregator partition from checkpoint"
                );
                return Ok(RestoredPartition {
                    state: checkpoint.state,
                    start: ConsumerStart::Sequence(checkpoint.next_sequence),
                    restored_from: RestoreSource::Checkpoint,
                    rebuilt_from_archive: false,
                });
            }
            Ok(Some(checkpoint)) => {
                warn!(
                    partition = %partition,
                    source_stream = %checkpoint.source_stream,
                    schema_version = checkpoint.state.schema_version,
                    expected_schema_version = options.expected_schema_version,
                    "ignoring stale or incompatible checkpoint; rebuilding from archive"
                );
            }
            Ok(None) => {
                info!(
                    partition = %partition,
                    "no checkpoint found; rebuilding from archive"
                );
            }
            Err(error) => {
                AggregatorMetrics::record_checkpoint_failure(partition.get(), error.error_type());
                warn!(
                    partition = %partition,
                    error = %error,
                    "failed to load checkpoint; rebuilding from archive"
                );
            }
        },
    }

    AggregatorMetrics::record_replay_progress(partition.get(), "archive", 0.0);
    let state = rebuild_state_from_archive(
        archive,
        partition,
        &options.intervals,
        options.partition_count,
        options.allowed_lateness,
    )
    .await?;
    AggregatorMetrics::record_replay_progress(partition.get(), "archive", 1.0);
    info!(
        partition = %partition,
        watermark = ?state.watermark,
        open_candles = state.open_candles_len(),
        "rebuilt aggregator partition state from archived raw events"
    );
    Ok(RestoredPartition {
        state,
        start: ConsumerStart::Earliest,
        restored_from: RestoreSource::Archive,
        rebuilt_from_archive: true,
    })
}

/// Replays all archived events for one partition into a fresh keyed state.
pub async fn rebuild_state_from_archive(
    archive: &mut dyn ArchivedEventSource,
    partition: PartitionId,
    intervals: &[CandleInterval],
    partition_count: usize,
    allowed_lateness: DurationMilliseconds,
) -> BpResult<AggregatorState> {
    let config = ProcessorConfig::new(intervals.to_vec(), allowed_lateness);
    let mut processor = CandleProcessor::with_config(config);
    let mut decoders = HashMap::new();
    let mut replayed = 0_u64;

    // TODO(#157): archived raw events do not yet carry the state epoch that
    // produced their closed-candle revisions. Once they do, continue that epoch
    // here instead of starting a new lineage at the default epoch.
    while let Some(event) = archive.next_event().await? {
        replayed = replayed.saturating_add(1);
        if replayed.is_multiple_of(1_000) {
            AggregatorMetrics::record_replay_events(partition.get(), "archive", 1_000);
        }
        let decoder = decoder_for(&mut decoders, event.exchange)?;
        let trades = match decoder.decode(&event) {
            Ok(trades) => trades
                .into_iter()
                .filter(|trade| {
                    partition_for(trade.exchange, trade.trading_pair, partition_count) == partition
                })
                .collect::<Vec<_>>(),
            Err(error) => {
                warn!(%error, stream = %event.stream, "skipping unreadable archived event");
                Vec::new()
            }
        };
        // Closed candles produced during rebuild are already represented by
        // the archive's candle records; the fold reconstructs the same keyed
        // state, deduplication ranges, gaps, and event-time watermarks. Events
        // with no trades for this partition must not move its watermark.
        if !trades.is_empty() {
            let _ = processor.apply_event(&event, &trades);
        }
    }
    AggregatorMetrics::record_replay_events(partition.get(), "archive", replayed % 1_000);

    Ok(processor.into_state())
}

fn event_timestamp(event: &MarketEvent) -> TimestampMilliseconds {
    event.exchange_timestamp.unwrap_or(event.received_at)
}

fn decoder_for(
    decoders: &mut HashMap<CexId, SharedTradeDecoder>,
    exchange: CexId,
) -> BpResult<SharedTradeDecoder> {
    if let Some(decoder) = decoders.get(&exchange) {
        return Ok(decoder.clone());
    }
    let decoder = TradeDecoderFactory::for_exchange(exchange)?;
    decoders.insert(exchange, decoder.clone());
    Ok(decoder)
}

/// Replays raw event objects from S3 for one partition.
///
/// This first cut downloads one Parquet object at a time and decodes it in
/// memory. TODO: stream column chunks and skip events owned by other partitions
/// without fully decoding them.
pub struct S3ArchivedEventSource {
    client: aws_sdk_s3::Client,
    bucket: Bucket,
    partition: PartitionId,
    partition_count: usize,
    objects: VecDeque<ObjectKey>,
    events: VecDeque<MarketEvent>,
    decoders: HashMap<CexId, SharedTradeDecoder>,
}

impl S3ArchivedEventSource {
    pub async fn connect(
        config: &ArchiveRecoveryConfig,
        partition: PartitionId,
        partition_count: usize,
    ) -> BpResult<Self> {
        let client = S3::s3_client().await;
        let bucket = Bucket::new(config.bucket.clone());
        let prefix = config.prefix.trim_matches('/').to_string();
        let list_prefix = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };
        let mut objects = S3::list_objects(&client, &bucket, &ObjectKey::new(list_prefix))
            .await?
            .into_iter()
            .filter(|object| {
                ArchivePath::relative_key(object.key.as_str(), &prefix).and_then(ArchivePath::kind)
                    == Some(ArchiveKind::Events)
            })
            .map(|object| object.key)
            .collect::<Vec<_>>();
        objects.sort();

        info!(
            bucket = %bucket,
            prefix = %prefix,
            objects = objects.len(),
            partition = %partition,
            "connected archive recovery source"
        );

        Ok(Self {
            client,
            bucket,
            partition,
            partition_count,
            objects: objects.into(),
            events: VecDeque::new(),
            decoders: HashMap::new(),
        })
    }

    async fn load_next_object(&mut self) -> BpResult<bool> {
        let Some(key) = self.objects.pop_front() else {
            return Ok(false);
        };
        let bytes = S3::download_bytes(&self.client, &self.bucket, &key).await?;
        let batch = ArchiveParquet::decode(ArchiveKind::Events, bytes)?;
        let mut events = batch
            .into_records()
            .into_iter()
            .filter_map(|record| match record {
                platform_archive::ArchiveRecord::MarketEvent(event) => Some(event),
                platform_archive::ArchiveRecord::ClosedCandle(_) => None,
            })
            .collect::<Vec<_>>();
        events.sort_by_key(|event| (event_timestamp(event).0, event.sequence.unwrap_or_default()));
        self.events.extend(events);
        info!(
            key = %key,
            queued_events = self.events.len(),
            "loaded archive object for recovery"
        );
        Ok(true)
    }
}

#[async_trait]
impl ArchivedEventSource for S3ArchivedEventSource {
    async fn next_event(&mut self) -> BpResult<Option<MarketEvent>> {
        loop {
            if let Some(event) = self.events.pop_front() {
                let decoder = decoder_for(&mut self.decoders, event.exchange)?;
                let trades = decoder.decode(&event)?;
                if trades.iter().any(|trade| {
                    partition_for(trade.exchange, trade.trading_pair, self.partition_count)
                        == self.partition
                }) {
                    return Ok(Some(event));
                }
                continue;
            }
            if !self.load_next_object().await? {
                return Ok(None);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{CheckpointRecord, InMemoryStateStore};
    use crate::state::STATE_SCHEMA_VERSION;
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use platform_market_data::decoders::trade::Trade;
    use rust_decimal::Decimal;
    use stock_trek_types::cex::{asset_id::AssetId, trading_pair::TradingPair};

    fn pair() -> TradingPair {
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD)
    }

    fn event(sequence: u64, timestamp: i64) -> MarketEvent {
        MarketEvent {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            sequence: Some(sequence),
            exchange_timestamp: Some(TimestampMilliseconds(timestamp)),
            received_at: TimestampMilliseconds(timestamp),
            payload: STANDARD.encode(format!(
                r#"{{"e":"trade","E":{timestamp},"s":"BTCUSDT","t":{sequence},"p":"100","q":"1","T":{timestamp}}}"#
            )),
        }
    }

    struct VecArchive {
        events: VecDeque<MarketEvent>,
    }

    impl VecArchive {
        fn new(events: Vec<MarketEvent>) -> Self {
            Self {
                events: events.into(),
            }
        }
    }

    #[async_trait]
    impl ArchivedEventSource for VecArchive {
        async fn next_event(&mut self) -> BpResult<Option<MarketEvent>> {
            Ok(self.events.pop_front())
        }
    }

    fn interval() -> CandleInterval {
        CandleInterval::from_label("1m").unwrap()
    }

    fn options(partition: PartitionId, partition_count: usize) -> RestoreOptions {
        RestoreOptions {
            partition,
            source_stream: "aggregator-shards".to_string(),
            intervals: vec![interval()],
            partition_count,
            allowed_lateness: DurationMilliseconds::ZERO,
            replay_start_policy: ReplayStartPolicy::Checkpoint,
            expected_schema_version: STATE_SCHEMA_VERSION,
        }
    }

    #[tokio::test]
    async fn a_compatible_checkpoint_is_restored_without_replaying_the_archive() {
        let store = InMemoryStateStore::new();
        let partition = PartitionId::new(0);
        let mut state = AggregatorState::new();
        state.mark_processed(10);
        state.advance_watermark(TimestampMilliseconds(61_000));
        let checkpoint = CheckpointRecord::new(partition, "aggregator-shards", 11, state);
        store.save(&checkpoint).await.unwrap();

        let mut archive = VecArchive::new(vec![event(1, 1_000)]);
        let restored = restore_partition(&store, &mut archive, &options(partition, 1))
            .await
            .unwrap();

        assert_eq!(restored.start_sequence(), Some(11));
        assert_eq!(restored.start, ConsumerStart::Sequence(11));
        assert!(!restored.rebuilt_from_archive);
        assert_eq!(
            restored.state.watermark,
            Some(TimestampMilliseconds(61_000))
        );
        assert_eq!(archive.events.len(), 1);
    }

    #[tokio::test]
    async fn a_missing_checkpoint_rebuilds_from_archived_events() {
        let store = InMemoryStateStore::new();
        let partition = PartitionId::new(0);
        let mut archive = VecArchive::new(vec![event(1, 61_000), event(2, 62_000)]);

        let restored = restore_partition(&store, &mut archive, &options(partition, 1))
            .await
            .unwrap();

        assert_eq!(restored.start_sequence(), None);
        assert_eq!(restored.start, ConsumerStart::Earliest);
        assert!(restored.rebuilt_from_archive);
        assert_eq!(
            restored.replay_watermark(),
            Some(TimestampMilliseconds(62_000))
        );
        assert_eq!(restored.state.open_candles_len(), 1);
        let candle = restored
            .state
            .open_candles
            .values()
            .next()
            .unwrap()
            .last()
            .unwrap();
        assert_eq!(candle.market_candle.ohlcv.open, 100.0);
        assert_eq!(candle.market_candle.ohlcv.volume, 2.0);
        assert_eq!(candle.market_candle.trade_count, 2);
    }

    #[tokio::test]
    async fn an_incompatible_checkpoint_rebuilds_from_archived_events() {
        let store = InMemoryStateStore::new();
        let partition = PartitionId::new(0);
        let mut state = AggregatorState::new();
        state.schema_version = STATE_SCHEMA_VERSION + 1;
        let checkpoint = CheckpointRecord::new(partition, "aggregator-shards", 11, state);
        store.save(&checkpoint).await.unwrap();

        let mut archive = VecArchive::new(vec![event(1, 61_000)]);
        let restored = restore_partition(&store, &mut archive, &options(partition, 1))
            .await
            .unwrap();

        assert!(restored.rebuilt_from_archive);
        assert_eq!(restored.start_sequence(), None);
        assert_eq!(restored.state.open_candles_len(), 1);
        assert_eq!(restored.state.schema_version, STATE_SCHEMA_VERSION);
    }

    #[tokio::test]
    async fn rebuild_only_applies_events_owned_by_the_partition() {
        let store = InMemoryStateStore::new();
        let partition_count = 2;
        let bitcoin_partition = partition_for(CexId::Binance, pair(), partition_count);
        let other_partition = PartitionId::new((bitcoin_partition.get() + 1) % 2);
        let mut archive = VecArchive::new(vec![event(1, 61_000)]);

        let restored = restore_partition(
            &store,
            &mut archive,
            &options(other_partition, partition_count),
        )
        .await
        .unwrap();

        assert!(restored.rebuilt_from_archive);
        assert_eq!(restored.state.open_candles_len(), 0);
    }

    #[tokio::test]
    async fn rebuild_watermark_only_moves_forward_for_events_owned_by_the_partition() {
        let store = InMemoryStateStore::new();
        let partition_count = 2;
        let bitcoin_partition = partition_for(CexId::Binance, pair(), partition_count);
        let other_partition = PartitionId::new((bitcoin_partition.get() + 1) % 2);
        let mut archive = VecArchive::new(vec![event(1, 61_000)]);

        let restored = restore_partition(
            &store,
            &mut archive,
            &options(other_partition, partition_count),
        )
        .await
        .unwrap();

        assert_eq!(restored.state.watermark, None);
        assert_eq!(restored.replay_watermark(), None);
    }

    #[tokio::test]
    async fn archive_policy_ignores_a_compatible_checkpoint() {
        let store = InMemoryStateStore::new();
        let partition = PartitionId::new(0);
        let mut state = AggregatorState::new();
        state.mark_processed(10);
        store
            .save(&CheckpointRecord::new(
                partition,
                "aggregator-shards",
                11,
                state,
            ))
            .await
            .unwrap();

        let mut archive = VecArchive::new(vec![event(1, 61_000)]);
        let mut options = options(partition, 1);
        options.replay_start_policy = ReplayStartPolicy::Archive;
        let restored = restore_partition(&store, &mut archive, &options)
            .await
            .unwrap();

        assert_eq!(restored.restored_from, RestoreSource::Archive);
        assert!(restored.rebuilt_from_archive);
        assert_eq!(restored.start, ConsumerStart::Earliest);
        assert_eq!(restored.state.open_candles_len(), 1);
    }

    #[tokio::test]
    async fn earliest_policy_skips_checkpoint_and_archive() {
        let store = InMemoryStateStore::new();
        let partition = PartitionId::new(0);
        let mut archive = VecArchive::new(vec![event(1, 61_000)]);
        let mut options = options(partition, 1);
        options.replay_start_policy = ReplayStartPolicy::Earliest;

        let restored = restore_partition(&store, &mut archive, &options)
            .await
            .unwrap();

        assert_eq!(restored.restored_from, RestoreSource::Empty);
        assert!(!restored.rebuilt_from_archive);
        assert_eq!(restored.start, ConsumerStart::Earliest);
        assert_eq!(restored.state.schema_version, STATE_SCHEMA_VERSION);
        assert_eq!(restored.state.open_candles_len(), 0);
        assert_eq!(archive.events.len(), 1);
    }

    #[tokio::test]
    async fn latest_policy_starts_at_the_end_of_the_live_source() {
        let store = InMemoryStateStore::new();
        let partition = PartitionId::new(0);
        let mut archive = VecArchive::new(vec![event(1, 61_000)]);
        let mut options = options(partition, 1);
        options.replay_start_policy = ReplayStartPolicy::Latest;

        let restored = restore_partition(&store, &mut archive, &options)
            .await
            .unwrap();

        assert_eq!(restored.restored_from, RestoreSource::Empty);
        assert_eq!(restored.start, ConsumerStart::Latest);
        assert_eq!(restored.state.open_candles_len(), 0);
    }

    #[tokio::test]
    async fn configured_schema_mismatch_forces_archive_rebuild() {
        let store = InMemoryStateStore::new();
        let partition = PartitionId::new(0);
        let mut state = AggregatorState::new();
        state.mark_processed(10);
        store
            .save(&CheckpointRecord::new(
                partition,
                "aggregator-shards",
                11,
                state,
            ))
            .await
            .unwrap();

        let mut archive = VecArchive::new(vec![event(1, 61_000)]);
        let mut options = options(partition, 1);
        options.expected_schema_version = STATE_SCHEMA_VERSION + 1;
        let restored = restore_partition(&store, &mut archive, &options)
            .await
            .unwrap();

        assert_eq!(restored.restored_from, RestoreSource::Archive);
        assert!(restored.rebuilt_from_archive);
    }

    #[test]
    fn restore_options_come_from_the_aggregator_configuration() {
        use crate::config::AggregatorConfig;
        use platform_config::env_vars::EnvVars;

        let config = AggregatorConfig::from_vars(&EnvVars::from_map(vec![
            ("RUST_LOG", "info"),
            ("AGGREGATOR_NATS_URL", "nats://localhost:4222"),
            ("AGGREGATOR_EVENT_SUBJECT_PREFIX", "market.events"),
            (
                "AGGREGATOR_OPEN_CANDLE_SUBJECT_PREFIX",
                "market.candles.open",
            ),
            (
                "AGGREGATOR_CLOSED_CANDLE_SUBJECT_PREFIX",
                "market.candles.closed",
            ),
            ("AGGREGATOR_STATE_SCHEMA_VERSION", "4"),
            ("AGGREGATOR_REPLAY_START_POLICY", "archive"),
            ("AGGREGATOR_WATERMARK_IDLE_TIMEOUT_SECS", "12"),
        ]))
        .unwrap();
        let options =
            RestoreOptions::from_config(&config, PartitionId::new(2), "aggregator-shards");

        assert_eq!(options.partition, PartitionId::new(2));
        assert_eq!(options.expected_schema_version, 4);
        assert_eq!(options.replay_start_policy, ReplayStartPolicy::Archive);
        assert_eq!(
            config.watermark_idle_timeout,
            std::time::Duration::from_secs(12)
        );
    }

    #[test]
    fn archive_recovery_config_is_optional() {
        assert_eq!(
            ArchiveRecoveryConfig::from_vars(&EnvVars::from_map(Vec::<(&str, &str)>::new()))
                .unwrap(),
            None
        );
        let config = ArchiveRecoveryConfig::from_vars(&EnvVars::from_map(vec![(
            AGGREGATOR_ARCHIVE_S3_BUCKET,
            "market-data",
        )]))
        .unwrap()
        .unwrap();
        assert_eq!(config.bucket, "market-data");
        assert_eq!(config.prefix, DEFAULT_ARCHIVE_PREFIX);
    }

    #[test]
    fn trade_type_still_has_the_expected_shape_for_rebuild_tests() {
        // Keep the test helper's payload in sync with the Binance decoder.
        let event = event(7, 61_000);
        let decoder = TradeDecoderFactory::for_exchange(event.exchange).unwrap();
        let trades: Vec<Trade> = decoder.decode(&event).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].price, Decimal::from_str_exact("100").unwrap());
    }
}
