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
use crate::state::{AggregatorState, STATE_SCHEMA_VERSION};
use async_trait::async_trait;
use aws_sdk_s3_transfer_manager::Client as TransferClient;
use platform_archive::{
    ArchiveKind, ArchiveParquet, ArchivePath, ArchivedMarketEvent, Bucket, ObjectKey,
    ParquetReader, S3, S3Object,
};
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
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use stock_trek_types::cex::cex_id::CexId;
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};
use tracing::{info, warn};

pub const AGGREGATOR_ARCHIVE_S3_BUCKET: &str = "AGGREGATOR_ARCHIVE_S3_BUCKET";
pub const AGGREGATOR_ARCHIVE_S3_PREFIX: &str = "AGGREGATOR_ARCHIVE_S3_PREFIX";

const DEFAULT_ARCHIVE_PREFIX: &str = "market-data/raw";

/// Maximum number of archive objects streamed concurrently across all
/// partitions in one aggregator process.
const MAX_CONCURRENT_ARCHIVE_OBJECTS: usize = 4;

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
    async fn next_event(&mut self) -> BpResult<Option<ArchivedMarketEvent>>;
}

/// Archive source used when recovery from archived events is not configured.
pub struct EmptyArchivedEventSource;

#[async_trait]
impl ArchivedEventSource for EmptyArchivedEventSource {
    async fn next_event(&mut self) -> BpResult<Option<ArchivedMarketEvent>> {
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
///
/// `connect_archive` is only invoked when archive replay is required, so a
/// valid checkpoint or an `earliest`/`latest` start performs no archive I/O.
pub async fn restore_partition<F, Fut>(
    store: &dyn StateStore,
    connect_archive: F,
    options: &RestoreOptions,
) -> BpResult<RestoredPartition>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = BpResult<Box<dyn ArchivedEventSource>>>,
{
    let started = Instant::now();
    let result = restore_partition_inner(store, connect_archive, options).await;
    let outcome = match &result {
        Ok(restored) => restored.restored_from.label(),
        Err(_) => "error",
    };
    AggregatorMetrics::record_state_restore(options.partition.get(), outcome, started.elapsed());
    result
}

async fn restore_partition_inner<F, Fut>(
    store: &dyn StateStore,
    connect_archive: F,
    options: &RestoreOptions,
) -> BpResult<RestoredPartition>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = BpResult<Box<dyn ArchivedEventSource>>>,
{
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
                state: AggregatorState::new_with_schema_version(options.expected_schema_version),
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
    let mut archive = connect_archive().await?;
    let state = rebuild_state_from_archive_with_schema(
        archive.as_mut(),
        partition,
        &options.intervals,
        options.partition_count,
        options.allowed_lateness,
        options.expected_schema_version,
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
    rebuild_state_from_archive_with_schema(
        archive,
        partition,
        intervals,
        partition_count,
        allowed_lateness,
        STATE_SCHEMA_VERSION,
    )
    .await
}

/// Replays all archived events for one partition into fresh keyed state with
/// the configured schema version.
pub async fn rebuild_state_from_archive_with_schema(
    archive: &mut dyn ArchivedEventSource,
    partition: PartitionId,
    intervals: &[CandleInterval],
    partition_count: usize,
    allowed_lateness: DurationMilliseconds,
    state_schema_version: u32,
) -> BpResult<AggregatorState> {
    let config = ProcessorConfig::new(intervals.to_vec(), allowed_lateness)
        .with_state_schema_version(state_schema_version);
    let mut processor = CandleProcessor::with_config(config);
    let mut decoders = HashMap::new();
    let mut replayed = 0_u64;

    while let Some(archived) = archive.next_event().await? {
        replayed = replayed.saturating_add(1);
        if replayed.is_multiple_of(1_000) {
            AggregatorMetrics::record_replay_events(partition.get(), "archive", 1_000);
        }
        let event = &archived.event;
        let decoder = decoder_for(&mut decoders, event.exchange)?;
        let trades = match decoder.decode(event) {
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
            let state = processor.state_mut();
            state.state_epoch = state.state_epoch.max(archived.state_epoch);
            let _ = processor.apply_event(event, &trades);
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

/// Lazily connects to the archive and shares its clients and object listing
/// across every partition recovery source in this process.
///
/// The snapshot is created on the first partition that actually needs archive
/// replay, so checkpoints and live-only starts never touch S3. The listing is
/// startup-scoped; archive growth during recovery is not observed by later
/// partitions.
pub struct S3ArchiveRecovery {
    config: ArchiveRecoveryConfig,
    snapshot: OnceCell<ArchiveSnapshot>,
    recovery_slots: Arc<Semaphore>,
}

struct ArchiveSnapshot {
    transfer_client: TransferClient,
    bucket: Bucket,
    prefix: String,
    objects: Vec<S3Object>,
}

impl S3ArchiveRecovery {
    pub fn new(config: ArchiveRecoveryConfig) -> Self {
        Self {
            config,
            snapshot: OnceCell::new(),
            recovery_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_ARCHIVE_OBJECTS)),
        }
    }

    /// Builds a partition-specific source from the shared archive snapshot.
    ///
    /// The first call performs the S3 client setup and listing; later calls
    /// reuse both and only select the objects owned by their partition.
    pub async fn connect(
        &self,
        partition: PartitionId,
        partition_count: usize,
    ) -> BpResult<S3ArchivedEventSource> {
        let snapshot = self
            .snapshot
            .get_or_try_init(|| async {
                let (client, transfer_client) = S3::clients().await;
                let bucket = Bucket::new(self.config.bucket.clone());
                let prefix = self.config.prefix.trim_matches('/').to_string();
                let list_prefix = if prefix.is_empty() {
                    String::new()
                } else {
                    format!("{prefix}/")
                };
                let objects =
                    S3::list_objects(&client, &bucket, &ObjectKey::new(list_prefix)).await?;
                Ok::<_, BpError>(ArchiveSnapshot {
                    transfer_client,
                    bucket,
                    prefix,
                    objects,
                })
            })
            .await?;

        let mut objects = select_event_objects(
            snapshot.objects.clone(),
            &snapshot.prefix,
            partition,
            partition_count,
        );
        objects.sort_by(|left, right| left.key.cmp(&right.key));

        info!(
            bucket = %snapshot.bucket,
            prefix = %snapshot.prefix,
            objects = objects.len(),
            partition = %partition,
            "connected archive recovery source"
        );

        Ok(S3ArchivedEventSource {
            transfer_client: snapshot.transfer_client.clone(),
            bucket: snapshot.bucket.clone(),
            partition,
            partition_count,
            objects: objects.into(),
            reader: None,
            _reader_permit: None,
            recovery_slots: self.recovery_slots.clone(),
            events: VecDeque::new(),
            decoders: HashMap::new(),
        })
    }
}

/// Replays raw event objects from S3 for one partition.
///
/// Objects are read in bounded Parquet batches through ranged S3 requests, so
/// recovery never downloads or decodes a whole object at once. A
/// process-wide semaphore bounds how many archive objects are streamed
/// concurrently across all partitions.
pub struct S3ArchivedEventSource {
    transfer_client: TransferClient,
    bucket: Bucket,
    partition: PartitionId,
    partition_count: usize,
    objects: VecDeque<S3Object>,
    reader: Option<platform_archive::ArchiveParquetStream<ParquetReader>>,
    /// Keeps the recovery slot held for as long as an object is being streamed.
    _reader_permit: Option<OwnedSemaphorePermit>,
    recovery_slots: Arc<Semaphore>,
    events: VecDeque<ArchivedMarketEvent>,
    decoders: HashMap<CexId, SharedTradeDecoder>,
}

impl S3ArchivedEventSource {
    pub async fn connect(
        config: &ArchiveRecoveryConfig,
        partition: PartitionId,
        partition_count: usize,
    ) -> BpResult<Self> {
        S3ArchiveRecovery::new(config.clone())
            .connect(partition, partition_count)
            .await
    }

    async fn load_next_batch(&mut self) -> BpResult<bool> {
        loop {
            if self.reader.is_none() {
                let Some(object) = self.objects.pop_front() else {
                    return Ok(false);
                };
                let file_size = u64::try_from(object.size).map_err(|error| {
                    BpError::external(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "archive object {} has invalid size {}: {error}",
                            object.key, object.size
                        ),
                    ))
                })?;
                let permit =
                    self.recovery_slots
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|error| {
                            BpError::external(std::io::Error::other(format!(
                                "archive recovery limiter closed: {error}"
                            )))
                        })?;
                let reader =
                    ParquetReader::new(&self.transfer_client, &self.bucket, &object.key, file_size);
                self.reader =
                    Some(ArchiveParquet::decode_reader_batches(ArchiveKind::Events, reader).await?);
                self._reader_permit = Some(permit);
                info!(
                    key = %object.key,
                    "streaming archive object for recovery"
                );
            }

            let Some(batch) = self
                .reader
                .as_mut()
                .expect("archive reader is initialized")
                .next_batch()
                .await?
            else {
                self.reader = None;
                self._reader_permit = None;
                continue;
            };
            let mut events = batch
                .into_records()
                .into_iter()
                .filter_map(|record| match record {
                    platform_archive::ArchiveRecord::MarketEvent(event) => Some(event),
                    platform_archive::ArchiveRecord::ClosedCandle(_) => None,
                })
                .collect::<Vec<_>>();
            events.sort_by_key(|event| {
                (
                    event_timestamp(&event.event).0,
                    event.event.sequence.unwrap_or_default(),
                    event.state_epoch,
                )
            });
            if events.is_empty() {
                continue;
            }
            self.events.extend(events);
            info!(
                queued_events = self.events.len(),
                "loaded archive batch for recovery"
            );
            return Ok(true);
        }
    }
}

/// Selects the raw-event objects owned by one partition.
///
/// Archive objects are written under a fixed virtual routing bucket, so a
/// partition can discard every object belonging to another partition before
/// downloading it. This keeps a cold rebuild at `O(archive)` across the cluster
/// instead of making every partition scan the whole archive.
fn select_event_objects(
    objects: Vec<S3Object>,
    prefix: &str,
    partition: PartitionId,
    partition_count: usize,
) -> Vec<S3Object> {
    objects
        .into_iter()
        .filter(|object| {
            let Some(relative) = ArchivePath::relative_key(object.key.as_str(), prefix) else {
                return false;
            };
            if ArchivePath::kind(relative) != Some(ArchiveKind::Events) {
                return false;
            }
            ArchivePath::routing_bucket_from_key(relative)
                .is_some_and(|bucket| bucket.is_owned_by(partition.get(), partition_count))
        })
        .collect()
}

#[async_trait]
impl ArchivedEventSource for S3ArchivedEventSource {
    async fn next_event(&mut self) -> BpResult<Option<ArchivedMarketEvent>> {
        loop {
            if let Some(event) = self.events.pop_front() {
                let decoder = decoder_for(&mut self.decoders, event.exchange)?;
                let trades = decoder.decode(&event.event)?;
                if trades.iter().any(|trade| {
                    partition_for(trade.exchange, trade.trading_pair, self.partition_count)
                        == self.partition
                }) {
                    return Ok(Some(event));
                }
                continue;
            }
            if !self.load_next_batch().await? {
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
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
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
        events: VecDeque<ArchivedMarketEvent>,
    }

    impl VecArchive {
        fn new(events: Vec<MarketEvent>) -> Self {
            Self {
                events: events.into_iter().map(ArchivedMarketEvent::from).collect(),
            }
        }

        fn with_epochs(events: Vec<(MarketEvent, u64)>) -> Self {
            Self {
                events: events
                    .into_iter()
                    .map(|(event, epoch)| ArchivedMarketEvent::new(event, epoch))
                    .collect(),
            }
        }
    }

    #[async_trait]
    impl ArchivedEventSource for VecArchive {
        async fn next_event(&mut self) -> BpResult<Option<ArchivedMarketEvent>> {
            Ok(self.events.pop_front())
        }
    }

    fn archive_source(
        archive: VecArchive,
    ) -> impl FnOnce() -> std::future::Ready<BpResult<Box<dyn ArchivedEventSource>>> {
        || std::future::ready(Ok(Box::new(archive) as Box<dyn ArchivedEventSource>))
    }

    fn counting_archive(
        events: Vec<MarketEvent>,
        connects: Arc<AtomicUsize>,
    ) -> impl FnOnce() -> std::future::Ready<BpResult<Box<dyn ArchivedEventSource>>> {
        move || {
            connects.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(
                Box::new(VecArchive::new(events)) as Box<dyn ArchivedEventSource>
            ))
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

        let connects = Arc::new(AtomicUsize::new(0));
        let restored = restore_partition(
            &store,
            counting_archive(vec![event(1, 1_000)], connects.clone()),
            &options(partition, 1),
        )
        .await
        .unwrap();

        assert_eq!(restored.start_sequence(), Some(11));
        assert_eq!(restored.start, ConsumerStart::Sequence(11));
        assert!(!restored.rebuilt_from_archive);
        assert_eq!(
            restored.state.watermark,
            Some(TimestampMilliseconds(61_000))
        );
        assert_eq!(connects.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_missing_checkpoint_rebuilds_from_archived_events() {
        let store = InMemoryStateStore::new();
        let partition = PartitionId::new(0);
        let restored = restore_partition(
            &store,
            archive_source(VecArchive::new(vec![event(1, 61_000), event(2, 62_000)])),
            &options(partition, 1),
        )
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
    async fn a_rebuild_continues_the_latest_archived_state_epoch() {
        let store = InMemoryStateStore::new();
        let partition = PartitionId::new(0);
        let archive = VecArchive::with_epochs(vec![(event(1, 61_000), 7), (event(2, 62_000), 3)]);

        let restored = restore_partition(&store, archive_source(archive), &options(partition, 1))
            .await
            .unwrap();

        assert!(restored.rebuilt_from_archive);
        assert_eq!(restored.state.state_epoch, 7);
    }

    #[tokio::test]
    async fn an_incompatible_checkpoint_rebuilds_from_archived_events() {
        let store = InMemoryStateStore::new();
        let partition = PartitionId::new(0);
        let mut state = AggregatorState::new();
        state.schema_version = STATE_SCHEMA_VERSION + 1;
        let checkpoint = CheckpointRecord::new(partition, "aggregator-shards", 11, state);
        store.save(&checkpoint).await.unwrap();

        let restored = restore_partition(
            &store,
            archive_source(VecArchive::new(vec![event(1, 61_000)])),
            &options(partition, 1),
        )
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
        let restored = restore_partition(
            &store,
            archive_source(VecArchive::new(vec![event(1, 61_000)])),
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
        let restored = restore_partition(
            &store,
            archive_source(VecArchive::new(vec![event(1, 61_000)])),
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

        let connects = Arc::new(AtomicUsize::new(0));
        let mut options = options(partition, 1);
        options.replay_start_policy = ReplayStartPolicy::Archive;
        let restored = restore_partition(
            &store,
            counting_archive(vec![event(1, 61_000)], connects.clone()),
            &options,
        )
        .await
        .unwrap();

        assert_eq!(restored.restored_from, RestoreSource::Archive);
        assert!(restored.rebuilt_from_archive);
        assert_eq!(restored.start, ConsumerStart::Earliest);
        assert_eq!(restored.state.open_candles_len(), 1);
        assert_eq!(connects.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn earliest_policy_skips_checkpoint_and_archive() {
        let store = InMemoryStateStore::new();
        let partition = PartitionId::new(0);
        let connects = Arc::new(AtomicUsize::new(0));
        let mut options = options(partition, 1);
        options.replay_start_policy = ReplayStartPolicy::Earliest;

        let restored = restore_partition(
            &store,
            counting_archive(vec![event(1, 61_000)], connects.clone()),
            &options,
        )
        .await
        .unwrap();

        assert_eq!(restored.restored_from, RestoreSource::Empty);
        assert!(!restored.rebuilt_from_archive);
        assert_eq!(restored.start, ConsumerStart::Earliest);
        assert_eq!(restored.state.schema_version, STATE_SCHEMA_VERSION);
        assert_eq!(restored.state.open_candles_len(), 0);
        assert_eq!(connects.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn empty_state_uses_the_configured_schema_version() {
        let store = InMemoryStateStore::new();
        let partition = PartitionId::new(0);
        let mut options = options(partition, 1);
        options.replay_start_policy = ReplayStartPolicy::Earliest;
        options.expected_schema_version = STATE_SCHEMA_VERSION + 2;

        let restored = restore_partition(
            &store,
            archive_source(VecArchive::new(vec![event(1, 61_000)])),
            &options,
        )
        .await
        .unwrap();

        assert_eq!(restored.restored_from, RestoreSource::Empty);
        assert_eq!(restored.state.schema_version, STATE_SCHEMA_VERSION + 2);
    }

    #[tokio::test]
    async fn latest_policy_starts_at_the_end_of_the_live_source() {
        let store = InMemoryStateStore::new();
        let partition = PartitionId::new(0);
        let mut options = options(partition, 1);
        options.replay_start_policy = ReplayStartPolicy::Latest;

        let restored = restore_partition(
            &store,
            archive_source(VecArchive::new(vec![event(1, 61_000)])),
            &options,
        )
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

        let mut options = options(partition, 1);
        options.expected_schema_version = STATE_SCHEMA_VERSION + 1;
        let restored = restore_partition(
            &store,
            archive_source(VecArchive::new(vec![event(1, 61_000)])),
            &options,
        )
        .await
        .unwrap();

        assert_eq!(restored.restored_from, RestoreSource::Archive);
        assert!(restored.rebuilt_from_archive);
        assert_eq!(restored.state.schema_version, STATE_SCHEMA_VERSION + 1);
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

    #[test]
    fn archive_recovery_selects_only_owned_event_objects() {
        let prefix = "market-data/raw";
        let partition_count = 4;
        let partition = PartitionId::new(2);
        let owned = (0..platform_archive::ROUTING_BUCKET_COUNT)
            .map(|index| platform_archive::RoutingBucket::from_index(index).unwrap())
            .find(|bucket| bucket.is_owned_by(partition.get(), partition_count))
            .unwrap();
        let other = (0..platform_archive::ROUTING_BUCKET_COUNT)
            .map(|index| platform_archive::RoutingBucket::from_index(index).unwrap())
            .find(|bucket| !bucket.is_owned_by(partition.get(), partition_count))
            .unwrap();

        let owned_event = ArchivePath::raw_key(
            prefix,
            ArchiveKind::Events,
            TimestampMilliseconds(0),
            owned,
            "owned.parquet",
        )
        .unwrap();
        let other_event = ArchivePath::raw_key(
            prefix,
            ArchiveKind::Events,
            TimestampMilliseconds(0),
            other,
            "other.parquet",
        )
        .unwrap();
        let owned_candle = ArchivePath::raw_key(
            prefix,
            ArchiveKind::Candles,
            TimestampMilliseconds(0),
            owned,
            "candles.parquet",
        )
        .unwrap();
        let objects = vec![
            s3_object(&other_event),
            s3_object(&owned_candle),
            s3_object(&owned_event),
        ];

        let selected = select_event_objects(objects, prefix, partition, partition_count);

        assert_eq!(selected, vec![s3_object(&owned_event)]);
    }

    #[test]
    fn archive_recovery_ignores_event_objects_without_a_routing_bucket() {
        let prefix = "market-data/raw";
        let key = "market-data/raw/events/year=2024/month=01/day=02/hour=03/legacy.parquet";

        let selected = select_event_objects(vec![s3_object(key)], prefix, PartitionId::new(0), 1);

        assert!(selected.is_empty());
    }

    fn s3_object(key: &str) -> S3Object {
        S3Object {
            key: ObjectKey::new(key),
            last_modified_millis: None,
            size: 0,
        }
    }
}
