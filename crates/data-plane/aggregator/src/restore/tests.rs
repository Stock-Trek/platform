use super::catalog::{
    AGGREGATOR_ARCHIVE_S3_BUCKET, AGGREGATOR_ARCHIVE_S3_PREFIX, ArchiveObject,
    ArchiveRecoveryConfig, select_event_objects, sort_archive_objects,
};
use super::rebuild::{RestoreOptions, RestoreSource, rebuild_state_from_archive, restore_shard};
use super::replay::ArchivedEventSource;
use crate::checkpoint::{CheckpointRecord, InMemoryStateStore, StateStore};
use crate::config::ReplayStartPolicy;
use crate::processor::{CandleProcessor, ProcessorConfig};
use crate::shard::{ShardId, shard_for};
use crate::state::{AggregatorState, STATE_SCHEMA_VERSION};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use platform_archive::{ArchiveKind, ArchivePath, ArchivedMarketEvent, Bucket, ObjectKey};
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult, ConfigError, Retryability};
use platform_market_data::{
    candle::{Candle, CandleInterval, CandleKey},
    decoders::trade::{Trade, TradeDecoderFactory},
    event::MarketEvent,
};
use platform_nats::ConsumerStart;
use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

fn pair() -> TradingPair {
    TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD)
}

fn trade(timestamp: i64) -> Trade {
    Trade {
        exchange: CexId::Binance,
        trading_pair: pair(),
        price: Decimal::from_str_exact("100").unwrap(),
        quantity: Decimal::ONE,
        timestamp: TimestampMilliseconds(timestamp),
    }
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

struct FailingStateStore {
    retryability: Retryability,
}

#[async_trait]
impl StateStore for FailingStateStore {
    async fn load(&self, _shard: ShardId) -> BpResult<Option<CheckpointRecord>> {
        Err(match self.retryability {
            Retryability::Retryable => {
                BpError::external(std::io::Error::other("transient checkpoint read failure"))
            }
            Retryability::Permanent => {
                BpError::config_invalid("checkpoint", "corrupt checkpoint payload")
            }
        })
    }

    async fn save(&self, _checkpoint: &CheckpointRecord) -> BpResult<()> {
        Ok(())
    }
}

fn interval() -> CandleInterval {
    CandleInterval::from_label("1m").unwrap()
}

fn options(shard: ShardId, shard_count: usize) -> RestoreOptions {
    RestoreOptions {
        shard,
        source_stream: "aggregator-shards".to_string(),
        intervals: vec![interval()],
        shard_count,
        allowed_lateness: DurationMilliseconds::ZERO,
        max_closed_candles_per_interval: BTreeMap::from([(interval(), usize::MAX)]),
        correction_cutoff: DurationMilliseconds(i64::MAX),
        replay_start_policy: ReplayStartPolicy::Checkpoint,
        expected_schema_version: STATE_SCHEMA_VERSION,
    }
}

#[tokio::test]
async fn a_compatible_checkpoint_is_restored_without_replaying_the_archive() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);
    let mut state = AggregatorState::new();
    state.mark_processed(10);
    state.advance_watermark(TimestampMilliseconds(61_000));
    let checkpoint = CheckpointRecord::new(shard, "aggregator-shards", 11, state);
    store.save(&checkpoint).await.unwrap();

    let connects = Arc::new(AtomicUsize::new(0));
    let restored = restore_shard(
        &store,
        counting_archive(vec![event(1, 1_000)], connects.clone()),
        &options(shard, 1),
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
    let shard = ShardId::new(0);
    let restored = restore_shard(
        &store,
        archive_source(VecArchive::new(vec![event(1, 61_000), event(2, 62_000)])),
        &options(shard, 1),
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
async fn a_rebuild_continues_the_latest_archived_state_epoch_without_advancing_it() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);
    let archive = VecArchive::with_epochs(vec![(event(1, 61_000), 7), (event(2, 62_000), 3)]);

    let restored = restore_shard(&store, archive_source(archive), &options(shard, 1))
        .await
        .unwrap();

    assert!(restored.rebuilt_from_archive);
    assert_eq!(restored.state.state_epoch, 7);
}

#[tokio::test]
async fn an_incompatible_checkpoint_rebuilds_from_archived_events() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);
    let mut state = AggregatorState::new();
    state.schema_version = STATE_SCHEMA_VERSION + 1;
    let checkpoint = CheckpointRecord::new(shard, "aggregator-shards", 11, state);
    store.save(&checkpoint).await.unwrap();

    let restored = restore_shard(
        &store,
        archive_source(VecArchive::new(vec![event(1, 61_000)])),
        &options(shard, 1),
    )
    .await
    .unwrap();

    assert!(restored.rebuilt_from_archive);
    assert_eq!(restored.start_sequence(), None);
    assert_eq!(restored.state.open_candles_len(), 1);
    assert_eq!(restored.state.schema_version, STATE_SCHEMA_VERSION);
}

#[tokio::test]
async fn rebuild_only_applies_events_owned_by_the_shard() {
    let store = InMemoryStateStore::new();
    let shard_count = 2;
    let bitcoin_shard = shard_for(CexId::Binance, pair(), shard_count);
    let other_shard = ShardId::new((bitcoin_shard.get() + 1) % 2);
    let restored = restore_shard(
        &store,
        archive_source(VecArchive::new(vec![event(1, 61_000)])),
        &options(other_shard, shard_count),
    )
    .await
    .unwrap();

    assert!(restored.rebuilt_from_archive);
    assert_eq!(restored.state.open_candles_len(), 0);
}

#[tokio::test]
async fn rebuild_watermark_only_moves_forward_for_events_owned_by_the_shard() {
    let store = InMemoryStateStore::new();
    let shard_count = 2;
    let bitcoin_shard = shard_for(CexId::Binance, pair(), shard_count);
    let other_shard = ShardId::new((bitcoin_shard.get() + 1) % 2);
    let restored = restore_shard(
        &store,
        archive_source(VecArchive::new(vec![event(1, 61_000)])),
        &options(other_shard, shard_count),
    )
    .await
    .unwrap();

    assert_eq!(restored.state.watermark, None);
    assert_eq!(restored.replay_watermark(), None);
}

#[tokio::test]
async fn archive_policy_ignores_a_compatible_checkpoint() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);
    let mut state = AggregatorState::new();
    state.mark_processed(10);
    store
        .save(&CheckpointRecord::new(
            shard,
            "aggregator-shards",
            11,
            state,
        ))
        .await
        .unwrap();

    let connects = Arc::new(AtomicUsize::new(0));
    let mut options = options(shard, 1);
    options.replay_start_policy = ReplayStartPolicy::Archive;
    let restored = restore_shard(
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
    let shard = ShardId::new(0);
    let connects = Arc::new(AtomicUsize::new(0));
    let mut options = options(shard, 1);
    options.replay_start_policy = ReplayStartPolicy::Earliest;

    let restored = restore_shard(
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
    let shard = ShardId::new(0);
    let mut options = options(shard, 1);
    options.replay_start_policy = ReplayStartPolicy::Earliest;
    options.expected_schema_version = STATE_SCHEMA_VERSION + 2;

    let restored = restore_shard(
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
    let shard = ShardId::new(0);
    let mut options = options(shard, 1);
    options.replay_start_policy = ReplayStartPolicy::Latest;

    let restored = restore_shard(
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
    let shard = ShardId::new(0);
    let mut state = AggregatorState::new();
    state.mark_processed(10);
    store
        .save(&CheckpointRecord::new(
            shard,
            "aggregator-shards",
            11,
            state,
        ))
        .await
        .unwrap();

    let mut options = options(shard, 1);
    options.expected_schema_version = STATE_SCHEMA_VERSION + 1;
    let restored = restore_shard(
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
    let shard_count = 4;
    let shard = ShardId::new(2);
    let owned = (0..platform_archive::ROUTING_BUCKET_COUNT)
        .map(|index| platform_archive::RoutingBucket::from_index(index).unwrap())
        .find(|bucket| bucket.is_owned_by(shard.get(), shard_count))
        .unwrap();
    let other = (0..platform_archive::ROUTING_BUCKET_COUNT)
        .map(|index| platform_archive::RoutingBucket::from_index(index).unwrap())
        .find(|bucket| !bucket.is_owned_by(shard.get(), shard_count))
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
    let owned_compacted = ArchivePath::compacted_key(
        "market-data/compacted",
        &platform_archive::ArchivePartition::Events {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
        },
        TimestampMilliseconds(3_600_000),
        owned,
        "compacted.parquet",
    )
    .unwrap();
    let objects = vec![
        archive_object(&other_event),
        archive_object(&owned_candle),
        archive_object(&owned_compacted),
        archive_object(&owned_event),
    ];

    let mut selected = select_event_objects(objects, shard, shard_count);
    sort_archive_objects(&mut selected);

    assert_eq!(
        selected,
        vec![
            archive_object(&owned_event),
            archive_object(&owned_compacted)
        ]
    );
}

#[test]
fn archive_recovery_orders_files_by_coverage_hour_not_layer_prefix() {
    let bucket = platform_archive::RoutingBucket::from_index(0).unwrap();
    let older_raw = ArchivePath::raw_key(
        "market-data/raw",
        ArchiveKind::Events,
        TimestampMilliseconds(0),
        bucket,
        "older.parquet",
    )
    .unwrap();
    let newer_compacted = ArchivePath::compacted_key(
        "market-data/compacted",
        &platform_archive::ArchivePartition::Events {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
        },
        TimestampMilliseconds(3_600_000),
        bucket,
        "newer.parquet",
    )
    .unwrap();

    let mut objects = vec![archive_object(&newer_compacted), archive_object(&older_raw)];
    sort_archive_objects(&mut objects);

    assert_eq!(
        objects,
        vec![archive_object(&older_raw), archive_object(&newer_compacted)]
    );
}

#[test]
fn archive_recovery_ignores_event_objects_without_a_routing_bucket() {
    let key = "market-data/raw/events/year=2024/month=01/day=02/hour=03/legacy.parquet";

    let selected = select_event_objects(vec![archive_object(key)], ShardId::new(0), 1);

    assert!(selected.is_empty());
}

fn archive_object(key: &str) -> ArchiveObject {
    ArchiveObject {
        bucket: Bucket::new("market-data"),
        key: ObjectKey::new(key),
        size: 0,
    }
}

#[test]
fn archive_recovery_config_is_required_for_policies_that_replay_the_archive() {
    let vars = EnvVars::from_map(Vec::<(String, String)>::new());

    for policy in [ReplayStartPolicy::Checkpoint, ReplayStartPolicy::Archive] {
        assert!(matches!(
            ArchiveRecoveryConfig::from_vars_for_policy(&vars, policy).unwrap_err(),
            BpError::Config(ConfigError::Missing { name })
                if name == AGGREGATOR_ARCHIVE_S3_BUCKET
        ));
    }
}

#[test]
fn archive_recovery_config_is_optional_for_explicit_empty_starts() {
    let vars = EnvVars::from_map(Vec::<(String, String)>::new());

    for policy in [ReplayStartPolicy::Earliest, ReplayStartPolicy::Latest] {
        assert_eq!(
            ArchiveRecoveryConfig::from_vars_for_policy(&vars, policy).unwrap(),
            None
        );
    }
}

#[test]
fn optional_archive_recovery_config_validates_a_configured_bucket() {
    let vars = EnvVars::from_map([(AGGREGATOR_ARCHIVE_S3_BUCKET, "market-archive")]);

    assert!(matches!(
        ArchiveRecoveryConfig::optional_from_vars(&vars).unwrap_err(),
        BpError::Config(ConfigError::Missing { name })
            if name == AGGREGATOR_ARCHIVE_S3_PREFIX
    ));
}

#[tokio::test]
async fn an_empty_archive_starts_with_explicit_empty_state() {
    for policy in [ReplayStartPolicy::Checkpoint, ReplayStartPolicy::Archive] {
        let store = InMemoryStateStore::new();
        let shard = ShardId::new(0);
        let mut options = options(shard, 1);
        options.replay_start_policy = policy;

        let restored = restore_shard(
            &store,
            archive_source(VecArchive::new(Vec::new())),
            &options,
        )
        .await
        .unwrap();

        assert_eq!(restored.restored_from, RestoreSource::Empty);
        assert!(!restored.rebuilt_from_archive);
        assert_eq!(restored.start, ConsumerStart::Earliest);
        assert_eq!(restored.state.schema_version, STATE_SCHEMA_VERSION);
        assert_eq!(restored.state.open_candles_len(), 0);
    }
}

#[tokio::test]
async fn transient_checkpoint_read_failures_are_returned_instead_of_falling_back() {
    let store = FailingStateStore {
        retryability: Retryability::Retryable,
    };
    let shard = ShardId::new(0);
    let connects = Arc::new(AtomicUsize::new(0));

    let error = restore_shard(
        &store,
        counting_archive(vec![event(1, 61_000)], connects.clone()),
        &options(shard, 1),
    )
    .await
    .unwrap_err();

    assert!(error.retryability().is_retryable());
    assert_eq!(connects.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn permanent_checkpoint_read_failures_still_rebuild_from_the_archive() {
    let store = FailingStateStore {
        retryability: Retryability::Permanent,
    };
    let shard = ShardId::new(0);
    let connects = Arc::new(AtomicUsize::new(0));

    let restored = restore_shard(
        &store,
        counting_archive(vec![event(1, 61_000)], connects.clone()),
        &options(shard, 1),
    )
    .await
    .unwrap();

    assert_eq!(restored.restored_from, RestoreSource::Archive);
    assert!(restored.rebuilt_from_archive);
    assert_eq!(connects.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn checkpoint_fallback_without_archive_configuration_fails_fast() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);

    let error = restore_shard(
        &store,
        || async { Err(BpError::config_missing(AGGREGATOR_ARCHIVE_S3_BUCKET)) },
        &options(shard, 1),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        BpError::Config(ConfigError::Missing { name })
            if name == AGGREGATOR_ARCHIVE_S3_BUCKET
    ));
}

#[tokio::test]
async fn an_oversized_checkpoint_is_pruned_to_the_current_caps() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);
    let interval = interval();
    let mut state = AggregatorState::new();
    for timestamp in [60_000, 120_000, 180_000] {
        let mut candle = Candle::open(&trade(timestamp), interval).unwrap();
        candle.close();
        let evicted = state.retain_closed_candle(candle, usize::MAX);
        assert!(evicted.is_empty());
    }
    let checkpoint = CheckpointRecord::new(shard, "aggregator-shards", 11, state);
    store.save(&checkpoint).await.unwrap();

    let mut options = options(shard, 1);
    options.max_closed_candles_per_interval = BTreeMap::from([(interval, 1)]);
    let restored = restore_shard(
        &store,
        archive_source(VecArchive::new(Vec::new())),
        &options,
    )
    .await
    .unwrap();

    assert_eq!(restored.restored_from, RestoreSource::Checkpoint);
    assert_eq!(restored.state.closed_candles_len(), 1);
    let key = CandleKey::new(CexId::Binance, pair(), interval);
    assert!(
        restored
            .state
            .closed_candle(&key, TimestampMilliseconds(120_000))
            .is_none()
    );
    assert!(
        restored
            .state
            .closed_candle(&key, TimestampMilliseconds(180_000))
            .is_some()
    );
}

#[tokio::test]
async fn restore_prunes_only_the_intervals_whose_cap_shrank() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);
    let one_minute = interval();
    let five_minutes = CandleInterval::from_label("5m").unwrap();
    let mut state = AggregatorState::new();
    for (interval, timestamp) in [
        (one_minute, 60_000),
        (one_minute, 120_000),
        (five_minutes, 0),
        (five_minutes, 300_000),
    ] {
        let mut candle = Candle::open(&trade(timestamp), interval).unwrap();
        candle.close();
        state.retain_closed_candle(candle, usize::MAX);
    }
    let checkpoint = CheckpointRecord::new(shard, "aggregator-shards", 11, state);
    store.save(&checkpoint).await.unwrap();

    let mut options = options(shard, 1);
    options.intervals = vec![one_minute, five_minutes];
    options.max_closed_candles_per_interval = BTreeMap::from([(one_minute, 1), (five_minutes, 2)]);
    let restored = restore_shard(
        &store,
        archive_source(VecArchive::new(Vec::new())),
        &options,
    )
    .await
    .unwrap();

    assert_eq!(restored.restored_from, RestoreSource::Checkpoint);
    let pair = pair();
    let one_minute_key = CandleKey::new(CexId::Binance, pair, one_minute);
    let five_minute_key = CandleKey::new(CexId::Binance, pair, five_minutes);
    assert_eq!(
        restored.state.closed_candles_len_by_interval(),
        BTreeMap::from([(one_minute, 1), (five_minutes, 2)])
    );
    assert!(
        restored
            .state
            .closed_candle(&one_minute_key, TimestampMilliseconds(60_000))
            .is_none()
    );
    assert!(
        restored
            .state
            .closed_candle(&five_minute_key, TimestampMilliseconds(0))
            .is_some()
    );
    assert!(
        restored
            .state
            .closed_candle(&five_minute_key, TimestampMilliseconds(300_000))
            .is_some()
    );
}

#[tokio::test]
async fn archive_rebuild_uses_the_same_caps_and_cutoff_as_live_processing() {
    let interval = interval();
    let shard = ShardId::new(0);
    let caps = BTreeMap::from([(interval, 1)]);
    let cutoff = DurationMilliseconds(1_000);
    let events = vec![
        event(1, 61_000),
        event(2, 121_000),
        event(3, 181_000),
        event(4, 61_500),
    ];
    let config = ProcessorConfig::new(
        vec![interval],
        DurationMilliseconds::ZERO,
        caps.clone(),
        cutoff,
    );
    let mut live = CandleProcessor::with_config(config);
    for event in &events {
        live.apply_event(event, &[trade(event.exchange_timestamp.unwrap().0)]);
    }
    let live_state = live.into_state();

    let archive_state = rebuild_state_from_archive(
        &mut VecArchive::new(events),
        shard,
        &[interval],
        1,
        DurationMilliseconds::ZERO,
        caps,
        cutoff,
    )
    .await
    .unwrap();

    assert_eq!(live_state, archive_state);
    assert_eq!(
        archive_state.closed_candles_len_by_interval(),
        BTreeMap::from([(interval, 1)])
    );
    let key = CandleKey::new(CexId::Binance, pair(), interval);
    assert!(
        archive_state
            .closed_candle(&key, TimestampMilliseconds(120_000))
            .is_some()
    );
    assert!(
        archive_state
            .closed_candle(&key, TimestampMilliseconds(60_000))
            .is_none()
    );
}
