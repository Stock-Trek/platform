//! Checkpoint restore and archive rebuild for aggregator partitions.
//!
//! Recovery prefers the latest compatible checkpoint because it contains the
//! exact keyed state and the next source sequence. If the checkpoint is
//! missing, stale, or from an incompatible schema, the partition state is
//! rebuilt from the raw event archive instead.

use crate::aggregator::CandleAggregator;
use crate::checkpoint::StateStore;
use crate::partition::{PartitionId, partition_for};
use crate::state::AggregatorState;
use async_trait::async_trait;
use platform_archive::{ArchiveKind, ArchiveParquet, ArchivePath, Bucket, ObjectKey, S3};
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_market_data::{
    candle::CandleInterval,
    decoders::trade::{SharedTradeDecoder, TradeDecoderFactory},
    event::MarketEvent,
};
use platform_types::new_types::TimestampMilliseconds;
use std::collections::{HashMap, VecDeque};
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
    /// Source sequence to start from, or `None` when the source must replay
    /// from the beginning (the archive-backed rebuild case).
    pub start_sequence: Option<u64>,
    /// Whether the state was rebuilt by replaying archived raw events.
    pub rebuilt_from_archive: bool,
}

/// Alias used by startup code.
pub type RestoreResult = RestoredPartition;

impl RestoredPartition {
    pub fn replay_watermark(&self) -> Option<TimestampMilliseconds> {
        if self.rebuilt_from_archive {
            self.state.watermark
        } else {
            None
        }
    }
}

/// Restores a partition from its latest checkpoint or rebuilds it from archived
/// raw events.
///
/// A checkpoint is accepted only when it belongs to this partition, names the
/// same source stream, and has a compatible state schema. Otherwise the archive
/// is replayed from the beginning and the caller starts the live source from
/// the beginning as well. Events at or before the rebuilt watermark are then
/// ignored by [`crate::runner::AggregatorRunner`] to avoid applying state twice.
pub async fn restore_partition(
    store: &dyn StateStore,
    archive: &mut dyn ArchivedEventSource,
    partition: PartitionId,
    source_stream: &str,
    intervals: &[CandleInterval],
    partition_count: usize,
) -> BpResult<RestoredPartition> {
    match store.load(partition).await {
        Ok(Some(checkpoint)) if checkpoint.is_compatible(partition, source_stream) => {
            info!(
                partition = %partition,
                next_sequence = checkpoint.next_sequence,
                watermark = ?checkpoint.watermark,
                open_candles = checkpoint.state.open_candles_len(),
                "restored aggregator partition from checkpoint"
            );
            return Ok(RestoredPartition {
                state: checkpoint.state,
                start_sequence: Some(checkpoint.next_sequence),
                rebuilt_from_archive: false,
            });
        }
        Ok(Some(checkpoint)) => {
            warn!(
                partition = %partition,
                source_stream = %checkpoint.source_stream,
                schema_version = checkpoint.state.schema_version,
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
            warn!(
                partition = %partition,
                error = %error,
                "failed to load checkpoint; rebuilding from archive"
            );
        }
    }

    let state = rebuild_state_from_archive(archive, partition, intervals, partition_count).await?;
    info!(
        partition = %partition,
        watermark = ?state.watermark,
        open_candles = state.open_candles_len(),
        "rebuilt aggregator partition state from archived raw events"
    );
    Ok(RestoredPartition {
        state,
        start_sequence: None,
        rebuilt_from_archive: true,
    })
}

/// Replays all archived events for one partition into a fresh keyed state.
pub async fn rebuild_state_from_archive(
    archive: &mut dyn ArchivedEventSource,
    partition: PartitionId,
    intervals: &[CandleInterval],
    partition_count: usize,
) -> BpResult<AggregatorState> {
    let mut aggregator = CandleAggregator::new(intervals.to_vec());
    let mut decoders = HashMap::new();

    while let Some(event) = archive.next_event().await? {
        let decoder = decoder_for(&mut decoders, event.exchange)?;
        let trades = decoder.decode(&event)?;
        for trade in trades {
            if partition_for(trade.exchange, trade.trading_pair, partition_count) == partition {
                // Closed candles produced during rebuild are already represented
                // by the archive's archived candle records; only the open-candle
                // state is needed here.
                let _ = aggregator.apply_trade(&trade);
            }
        }
    }

    Ok(aggregator.into_state())
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
        let restored = restore_partition(
            &store,
            &mut archive,
            partition,
            "aggregator-shards",
            &[interval()],
            1,
        )
        .await
        .unwrap();

        assert_eq!(restored.start_sequence, Some(11));
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

        let restored = restore_partition(
            &store,
            &mut archive,
            partition,
            "aggregator-shards",
            &[interval()],
            1,
        )
        .await
        .unwrap();

        assert_eq!(restored.start_sequence, None);
        assert!(restored.rebuilt_from_archive);
        assert_eq!(
            restored.replay_watermark(),
            Some(TimestampMilliseconds(62_000))
        );
        assert_eq!(restored.state.open_candles_len(), 1);
        let candle = restored.state.open_candles.values().next().unwrap();
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
        let restored = restore_partition(
            &store,
            &mut archive,
            partition,
            "aggregator-shards",
            &[interval()],
            1,
        )
        .await
        .unwrap();

        assert!(restored.rebuilt_from_archive);
        assert_eq!(restored.start_sequence, None);
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
            other_partition,
            "aggregator-shards",
            &[interval()],
            partition_count,
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
            other_partition,
            "aggregator-shards",
            &[interval()],
            partition_count,
        )
        .await
        .unwrap();

        assert_eq!(restored.state.watermark, None);
        assert_eq!(restored.replay_watermark(), None);
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
