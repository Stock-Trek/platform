use crate::checkpoint_record::{CheckpointRecord, StateStore};
use crate::config::ReplayStartPolicy;
use crate::restore::catalog::ArchiveObject;
use crate::restore::replay::ArchivedEventSource;
use crate::restore::restored_shard::RestoreOptions;
use crate::shard_id::ShardId;
use crate::state::STATE_SCHEMA_VERSION;
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use platform_archive::{ArchivedMarketEvent, Bucket, ObjectKey};
use platform_errors::{BuildError, BuildResult, Retryability};
use platform_market_data::{candle::AlignedWindow, decoders::trade::Trade, event::MarketEvent};
use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

pub(super) struct VecArchive {
    events: VecDeque<ArchivedMarketEvent>,
}

impl VecArchive {
    pub(super) fn new(events: Vec<MarketEvent>) -> Self {
        Self {
            events: events.into_iter().map(ArchivedMarketEvent::from).collect(),
        }
    }

    pub(super) fn with_epochs(events: Vec<(MarketEvent, u64)>) -> Self {
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
    async fn next_event(&mut self) -> BuildResult<Option<ArchivedMarketEvent>> {
        Ok(self.events.pop_front())
    }
}

pub(super) struct FailingStateStore {
    pub(super) retryability: Retryability,
}

#[async_trait]
impl StateStore for FailingStateStore {
    async fn load(&self, _shard: ShardId) -> BuildResult<Option<CheckpointRecord>> {
        Err(match self.retryability {
            Retryability::Retryable => {
                BuildError::external(std::io::Error::other("transient checkpoint read failure"))
            }
            Retryability::Permanent => {
                BuildError::config_invalid("checkpoint", "corrupt checkpoint payload")
            }
        })
    }

    async fn save(&self, _checkpoint: &CheckpointRecord) -> BuildResult<()> {
        Ok(())
    }
}

pub(super) struct Fixtures;

impl Fixtures {
    pub(super) fn pair() -> TradingPair {
        TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD)
    }

    pub(super) fn trade(timestamp: i64) -> Trade {
        Trade {
            exchange: CexId::Binance,
            trading_pair: Self::pair(),
            price: Decimal::from_str_exact("100").unwrap(),
            quantity: Decimal::ONE,
            timestamp: TimestampMilliseconds(timestamp),
        }
    }

    pub(super) fn event(sequence: u64, timestamp: i64) -> MarketEvent {
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

    pub(super) fn non_trade_event(sequence: u64, timestamp: i64) -> MarketEvent {
        MarketEvent {
            stream: "btcusdt@depth".to_string(),
            payload: String::new(),
            ..Self::event(sequence, timestamp)
        }
    }

    pub(super) fn undecodable_event(sequence: u64, timestamp: i64) -> MarketEvent {
        MarketEvent {
            payload: STANDARD.encode("not a market event"),
            ..Self::event(sequence, timestamp)
        }
    }

    pub(super) fn archive_source(
        archive: VecArchive,
    ) -> impl FnOnce() -> std::future::Ready<BuildResult<Box<dyn ArchivedEventSource>>> {
        || std::future::ready(Ok(Box::new(archive) as Box<dyn ArchivedEventSource>))
    }

    pub(super) fn counting_archive(
        events: Vec<MarketEvent>,
        connects: Arc<AtomicUsize>,
    ) -> impl FnOnce() -> std::future::Ready<BuildResult<Box<dyn ArchivedEventSource>>> {
        move || {
            connects.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(
                Box::new(VecArchive::new(events)) as Box<dyn ArchivedEventSource>
            ))
        }
    }

    pub(super) fn interval() -> AlignedWindow {
        AlignedWindow::try_from("1m").unwrap()
    }

    pub(super) fn options(shard: ShardId, shard_count: usize) -> RestoreOptions {
        RestoreOptions {
            shard,
            source_stream: "aggregator-shards".to_string(),
            intervals: vec![Self::interval()],
            shard_count,
            allowed_lateness: DurationMilliseconds::ZERO,
            max_closed_candles_per_interval: BTreeMap::from([(Self::interval(), usize::MAX)]),
            correction_cutoff: DurationMilliseconds(i64::MAX),
            replay_start_policy: ReplayStartPolicy::Checkpoint,
            expected_schema_version: STATE_SCHEMA_VERSION,
        }
    }

    pub(super) fn archive_object(key: &str) -> ArchiveObject {
        ArchiveObject {
            bucket: Bucket::new("market-data"),
            key: ObjectKey::new(key),
            size: 0,
        }
    }
}
