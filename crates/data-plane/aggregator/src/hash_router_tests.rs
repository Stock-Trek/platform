use crate::{
    HashRouter, NatsRouter,
    market_source::{MarketInput, MarketSource, SourceRecord},
    shard_id::ShardId,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::BuildResult;
use platform_market_data::{AlignedWindow, CandleKey, GapMarker, MarketEvent};
use platform_nats::EventPublisher;
use platform_types::{DurationMilliseconds, TimestampMilliseconds};
use std::sync::Arc;
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId};

fn trade_event() -> MarketEvent {
    MarketEvent {
        exchange: CexId::Binance,
        stream: "btcusdt@trade".to_string(),
        sequence: Some(1),
        exchange_timestamp: Some(TimestampMilliseconds(61_000)),
        received_at: TimestampMilliseconds(61_001),
        payload: "eyJlIjoidHJhZGUiLCJzIjoiQlRDVVNEVCIsInAiOiIxMDAiLCJxIjoiMSIsIlQiOjYxMDAwfQ=="
            .to_string(),
    }
}

fn non_trade_event() -> MarketEvent {
    MarketEvent {
        stream: "btcusdt@depth".to_string(),
        sequence: Some(2),
        payload: String::new(),
        ..trade_event()
    }
}

fn gap() -> GapMarker {
    GapMarker {
        exchange: CexId::Binance,
        stream: "btcusdt@trade".to_string(),
        previous_sequence: 1,
        observed_sequence: 3,
        missing_from: 2,
        missing_to: 2,
        detected_at: TimestampMilliseconds(62_000),
    }
}

#[test]
fn events_route_to_the_shard_owning_their_pair() {
    let router = HashRouter::default();
    let event = trade_event();
    let shards = router.shards(&MarketInput::Event(event), 8).unwrap();

    assert_eq!(shards.len(), 1);
    assert!(shards[0].get() < 8);
}

#[test]
fn non_trade_events_route_to_the_shard_owning_their_pair() {
    let router = HashRouter::default();
    let expected = vec![ShardId::for_trading_pair(
        CexId::Binance,
        stock_trek_types::cex::trading_pair::TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        4,
    )];
    let shards = router
        .shards(&MarketInput::Event(non_trade_event()), 4)
        .unwrap();

    assert_eq!(shards, expected);
}

#[test]
fn gaps_route_to_the_shard_owning_their_stream_pair() {
    let router = HashRouter::default();
    let gap = gap();
    let expected = vec![ShardId::for_trading_pair(
        CexId::Binance,
        stock_trek_types::cex::trading_pair::TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        4,
    )];
    let shards = router.shards(&MarketInput::Gap(gap.clone()), 4).unwrap();

    assert_eq!(shards, expected);
}

#[test]
fn unresolved_gaps_are_rejected_instead_of_broadcast() {
    let router = HashRouter::default();
    let mut gap = gap();
    gap.stream = "unknown@trade".to_string();

    assert!(router.shards(&MarketInput::Gap(gap), 4).is_err());
}

#[test]
fn every_interval_for_a_pair_uses_the_same_shard() {
    let first = CandleKey::new(
        CexId::Binance,
        stock_trek_types::cex::trading_pair::TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
        AlignedWindow::try_from("1m").unwrap(),
    );
    let second = CandleKey {
        interval: AlignedWindow::try_from("5m").unwrap(),
        ..first.clone()
    };

    assert_eq!(
        ShardId::for_candle_key(&first, 16),
        ShardId::for_candle_key(&second, 16)
    );
}

#[derive(Default)]
struct FakeSource {
    records: Vec<SourceRecord>,
    acked: Arc<Mutex<usize>>,
}

#[async_trait]
impl MarketSource for FakeSource {
    async fn next_input(&mut self) -> BuildResult<Option<SourceRecord>> {
        if self.records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.records.remove(0)))
        }
    }

    async fn ack(&mut self, _record: &SourceRecord) -> BuildResult<()> {
        *self.acked.lock() += 1;
        Ok(())
    }
}

type PublishedMessages = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

#[derive(Default, Clone)]
struct FakePublisher {
    published: PublishedMessages,
}

#[async_trait]
impl EventPublisher for FakePublisher {
    async fn publish(&self, subject: &str, payload: &[u8]) -> BuildResult<()> {
        self.published
            .lock()
            .push((subject.to_string(), payload.to_vec()));
        Ok(())
    }
}

#[tokio::test]
async fn router_publishes_to_shard_subject_then_acknowledges() {
    let acked = Arc::new(Mutex::new(0));
    let source = FakeSource {
        records: vec![SourceRecord::new(
            MarketInput::Event(trade_event()),
            Default::default(),
        )],
        acked: acked.clone(),
    };
    let publisher = FakePublisher::default();
    let observed = publisher.clone();
    let config = crate::config::AggregatorConfig {
        log_filter: "info".to_string(),
        nats_url: "nats://localhost:4222".to_string(),
        event_subject_prefix: "market.events".to_string(),
        gap_subject_prefix: "market.gaps".to_string(),
        open_candle_subject_prefix: "market.candles.open".to_string(),
        closed_candle_subject_prefix: "market.candles.closed".to_string(),
        shard_subject_prefix: "aggregator.shards".to_string(),
        shard_count: 4,
        shard_index: None,
        intervals: vec![AlignedWindow::try_from("1m").unwrap()],
        allowed_lateness: std::time::Duration::from_millis(5_000),
        max_closed_candles_per_interval: std::collections::BTreeMap::from([(
            AlignedWindow::try_from("1m").unwrap(),
            usize::MAX,
        )]),
        correction_cutoff: DurationMilliseconds(i64::MAX),
        closed_candle_memory_budget_bytes: usize::MAX,
        checkpoint_interval: std::time::Duration::from_secs(5),
        checkpoint_bucket: "aggregator_shards_checkpoints".to_string(),
        checkpoint_key_prefix: "shard".to_string(),
        watermark_idle_timeout: std::time::Duration::from_secs(60),
        replay_start_policy: crate::config::ReplayStartPolicy::Checkpoint,
        state_schema_version: crate::state::STATE_SCHEMA_VERSION,
        consumer_prefix: "aggregator".to_string(),
        lease_ttl: std::time::Duration::from_secs(30),
    };

    NatsRouter::new(config, source, publisher)
        .run()
        .await
        .unwrap();

    let published = observed.published.lock();
    assert_eq!(published.len(), 1);
    assert!(published[0].0.starts_with("aggregator.shards."));
    assert_eq!(
        MarketInput::decode(&published[0].1).unwrap(),
        MarketInput::Event(trade_event())
    );
    assert_eq!(*acked.lock(), 1);
}
