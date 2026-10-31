use super::*;
use crate::{
    checkpoint::{CheckpointRecord, InMemoryStateStore, StateStore},
    config::AggregatorConfig,
    lease::{InMemoryLeaseStore, LeaseStore},
    processor::{CandleProcessor, ProcessorUpdate},
    shard::ShardId,
    sink::{ClosedCandlePublisher, OpenCandleStore},
    source::{MarketInput, MarketSource, SourceRecord},
    state::{AggregatorState, DeduplicationKey},
};
use async_trait::async_trait;
use platform_errors::{BpError, BpResult};
use platform_market_data::{
    candle::{Candle, CandleKey, ClosedCandle},
    event::{GapMarker, MarketEvent},
};
use platform_metrics::descriptions::{
    MARKET_DATA_AGGREGATOR_CLOSED_CANDLES_EVICTED_TOTAL,
    MARKET_DATA_AGGREGATOR_OUT_OF_CONTRACT_GAP_WINDOWS_TOTAL,
    MARKET_DATA_AGGREGATOR_OUT_OF_CONTRACT_TRADES_TOTAL,
};
use platform_metrics::test_utils::TestRecorder;
use platform_nats::Message;
use platform_types::new_types::TimestampMilliseconds;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use stock_trek_types::cex::cex_id::CexId;
use tokio::sync::watch;

#[derive(Default)]
struct FakeSource {
    records: Vec<SourceRecord>,
    fail: bool,
    acked: Arc<Mutex<usize>>,
}

#[async_trait]
impl MarketSource for FakeSource {
    async fn next_input(&mut self) -> BpResult<Option<SourceRecord>> {
        if self.fail {
            return Err(BpError::external("source failed"));
        }
        if self.records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.records.remove(0)))
        }
    }

    async fn ack(&mut self, _record: &SourceRecord) -> BpResult<()> {
        *self.acked.lock().expect("acked lock") += 1;
        Ok(())
    }
}

#[derive(Default, Clone)]
struct FakeSink {
    open_updates: Arc<Mutex<usize>>,
    closed_updates: Arc<Mutex<usize>>,
    deleted_open_updates: Arc<Mutex<usize>>,
    fail: bool,
}

impl FakeSink {
    fn open_updates(&self) -> usize {
        *self.open_updates.lock().expect("open updates lock")
    }

    fn closed_updates(&self) -> usize {
        *self.closed_updates.lock().expect("closed updates lock")
    }

    fn deleted_open_updates(&self) -> usize {
        *self
            .deleted_open_updates
            .lock()
            .expect("deleted open updates lock")
    }
}

#[async_trait]
impl OpenCandleStore for FakeSink {
    async fn update_open(&self, candles: &[Candle]) -> BpResult<()> {
        if self.fail {
            return Err(BpError::bad_value("sink", "test", "write failed"));
        }
        *self.open_updates.lock().expect("open updates lock") += candles.len();
        Ok(())
    }

    async fn delete_open(&self, keys: &[CandleKey]) -> BpResult<()> {
        if self.fail {
            return Err(BpError::bad_value("sink", "test", "write failed"));
        }
        *self
            .deleted_open_updates
            .lock()
            .expect("deleted open updates lock") += keys.len();
        Ok(())
    }
}

#[async_trait]
impl ClosedCandlePublisher for FakeSink {
    async fn publish_closed(&self, candles: &[ClosedCandle]) -> BpResult<()> {
        if self.fail {
            return Err(BpError::bad_value("sink", "test", "write failed"));
        }
        *self.closed_updates.lock().expect("closed updates lock") += candles.len();
        Ok(())
    }
}

struct FailingCheckpointStore;

#[async_trait]
impl StateStore for FailingCheckpointStore {
    async fn load(&self, _shard: ShardId) -> BpResult<Option<CheckpointRecord>> {
        Ok(None)
    }

    async fn save(&self, _checkpoint: &CheckpointRecord) -> BpResult<()> {
        Err(BpError::bad_value("checkpoint", "test", "write failed"))
    }
}

fn config() -> AggregatorConfig {
    AggregatorConfig {
        log_filter: "info".to_string(),
        nats_url: "nats://localhost:4222".to_string(),
        event_subject_prefix: "market.events".to_string(),
        gap_subject_prefix: "market.gaps".to_string(),
        open_candle_subject_prefix: "market.candles.open".to_string(),
        closed_candle_subject_prefix: "market.candles.closed".to_string(),
        shard_subject_prefix: "aggregator.shards".to_string(),
        shard_count: 1,
        shard_index: None,
        intervals: vec![CandleInterval::from_label("1m").unwrap()],
        allowed_lateness: std::time::Duration::from_millis(5_000),
        max_closed_candles_per_interval: std::collections::BTreeMap::from([(
            CandleInterval::from_label("1m").unwrap(),
            usize::MAX,
        )]),
        correction_cutoff: platform_types::new_types::DurationMilliseconds(i64::MAX),
        closed_candle_memory_budget_bytes: usize::MAX,
        checkpoint_interval: std::time::Duration::from_secs(1),
        checkpoint_bucket: "aggregator_shards_checkpoints".to_string(),
        checkpoint_key_prefix: "shard".to_string(),
        watermark_idle_timeout: std::time::Duration::from_secs(60),
        replay_start_policy: crate::config::ReplayStartPolicy::Checkpoint,
        state_schema_version: crate::state::STATE_SCHEMA_VERSION,
        consumer_prefix: "aggregator".to_string(),
        lease_ttl: std::time::Duration::from_secs(30),
    }
}

use platform_market_data::candle::CandleInterval;

fn trade_event(timestamp_ms: i64) -> MarketEvent {
    MarketEvent {
        exchange: CexId::Binance,
        stream: "btcusdt@trade".to_string(),
        sequence: Some(1),
        exchange_timestamp: Some(TimestampMilliseconds(timestamp_ms)),
        received_at: TimestampMilliseconds(timestamp_ms + 1),
        payload: "eyJlIjoidHJhZGUiLCJzIjoiQlRDVVNEVCIsInAiOiIxMDAiLCJxIjoiMSIsIlQiOjYxMDAwfQ=="
            .to_string(),
    }
}

fn record(input: MarketInput, sequence: u64) -> SourceRecord {
    SourceRecord::new(
        input,
        Message {
            subject: format!("aggregator.shards.0.{sequence}"),
            stream: Some("aggregator-shards".to_string()),
            stream_sequence: Some(sequence),
            consumer_sequence: Some(sequence),
            ..Message::default()
        },
    )
}

struct SignalingSource {
    records: Vec<SourceRecord>,
    signal: QuiesceSignal,
    polls: Arc<AtomicUsize>,
    acked: Arc<Mutex<usize>>,
}

#[async_trait]
impl MarketSource for SignalingSource {
    async fn next_input(&mut self) -> BpResult<Option<SourceRecord>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if self.records.is_empty() {
            return std::future::pending().await;
        }
        let record = self.records.remove(0);
        self.signal.quiesce();
        Ok(Some(record))
    }

    async fn ack(&mut self, _record: &SourceRecord) -> BpResult<()> {
        *self.acked.lock().expect("acked lock") += 1;
        Ok(())
    }
}

#[tokio::test]
async fn quiesce_stops_new_inputs_and_drains_accepted_records() {
    let sink = FakeSink::default();
    let observed = sink.clone();
    let signal = QuiesceSignal::new();
    let polls = Arc::new(AtomicUsize::new(0));
    let acked = Arc::new(Mutex::new(0));
    let source = SignalingSource {
        records: vec![
            record(MarketInput::Event(trade_event(61_000)), 1),
            record(MarketInput::Event(trade_event(62_000)), 2),
        ],
        signal: signal.clone(),
        polls: polls.clone(),
        acked: acked.clone(),
    };
    let checkpoints = Arc::new(InMemoryStateStore::new());
    let leases = Arc::new(InMemoryLeaseStore::new());
    let runner = ProcessorRunner::new(config(), source, sink)
        .with_checkpoint_store(checkpoints.clone(), ShardId::new(0))
        .with_lease_store(leases.clone())
        .with_writer_id("quiescing-writer")
        .with_quiesce_signal(signal);

    tokio::time::timeout(std::time::Duration::from_secs(1), runner.run())
        .await
        .expect("runner should stop after quiescing")
        .unwrap();

    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(observed.open_updates(), 1);
    assert_eq!(*acked.lock().unwrap(), 1);
    let checkpoint = checkpoints.get(ShardId::new(0)).unwrap();
    assert_eq!(checkpoint.next_sequence, 2);
    assert_eq!(checkpoint.state.last_processed_sequence, Some(1));
    let lease = leases.current(ShardId::new(0)).await.unwrap().unwrap();
    assert!(lease.is_expired(TimestampMilliseconds::now()));
}

#[tokio::test]
async fn an_already_quiesced_worker_does_not_poll_its_source() {
    let signal = QuiesceSignal::new();
    signal.quiesce();
    let polls = Arc::new(AtomicUsize::new(0));
    let acked = Arc::new(Mutex::new(0));
    let source = SignalingSource {
        records: vec![record(MarketInput::Event(trade_event(61_000)), 1)],
        signal: signal.clone(),
        polls: polls.clone(),
        acked: acked.clone(),
    };
    let runner =
        ProcessorRunner::new(config(), source, FakeSink::default()).with_quiesce_signal(signal);

    tokio::time::timeout(std::time::Duration::from_secs(1), runner.run())
        .await
        .expect("runner should stop immediately")
        .unwrap();

    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(*acked.lock().unwrap(), 0);
}

#[tokio::test]
async fn a_watch_receiver_can_trigger_quiescence() {
    let (sender, receiver) = watch::channel(false);
    let polls = Arc::new(AtomicUsize::new(0));
    let acked = Arc::new(Mutex::new(0));
    let source = SignalingSource {
        records: vec![record(MarketInput::Event(trade_event(61_000)), 1)],
        signal: QuiesceSignal::new(),
        polls: polls.clone(),
        acked: acked.clone(),
    };
    let runner =
        ProcessorRunner::new(config(), source, FakeSink::default()).with_quiesce_signal(receiver);
    sender.send_replace(true);

    tokio::time::timeout(std::time::Duration::from_secs(1), runner.run())
        .await
        .expect("runner should stop after quiescing")
        .unwrap();

    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(*acked.lock().unwrap(), 0);
}

#[tokio::test]
async fn runs_with_injected_source_and_sink() {
    let sink = FakeSink::default();
    let observed = sink.clone();
    let runner = ProcessorRunner::new(config(), FakeSource::default(), sink);

    runner.run().await.unwrap();

    assert_eq!(observed.open_updates(), 0);
    assert_eq!(observed.closed_updates(), 0);
}

#[tokio::test]
async fn checkpoints_before_acknowledging_a_trade() {
    let sink = FakeSink::default();
    let observed = sink.clone();
    let acked = Arc::new(Mutex::new(0));
    let source = FakeSource {
        records: vec![record(MarketInput::Event(trade_event(61_000)), 1)],
        acked: acked.clone(),
        ..FakeSource::default()
    };
    let checkpoints = Arc::new(InMemoryStateStore::new());
    let runner = ProcessorRunner::new(config(), source, sink)
        .with_checkpoint_store(checkpoints.clone(), ShardId::new(0));

    runner.run().await.unwrap();

    assert_eq!(observed.open_updates(), 1);
    assert_eq!(*acked.lock().unwrap(), 1);
    let checkpoint = checkpoints.get(ShardId::new(0)).unwrap();
    assert_eq!(checkpoint.next_sequence, 2);
    assert_eq!(checkpoint.resume_sequence(), 2);
    assert_eq!(checkpoint.source_stream, "aggregator-shards");
    assert_eq!(checkpoint.shard, ShardId::new(0));
    assert_eq!(checkpoint.watermark, Some(TimestampMilliseconds(61_000)));
    assert_eq!(checkpoint.state.last_processed_sequence, Some(1));
    assert_eq!(checkpoint.state.open_candles_len(), 1);
    assert_eq!(checkpoint.fencing_token, 1);
    assert!(!checkpoint.writer_id.is_empty());
}

#[tokio::test]
async fn does_not_acknowledge_when_applying_a_trade_fails() {
    let acked = Arc::new(Mutex::new(0));
    let source = FakeSource {
        records: vec![record(MarketInput::Event(trade_event(61_000)), 1)],
        acked: acked.clone(),
        ..FakeSource::default()
    };
    let sink = FakeSink {
        fail: true,
        ..FakeSink::default()
    };

    assert!(
        ProcessorRunner::new(config(), source, sink)
            .run()
            .await
            .is_err()
    );
    assert_eq!(*acked.lock().unwrap(), 0);
}

#[tokio::test]
async fn does_not_acknowledge_when_checkpointing_fails() {
    let acked = Arc::new(Mutex::new(0));
    let source = FakeSource {
        records: vec![record(MarketInput::Event(trade_event(61_000)), 1)],
        acked: acked.clone(),
        ..FakeSource::default()
    };
    let runner = ProcessorRunner::new(config(), source, FakeSink::default())
        .with_checkpoint_store(Arc::new(FailingCheckpointStore), ShardId::new(0));

    assert!(runner.run().await.is_err());
    assert_eq!(*acked.lock().unwrap(), 0);
}

#[tokio::test]
async fn gap_markers_close_expired_candle_windows() {
    let sink = FakeSink::default();
    let observed = sink.clone();
    let acked = Arc::new(Mutex::new(0));
    let gap = GapMarker {
        exchange: CexId::Binance,
        stream: "btcusdt@trade".to_string(),
        previous_sequence: 1,
        observed_sequence: 3,
        missing_from: 2,
        missing_to: 2,
        detected_at: TimestampMilliseconds(126_000),
    };
    let source = FakeSource {
        records: vec![
            record(MarketInput::Event(trade_event(61_000)), 1),
            record(MarketInput::Gap(gap), 2),
        ],
        acked: acked.clone(),
        ..FakeSource::default()
    };

    ProcessorRunner::new(config(), source, sink)
        .run()
        .await
        .unwrap();

    // The intermediate open value from the trade is coalesced away before it
    // is flushed: the gap closes that window, so the final projection mutation
    // is a tombstone and the closed candle is the durable output.
    assert_eq!(observed.open_updates(), 0);
    assert_eq!(observed.closed_updates(), 1);
    assert_eq!(observed.deleted_open_updates(), 1);
    assert_eq!(*acked.lock().unwrap(), 2);
}

#[test]
fn closed_open_keys_keep_a_newer_open_window() {
    use platform_market_data::{candle::CandleInterval, decoders::trade::Trade};
    use rust_decimal::Decimal;
    use stock_trek_types::cex::{asset_id::AssetId, trading_pair::TradingPair};

    let interval = CandleInterval::from_label("1m").unwrap();
    let pair = TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD);
    let key = CandleKey::new(CexId::Binance, pair, interval);
    let trade_at = |timestamp| Trade {
        exchange: CexId::Binance,
        trading_pair: pair,
        price: Decimal::ONE,
        quantity: Decimal::ONE,
        timestamp: TimestampMilliseconds(timestamp),
    };

    let mut closed = Candle::open(&trade_at(61_000), interval).unwrap();
    closed.close();
    let newer_open = Candle::open(&trade_at(121_000), interval).unwrap();
    let update = ProcessorUpdate {
        closed: vec![ClosedCandle::new(
            closed.clone(),
            TimestampMilliseconds(126_000),
            1,
            0,
        )],
        ..ProcessorUpdate::default()
    };

    let mut state = AggregatorState::new();
    assert_eq!(
        ProcessorRunner::<FakeSource, FakeSink>::closed_open_keys(&update, &state),
        vec![key.clone()]
    );

    state.open_candles.entry(key).or_default().push(newer_open);
    assert!(ProcessorRunner::<FakeSource, FakeSink>::closed_open_keys(&update, &state).is_empty());
}

#[tokio::test]
async fn live_processing_ignores_trades_owned_by_another_shard() {
    use crate::shard::ShardId;
    use stock_trek_types::cex::{asset_id::AssetId, trading_pair::TradingPair};

    let shard_count = 2;
    let pair = TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD);
    let owner = ShardId::for_trading_pair(CexId::Binance, pair, shard_count);
    let other = ShardId::new((owner.get() + 1) % shard_count as u32);
    let sink = FakeSink::default();
    let observed = sink.clone();
    let acked = Arc::new(Mutex::new(0));
    let source = FakeSource {
        records: vec![record(MarketInput::Event(trade_event(61_000)), 1)],
        acked: acked.clone(),
        ..FakeSource::default()
    };
    let checkpoints = Arc::new(InMemoryStateStore::new());
    let mut config = config();
    config.shard_count = shard_count;

    ProcessorRunner::new(config, source, sink)
        .with_checkpoint_store(checkpoints.clone(), other)
        .run()
        .await
        .unwrap();

    assert_eq!(observed.open_updates(), 0);
    assert_eq!(*acked.lock().unwrap(), 1);
    let checkpoint = checkpoints.get(other).unwrap();
    assert_eq!(checkpoint.watermark, None);
    assert_eq!(checkpoint.state.open_candles_len(), 0);
}

#[tokio::test]
async fn duplicate_events_are_applied_once_but_both_are_acknowledged() {
    let sink = FakeSink::default();
    let observed = sink.clone();
    let acked = Arc::new(Mutex::new(0));
    let source = FakeSource {
        records: vec![
            record(MarketInput::Event(trade_event(61_000)), 1),
            record(MarketInput::Event(trade_event(61_000)), 2),
        ],
        acked: acked.clone(),
        ..FakeSource::default()
    };

    ProcessorRunner::new(config(), source, sink)
        .run()
        .await
        .unwrap();

    assert_eq!(observed.open_updates(), 1);
    assert_eq!(*acked.lock().unwrap(), 2);
}

#[tokio::test]
async fn repeated_open_writes_for_the_same_key_are_coalesced() {
    let sink = FakeSink::default();
    let observed = sink.clone();
    let acked = Arc::new(Mutex::new(0));
    let mut second = trade_event(62_000);
    second.sequence = Some(2);
    let source = FakeSource {
        records: vec![
            record(MarketInput::Event(trade_event(61_000)), 1),
            record(MarketInput::Event(second), 2),
        ],
        acked: acked.clone(),
        ..FakeSource::default()
    };

    ProcessorRunner::new(config(), source, sink)
        .run()
        .await
        .unwrap();

    assert_eq!(observed.open_updates(), 1);
    assert_eq!(*acked.lock().unwrap(), 2);
}

#[tokio::test]
async fn pending_open_writes_are_drained_when_quiescing() {
    let sink = FakeSink::default();
    let observed = sink.clone();
    let signal = QuiesceSignal::new();
    let source = SignalingSource {
        records: vec![record(MarketInput::Event(trade_event(61_000)), 1)],
        signal: signal.clone(),
        polls: Arc::new(AtomicUsize::new(0)),
        acked: Arc::new(Mutex::new(0)),
    };
    let checkpoints = Arc::new(InMemoryStateStore::new());

    ProcessorRunner::new(config(), source, sink)
        .with_checkpoint_store(checkpoints.clone(), ShardId::new(0))
        .with_quiesce_signal(signal)
        .run()
        .await
        .unwrap();

    assert_eq!(observed.open_updates(), 1);
    assert_eq!(
        checkpoints
            .get(ShardId::new(0))
            .unwrap()
            .state
            .open_candles_len(),
        1
    );
}

#[tokio::test]
async fn a_checkpoint_failure_after_closed_publication_does_not_acknowledge() {
    let sink = FakeSink::default();
    let observed = sink.clone();
    let acked = Arc::new(Mutex::new(0));
    let gap = GapMarker {
        exchange: CexId::Binance,
        stream: "btcusdt@trade".to_string(),
        previous_sequence: 1,
        observed_sequence: 3,
        missing_from: 2,
        missing_to: 2,
        detected_at: TimestampMilliseconds(126_000),
    };
    let source = FakeSource {
        records: vec![
            record(MarketInput::Event(trade_event(61_000)), 1),
            record(MarketInput::Gap(gap), 2),
        ],
        acked: acked.clone(),
        ..FakeSource::default()
    };

    let runner = ProcessorRunner::new(config(), source, sink)
        .with_checkpoint_store(Arc::new(FailingCheckpointStore), ShardId::new(0));

    assert!(runner.run().await.is_err());

    assert_eq!(observed.closed_updates(), 1);
    assert_eq!(*acked.lock().unwrap(), 0);
}

#[tokio::test]
async fn restores_state_and_skips_inputs_already_in_the_archive() {
    let sink = FakeSink::default();
    let observed = sink.clone();
    let acked = Arc::new(Mutex::new(0));
    let source = FakeSource {
        records: vec![record(MarketInput::Event(trade_event(61_000)), 1)],
        acked: acked.clone(),
        ..FakeSource::default()
    };
    let checkpoints = Arc::new(InMemoryStateStore::new());
    let mut restored_state = AggregatorState::new();
    restored_state.advance_watermark(TimestampMilliseconds(61_000));
    restored_state.observe_sequence(&DeduplicationKey::new(CexId::Binance, "btcusdt@trade", 1));

    ProcessorRunner::new(config(), source, sink)
        .with_checkpoint_store(checkpoints.clone(), ShardId::new(0))
        .with_state(restored_state)
        .with_replay_watermark(Some(TimestampMilliseconds(61_000)))
        .run()
        .await
        .unwrap();

    assert_eq!(observed.open_updates(), 0);
    assert_eq!(*acked.lock().unwrap(), 1);
    let checkpoint = checkpoints.get(ShardId::new(0)).unwrap();
    assert_eq!(checkpoint.next_sequence, 2);
    assert_eq!(checkpoint.watermark, Some(TimestampMilliseconds(61_000)));
    assert_eq!(checkpoint.state.open_candles_len(), 0);
}

#[tokio::test]
async fn idle_flush_after_restore_does_not_fail_without_a_source_record() {
    let shard = ShardId::new(0);
    let signal = QuiesceSignal::new();
    let trigger = signal.clone();
    let source = SignalingSource {
        records: Vec::new(),
        signal: signal.clone(),
        polls: Arc::new(AtomicUsize::new(0)),
        acked: Arc::new(Mutex::new(0)),
    };
    let mut config = config();
    config.watermark_idle_timeout = std::time::Duration::from_millis(10);
    let mut restored_state = AggregatorState::new();
    restored_state.advance_watermark(TimestampMilliseconds(61_000));
    let checkpoints = Arc::new(InMemoryStateStore::new());
    let runner = ProcessorRunner::new(config, source, FakeSink::default())
        .with_checkpoint_store(checkpoints.clone(), shard)
        .with_state(restored_state)
        .with_quiesce_signal(signal);

    let handle = tokio::spawn(async move { runner.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    trigger.quiesce();
    tokio::time::timeout(std::time::Duration::from_secs(1), handle)
        .await
        .expect("idle flush should not stop the runner")
        .expect("runner task should not panic")
        .unwrap();

    assert!(checkpoints.get(shard).is_none());
}

#[tokio::test]
async fn idle_flush_after_checkpoint_restore_preserves_the_source_position() {
    let shard = ShardId::new(0);
    let signal = QuiesceSignal::new();
    let trigger = signal.clone();
    let source = SignalingSource {
        records: Vec::new(),
        signal: signal.clone(),
        polls: Arc::new(AtomicUsize::new(0)),
        acked: Arc::new(Mutex::new(0)),
    };
    let mut restored_state = AggregatorState::new();
    restored_state.advance_watermark(TimestampMilliseconds(61_000));
    let checkpoints = Arc::new(InMemoryStateStore::new());
    let restored_checkpoint =
        CheckpointRecord::new(shard, "aggregator-shards", 42, restored_state.clone());
    checkpoints.save(&restored_checkpoint).await.unwrap();
    let mut config = config();
    config.watermark_idle_timeout = std::time::Duration::from_millis(10);
    let runner = ProcessorRunner::new(config, source, FakeSink::default())
        .with_checkpoint_store(checkpoints.clone(), shard)
        .with_state(restored_state)
        .with_quiesce_signal(signal);

    let handle = tokio::spawn(async move { runner.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    trigger.quiesce();
    tokio::time::timeout(std::time::Duration::from_secs(1), handle)
        .await
        .expect("idle flush should not stop the runner")
        .expect("runner task should not panic")
        .unwrap();

    let checkpoint = checkpoints.get(shard).unwrap();
    assert_eq!(checkpoint.next_sequence, 42);
    assert_eq!(checkpoint.source_stream, "aggregator-shards");
    assert_eq!(checkpoint.watermark, Some(TimestampMilliseconds(61_000)));
}

#[tokio::test]
async fn idle_flush_after_archive_restore_does_not_reuse_a_stale_checkpoint_position() {
    let shard = ShardId::new(0);
    let signal = QuiesceSignal::new();
    let trigger = signal.clone();
    let source = SignalingSource {
        records: Vec::new(),
        signal: signal.clone(),
        polls: Arc::new(AtomicUsize::new(0)),
        acked: Arc::new(Mutex::new(0)),
    };
    let mut restored_state = AggregatorState::new();
    restored_state.advance_watermark(TimestampMilliseconds(61_000));
    let checkpoints = Arc::new(InMemoryStateStore::new());
    let stale_checkpoint =
        CheckpointRecord::new(shard, "aggregator-shards", 99, AggregatorState::new());
    checkpoints.save(&stale_checkpoint).await.unwrap();
    let mut config = config();
    config.watermark_idle_timeout = std::time::Duration::from_millis(10);
    let runner = ProcessorRunner::new(config, source, FakeSink::default())
        .with_checkpoint_store(checkpoints.clone(), shard)
        .with_state(restored_state)
        .with_replay_watermark(Some(TimestampMilliseconds(61_000)))
        .with_quiesce_signal(signal);

    let handle = tokio::spawn(async move { runner.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    trigger.quiesce();
    tokio::time::timeout(std::time::Duration::from_secs(1), handle)
        .await
        .expect("idle flush should not stop the runner")
        .expect("runner task should not panic")
        .unwrap();

    assert_eq!(checkpoints.get(shard).unwrap(), stale_checkpoint);
}

#[tokio::test]
async fn restored_shards_apply_new_backfills_older_than_the_replay_watermark() {
    let sink = FakeSink::default();
    let observed = sink.clone();
    let acked = Arc::new(Mutex::new(0));
    let mut backfill = trade_event(61_000);
    backfill.sequence = Some(2);
    let source = FakeSource {
        records: vec![record(MarketInput::Event(backfill), 2)],
        acked: acked.clone(),
        ..FakeSource::default()
    };
    let checkpoints = Arc::new(InMemoryStateStore::new());
    let mut restored_state = AggregatorState::new();
    restored_state.advance_watermark(TimestampMilliseconds(121_000));
    restored_state.observe_sequence(&DeduplicationKey::new(CexId::Binance, "btcusdt@trade", 1));

    ProcessorRunner::new(config(), source, sink)
        .with_checkpoint_store(checkpoints.clone(), ShardId::new(0))
        .with_state(restored_state)
        .with_replay_watermark(Some(TimestampMilliseconds(121_000)))
        .run()
        .await
        .unwrap();

    assert_eq!(observed.open_updates(), 1);
    assert_eq!(*acked.lock().unwrap(), 1);
}

#[tokio::test]
async fn restored_shards_apply_sequence_less_backfills_older_than_the_replay_watermark() {
    let sink = FakeSink::default();
    let observed = sink.clone();
    let acked = Arc::new(Mutex::new(0));
    let mut backfill = trade_event(61_000);
    backfill.sequence = None;
    let source = FakeSource {
        records: vec![record(MarketInput::Event(backfill), 2)],
        acked: acked.clone(),
        ..FakeSource::default()
    };
    let checkpoints = Arc::new(InMemoryStateStore::new());
    let mut restored_state = AggregatorState::new();
    restored_state.advance_watermark(TimestampMilliseconds(121_000));

    ProcessorRunner::new(config(), source, sink)
        .with_checkpoint_store(checkpoints.clone(), ShardId::new(0))
        .with_state(restored_state)
        .with_replay_watermark(Some(TimestampMilliseconds(121_000)))
        .run()
        .await
        .unwrap();

    assert_eq!(observed.open_updates(), 1);
    assert_eq!(*acked.lock().unwrap(), 1);
}

#[tokio::test]
async fn waits_for_an_active_writer_then_takes_over_the_shard() {
    let leases = Arc::new(InMemoryLeaseStore::new());
    let ttl = std::time::Duration::from_secs(30);
    let held = leases
        .acquire(ShardId::new(0), "other-writer", ttl)
        .await
        .unwrap()
        .unwrap();
    let runner = ProcessorRunner::new(config(), FakeSource::default(), FakeSink::default())
        .with_lease_store(leases.clone())
        .with_writer_id("this-writer");

    let handle = tokio::spawn(async move { runner.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!handle.is_finished());
    leases.release(&held).await.unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(2), handle)
        .await
        .expect("runner should acquire the released lease")
        .expect("runner task should not panic")
        .unwrap();

    let lease = leases.current(held.shard).await.unwrap().unwrap();
    assert_eq!(lease.owner_id, "this-writer");
    assert_eq!(lease.fencing_token, held.fencing_token + 1);
}

#[tokio::test]
async fn does_not_acknowledge_a_record_without_checkpoint_metadata() {
    let acked = Arc::new(Mutex::new(0));
    let source = FakeSource {
        records: vec![SourceRecord::new(
            MarketInput::Event(trade_event(61_000)),
            Message {
                subject: "aggregator.shards.0".to_string(),
                ..Message::default()
            },
        )],
        acked: acked.clone(),
        ..FakeSource::default()
    };

    assert!(
        ProcessorRunner::new(config(), source, FakeSink::default())
            .run()
            .await
            .is_err()
    );
    assert_eq!(*acked.lock().unwrap(), 0);
}

#[tokio::test]
async fn propagates_source_error() {
    let sink = FakeSink::default();
    let runner = ProcessorRunner::new(
        config(),
        FakeSource {
            records: Vec::new(),
            fail: true,
            ..FakeSource::default()
        },
        sink,
    );

    let error = runner.run().await.unwrap_err();

    assert_eq!(error.to_string(), "source failed");
}

#[tokio::test]
async fn count_only_retention_updates_reach_metrics_without_publishing_candles() {
    let recorder = TestRecorder::default();
    let processor = CandleProcessor::with_config(config().processor_config());
    let interval = CandleInterval::from_label("1m").unwrap();
    let update = ProcessorUpdate {
        closed_candle_evictions: 3,
        closed_candle_evictions_by_interval: std::collections::BTreeMap::from([(interval, 3)]),
        out_of_contract_trades: 2,
        out_of_contract_gap_windows: 1,
        ..ProcessorUpdate::default()
    };

    metrics::with_local_recorder(&recorder, || {
        ProcessorRunner::<FakeSource, FakeSink>::record_update_metrics(
            ShardId::new(0),
            &processor,
            &update,
        );
    });

    assert_eq!(
        recorder.counter(
            MARKET_DATA_AGGREGATOR_CLOSED_CANDLES_EVICTED_TOTAL,
            &[("shard", "0"), ("interval", "1m")],
        ),
        3
    );
    assert_eq!(
        recorder.counter(MARKET_DATA_AGGREGATOR_OUT_OF_CONTRACT_TRADES_TOTAL, &[]),
        2
    );
    assert_eq!(
        recorder.counter(
            MARKET_DATA_AGGREGATOR_OUT_OF_CONTRACT_GAP_WINDOWS_TOTAL,
            &[],
        ),
        1
    );

    let sink = FakeSink::default();
    let writer = super::output::CandleOutputWriter::start(sink.clone());
    writer.enqueue(update, processor.state()).await.unwrap();
    writer.flush().await.unwrap();
    writer.shutdown().await.unwrap();

    assert_eq!(sink.open_updates(), 0);
    assert_eq!(sink.closed_updates(), 0);
    assert_eq!(sink.deleted_open_updates(), 0);
}
