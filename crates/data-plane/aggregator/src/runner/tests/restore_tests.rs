use crate::checkpoint_record::{CheckpointRecord, StateStore};
use crate::checkpoint_record_tests::InMemoryStateStore;
use crate::market_source::MarketInput;
use crate::runner::ProcessorRunner;
use crate::runner::tests::fixtures_tests::{FakeSink, FakeSource, Fixtures, SignalingSource};
use crate::shard_id::ShardId;
use crate::state::{AggregatorState, DeduplicationKey};
use platform_types::TimestampMilliseconds;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use stock_trek_types::cex::cex_id::CexId;

#[tokio::test]
async fn restores_state_and_skips_inputs_already_in_the_archive() {
    let sink = FakeSink::default();
    let observed = sink.clone();
    let acked = Arc::new(Mutex::new(0));
    let source = FakeSource {
        records: vec![Fixtures::record(
            MarketInput::Event(Fixtures::trade_event(61_000)),
            1,
        )],
        acked: acked.clone(),
        ..FakeSource::default()
    };
    let checkpoints = Arc::new(InMemoryStateStore::new());
    let mut restored_state = AggregatorState::new();
    restored_state.advance_watermark(TimestampMilliseconds(61_000));
    restored_state.observe_sequence(&DeduplicationKey::new(CexId::Binance, "btcusdt@trade", 1));

    ProcessorRunner::new(Fixtures::config(), source, sink)
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

#[tokio::test(start_paused = true)]
async fn idle_flush_after_restore_does_not_fail_without_a_source_record() {
    let shard = ShardId::new(0);
    let signal = crate::runner::QuiesceSignal::new();
    let trigger = signal.clone();
    let polls = Arc::new(AtomicUsize::new(0));
    let source = SignalingSource {
        records: Vec::new(),
        signal: signal.clone(),
        polls: polls.clone(),
        acked: Arc::new(Mutex::new(0)),
    };
    let mut config = Fixtures::config();
    config.watermark_idle_timeout = std::time::Duration::from_millis(10);
    let mut restored_state = AggregatorState::new();
    restored_state.advance_watermark(TimestampMilliseconds(61_000));
    let checkpoints = Arc::new(InMemoryStateStore::new());
    let runner = ProcessorRunner::new(config, source, FakeSink::default())
        .with_checkpoint_store(checkpoints.clone(), shard)
        .with_state(restored_state)
        .with_quiesce_signal(signal);

    let handle = tokio::spawn(async move { runner.run().await });
    Fixtures::wait_for_polls(&polls, 1).await;
    tokio::time::advance(std::time::Duration::from_millis(10)).await;
    Fixtures::wait_for_polls(&polls, 2).await;
    trigger.quiesce();
    tokio::time::timeout(std::time::Duration::from_secs(1), handle)
        .await
        .expect("idle flush should not stop the runner")
        .expect("runner task should not panic")
        .unwrap();

    assert!(checkpoints.get(shard).is_none());
}

#[tokio::test(start_paused = true)]
async fn idle_flush_after_checkpoint_restore_preserves_the_source_position() {
    let shard = ShardId::new(0);
    let signal = crate::runner::QuiesceSignal::new();
    let trigger = signal.clone();
    let polls = Arc::new(AtomicUsize::new(0));
    let source = SignalingSource {
        records: Vec::new(),
        signal: signal.clone(),
        polls: polls.clone(),
        acked: Arc::new(Mutex::new(0)),
    };
    let mut restored_state = AggregatorState::new();
    restored_state.advance_watermark(TimestampMilliseconds(61_000));
    let checkpoints = Arc::new(InMemoryStateStore::new());
    let restored_checkpoint =
        CheckpointRecord::new(shard, "aggregator-shards", 42, restored_state.clone());
    checkpoints.save(&restored_checkpoint).await.unwrap();
    let mut config = Fixtures::config();
    config.watermark_idle_timeout = std::time::Duration::from_millis(10);
    let runner = ProcessorRunner::new(config, source, FakeSink::default())
        .with_checkpoint_store(checkpoints.clone(), shard)
        .with_state(restored_state)
        .with_quiesce_signal(signal);

    let handle = tokio::spawn(async move { runner.run().await });
    Fixtures::wait_for_polls(&polls, 1).await;
    tokio::time::advance(std::time::Duration::from_millis(10)).await;
    Fixtures::wait_for_polls(&polls, 2).await;
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

#[tokio::test(start_paused = true)]
async fn idle_flush_after_archive_restore_does_not_reuse_a_stale_checkpoint_position() {
    let shard = ShardId::new(0);
    let signal = crate::runner::QuiesceSignal::new();
    let trigger = signal.clone();
    let polls = Arc::new(AtomicUsize::new(0));
    let source = SignalingSource {
        records: Vec::new(),
        signal: signal.clone(),
        polls: polls.clone(),
        acked: Arc::new(Mutex::new(0)),
    };
    let mut restored_state = AggregatorState::new();
    restored_state.advance_watermark(TimestampMilliseconds(61_000));
    let checkpoints = Arc::new(InMemoryStateStore::new());
    let stale_checkpoint =
        CheckpointRecord::new(shard, "aggregator-shards", 99, AggregatorState::new());
    checkpoints.save(&stale_checkpoint).await.unwrap();
    let mut config = Fixtures::config();
    config.watermark_idle_timeout = std::time::Duration::from_millis(10);
    let runner = ProcessorRunner::new(config, source, FakeSink::default())
        .with_checkpoint_store(checkpoints.clone(), shard)
        .with_state(restored_state)
        .with_replay_watermark(Some(TimestampMilliseconds(61_000)))
        .with_quiesce_signal(signal);

    let handle = tokio::spawn(async move { runner.run().await });
    Fixtures::wait_for_polls(&polls, 1).await;
    tokio::time::advance(std::time::Duration::from_millis(10)).await;
    Fixtures::wait_for_polls(&polls, 2).await;
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
    let mut backfill = Fixtures::trade_event(61_000);
    backfill.sequence = Some(2);
    let source = FakeSource {
        records: vec![Fixtures::record(MarketInput::Event(backfill), 2)],
        acked: acked.clone(),
        ..FakeSource::default()
    };
    let checkpoints = Arc::new(InMemoryStateStore::new());
    let mut restored_state = AggregatorState::new();
    restored_state.advance_watermark(TimestampMilliseconds(121_000));
    restored_state.observe_sequence(&DeduplicationKey::new(CexId::Binance, "btcusdt@trade", 1));

    ProcessorRunner::new(Fixtures::config(), source, sink)
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
    let mut backfill = Fixtures::trade_event(61_000);
    backfill.sequence = None;
    let source = FakeSource {
        records: vec![Fixtures::record(MarketInput::Event(backfill), 2)],
        acked: acked.clone(),
        ..FakeSource::default()
    };
    let checkpoints = Arc::new(InMemoryStateStore::new());
    let mut restored_state = AggregatorState::new();
    restored_state.advance_watermark(TimestampMilliseconds(121_000));

    ProcessorRunner::new(Fixtures::config(), source, sink)
        .with_checkpoint_store(checkpoints.clone(), ShardId::new(0))
        .with_state(restored_state)
        .with_replay_watermark(Some(TimestampMilliseconds(121_000)))
        .run()
        .await
        .unwrap();

    assert_eq!(observed.open_updates(), 1);
    assert_eq!(*acked.lock().unwrap(), 1);
}
