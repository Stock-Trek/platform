use crate::checkpoint_record_tests::InMemoryStateStore;
use crate::market_source::MarketInput;
use crate::runner::tests::fixtures_tests::{FakeSink, Fixtures, SignalingSource};
use crate::runner::{ProcessorRunner, QuiesceSignal};
use crate::shard_id::ShardId;
use crate::writer_lease::InMemoryLeaseStore;
use crate::writer_lease::LeaseStore;
use parking_lot::Mutex;
use platform_types::TimestampMilliseconds;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::watch;

#[tokio::test]
async fn quiesce_stops_new_inputs_and_drains_accepted_records() {
    let sink = FakeSink::default();
    let observed = sink.clone();
    let signal = QuiesceSignal::new();
    let polls = Arc::new(AtomicUsize::new(0));
    let acked = Arc::new(Mutex::new(0));
    let source = SignalingSource {
        records: vec![
            Fixtures::record(MarketInput::Event(Fixtures::trade_event(61_000)), 1),
            Fixtures::record(MarketInput::Event(Fixtures::trade_event(62_000)), 2),
        ],
        signal: signal.clone(),
        polls: polls.clone(),
        acked: acked.clone(),
    };
    let checkpoints = Arc::new(InMemoryStateStore::new());
    let leases = Arc::new(InMemoryLeaseStore::new());
    let runner = ProcessorRunner::new(Fixtures::config(), source, sink)
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
    assert_eq!(*acked.lock(), 1);
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
        records: vec![Fixtures::record(
            MarketInput::Event(Fixtures::trade_event(61_000)),
            1,
        )],
        signal: signal.clone(),
        polls: polls.clone(),
        acked: acked.clone(),
    };
    let runner = ProcessorRunner::new(Fixtures::config(), source, FakeSink::default())
        .with_quiesce_signal(signal);

    tokio::time::timeout(std::time::Duration::from_secs(1), runner.run())
        .await
        .expect("runner should stop immediately")
        .unwrap();

    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(*acked.lock(), 0);
}

#[tokio::test]
async fn a_watch_receiver_can_trigger_quiescence() {
    let (sender, receiver) = watch::channel(false);
    let polls = Arc::new(AtomicUsize::new(0));
    let acked = Arc::new(Mutex::new(0));
    let source = SignalingSource {
        records: vec![Fixtures::record(
            MarketInput::Event(Fixtures::trade_event(61_000)),
            1,
        )],
        signal: QuiesceSignal::new(),
        polls: polls.clone(),
        acked: acked.clone(),
    };
    let runner = ProcessorRunner::new(Fixtures::config(), source, FakeSink::default())
        .with_quiesce_signal(receiver);
    sender.send_replace(true);

    tokio::time::timeout(std::time::Duration::from_secs(1), runner.run())
        .await
        .expect("runner should stop after quiescing")
        .unwrap();

    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(*acked.lock(), 0);
}

#[tokio::test]
async fn pending_open_writes_are_drained_when_quiescing() {
    let sink = FakeSink::default();
    let observed = sink.clone();
    let signal = QuiesceSignal::new();
    let source = SignalingSource {
        records: vec![Fixtures::record(
            MarketInput::Event(Fixtures::trade_event(61_000)),
            1,
        )],
        signal: signal.clone(),
        polls: Arc::new(AtomicUsize::new(0)),
        acked: Arc::new(Mutex::new(0)),
    };
    let checkpoints = Arc::new(InMemoryStateStore::new());

    ProcessorRunner::new(Fixtures::config(), source, sink)
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
