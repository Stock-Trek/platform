use super::fixtures_tests::{FailingCheckpointStore, FakeSink, FakeSource, Fixtures};
use crate::checkpoint::tests::InMemoryStateStore;
use crate::runner::ProcessorRunner;
use crate::shard::ShardId;
use crate::source::{MarketInput, SourceRecord};
use crate::state::DeduplicationKey;
use platform_market_data::event::GapMarker;
use platform_nats::Message;
use platform_types::new_types::TimestampMilliseconds;
use std::sync::{Arc, Mutex};
use stock_trek_types::cex::cex_id::CexId;

#[tokio::test]
async fn runs_with_injected_source_and_sink() {
    let sink = FakeSink::default();
    let observed = sink.clone();
    let runner = ProcessorRunner::new(Fixtures::config(), FakeSource::default(), sink);

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
        records: vec![Fixtures::record(
            MarketInput::Event(Fixtures::trade_event(61_000)),
            1,
        )],
        acked: acked.clone(),
        ..FakeSource::default()
    };
    let checkpoints = Arc::new(InMemoryStateStore::new());
    let runner = ProcessorRunner::new(Fixtures::config(), source, sink)
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
async fn non_trade_events_advance_the_watermark_and_dedup_state() {
    let acked = Arc::new(Mutex::new(0));
    let source = FakeSource {
        records: vec![Fixtures::record(
            MarketInput::Event(Fixtures::non_trade_event(62_000)),
            1,
        )],
        acked: acked.clone(),
        ..FakeSource::default()
    };
    let checkpoints = Arc::new(InMemoryStateStore::new());

    ProcessorRunner::new(Fixtures::config(), source, FakeSink::default())
        .with_checkpoint_store(checkpoints.clone(), ShardId::new(0))
        .run()
        .await
        .unwrap();

    let checkpoint = checkpoints.get(ShardId::new(0)).unwrap();
    assert_eq!(checkpoint.watermark, Some(TimestampMilliseconds(62_000)));
    assert!(checkpoint.state.is_duplicate(&DeduplicationKey::new(
        CexId::Binance,
        "btcusdt@depth",
        2
    )));
    assert_eq!(checkpoint.state.open_candles_len(), 0);
    assert_eq!(*acked.lock().unwrap(), 1);
}

#[tokio::test]
async fn does_not_acknowledge_when_applying_a_trade_fails() {
    let acked = Arc::new(Mutex::new(0));
    let source = FakeSource {
        records: vec![Fixtures::record(
            MarketInput::Event(Fixtures::trade_event(61_000)),
            1,
        )],
        acked: acked.clone(),
        ..FakeSource::default()
    };
    let sink = FakeSink {
        fail: true,
        ..FakeSink::default()
    };

    assert!(
        ProcessorRunner::new(Fixtures::config(), source, sink)
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
        records: vec![Fixtures::record(
            MarketInput::Event(Fixtures::trade_event(61_000)),
            1,
        )],
        acked: acked.clone(),
        ..FakeSource::default()
    };
    let runner = ProcessorRunner::new(Fixtures::config(), source, FakeSink::default())
        .with_checkpoint_store(Arc::new(FailingCheckpointStore), ShardId::new(0));

    assert!(runner.run().await.is_err());
    assert_eq!(*acked.lock().unwrap(), 0);
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
            Fixtures::record(MarketInput::Event(Fixtures::trade_event(61_000)), 1),
            Fixtures::record(MarketInput::Gap(gap), 2),
        ],
        acked: acked.clone(),
        ..FakeSource::default()
    };

    let runner = ProcessorRunner::new(Fixtures::config(), source, sink)
        .with_checkpoint_store(Arc::new(FailingCheckpointStore), ShardId::new(0));

    assert!(runner.run().await.is_err());

    assert_eq!(observed.closed_updates(), 1);
    assert_eq!(*acked.lock().unwrap(), 0);
}

#[tokio::test]
async fn does_not_acknowledge_a_record_without_checkpoint_metadata() {
    let acked = Arc::new(Mutex::new(0));
    let source = FakeSource {
        records: vec![SourceRecord::new(
            MarketInput::Event(Fixtures::trade_event(61_000)),
            Message {
                subject: "aggregator.shards.0".to_string(),
                ..Message::default()
            },
        )],
        acked: acked.clone(),
        ..FakeSource::default()
    };

    assert!(
        ProcessorRunner::new(Fixtures::config(), source, FakeSink::default())
            .run()
            .await
            .is_err()
    );
    assert_eq!(*acked.lock().unwrap(), 0);
}
