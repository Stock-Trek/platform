use super::fixtures_tests::{FakeSink, FakeSource, Fixtures};
use crate::checkpoint_record_tests::InMemoryStateStore;
use crate::market_source::MarketInput;
use crate::runner::ProcessorRunner;
use crate::shard_id::ShardId;
use platform_market_data::event::GapMarker;
use platform_types::new_types::TimestampMilliseconds;
use std::sync::{Arc, Mutex};
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

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
            Fixtures::record(MarketInput::Event(Fixtures::trade_event(61_000)), 1),
            Fixtures::record(MarketInput::Gap(gap), 2),
        ],
        acked: acked.clone(),
        ..FakeSource::default()
    };

    ProcessorRunner::new(Fixtures::config(), source, sink)
        .run()
        .await
        .unwrap();

    assert_eq!(observed.open_updates(), 0);
    assert_eq!(observed.closed_updates(), 1);
    assert_eq!(observed.deleted_open_updates(), 1);
    assert_eq!(*acked.lock().unwrap(), 2);
}

#[tokio::test]
async fn live_processing_ignores_trades_owned_by_another_shard() {
    let shard_count = 2;
    let pair = TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD);
    let owner = ShardId::for_trading_pair(CexId::Binance, pair, shard_count);
    let other = ShardId::new((owner.get() + 1) % shard_count as u32);
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
    let mut config = Fixtures::config();
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
            Fixtures::record(MarketInput::Event(Fixtures::trade_event(61_000)), 1),
            Fixtures::record(MarketInput::Event(Fixtures::trade_event(61_000)), 2),
        ],
        acked: acked.clone(),
        ..FakeSource::default()
    };

    ProcessorRunner::new(Fixtures::config(), source, sink)
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
    let mut second = Fixtures::trade_event(62_000);
    second.sequence = Some(2);
    let source = FakeSource {
        records: vec![
            Fixtures::record(MarketInput::Event(Fixtures::trade_event(61_000)), 1),
            Fixtures::record(MarketInput::Event(second), 2),
        ],
        acked: acked.clone(),
        ..FakeSource::default()
    };

    ProcessorRunner::new(Fixtures::config(), source, sink)
        .run()
        .await
        .unwrap();

    assert_eq!(observed.open_updates(), 1);
    assert_eq!(*acked.lock().unwrap(), 2);
}

#[tokio::test]
async fn propagates_source_error() {
    let sink = FakeSink::default();
    let runner = ProcessorRunner::new(
        Fixtures::config(),
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
