use crate::checkpoint_record::{CheckpointRecord, StateStore};
use crate::checkpoint_record_tests::InMemoryStateStore;
use crate::restore::tests::fixtures_tests::{Fixtures, VecArchive};
use crate::shard_id::ShardId;
use crate::state::{AggregatorState, DeduplicationKey, STATE_SCHEMA_VERSION};
use platform_market_data::decoders::trade::TradeDecoderFactory;
use platform_nats::ConsumerStart;
use platform_types::new_types::TimestampMilliseconds;
use rust_decimal::Decimal;
use stock_trek_types::cex::cex_id::CexId;

#[tokio::test]
async fn a_missing_checkpoint_rebuilds_from_archived_events() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);
    let restored = Fixtures::options(shard, 1)
        .restore(
            &store,
            Fixtures::archive_source(VecArchive::new(vec![
                Fixtures::event(1, 61_000),
                Fixtures::event(2, 62_000),
            ])),
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
    assert_eq!(candle.ohlcv.open(), 100.0);
    assert_eq!(candle.ohlcv.volume(), 2.0);
    assert_eq!(candle.trade_count, 2);
}

#[tokio::test]
async fn a_rebuild_continues_the_latest_archived_state_epoch_without_advancing_it() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);
    let archive = VecArchive::with_epochs(vec![
        (Fixtures::event(1, 61_000), 7),
        (Fixtures::event(2, 62_000), 3),
    ]);

    let restored = Fixtures::options(shard, 1)
        .restore(&store, Fixtures::archive_source(archive))
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

    let restored = Fixtures::options(shard, 1)
        .restore(
            &store,
            Fixtures::archive_source(VecArchive::new(vec![Fixtures::event(1, 61_000)])),
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
    let bitcoin_shard = ShardId::for_trading_pair(CexId::Binance, Fixtures::pair(), shard_count);
    let other_shard = ShardId::new((bitcoin_shard.get() + 1) % 2);
    let restored = Fixtures::options(other_shard, shard_count)
        .restore(
            &store,
            Fixtures::archive_source(VecArchive::new(vec![Fixtures::event(1, 61_000)])),
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
    let bitcoin_shard = ShardId::for_trading_pair(CexId::Binance, Fixtures::pair(), shard_count);
    let other_shard = ShardId::new((bitcoin_shard.get() + 1) % 2);
    let restored = Fixtures::options(other_shard, shard_count)
        .restore(
            &store,
            Fixtures::archive_source(VecArchive::new(vec![Fixtures::event(1, 61_000)])),
        )
        .await
        .unwrap();

    assert_eq!(restored.state.watermark, None);
    assert_eq!(restored.replay_watermark(), None);
}

#[tokio::test]
async fn rebuild_advances_watermark_and_dedup_for_non_trade_events() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);
    let restored = Fixtures::options(shard, 1)
        .restore(
            &store,
            Fixtures::archive_source(VecArchive::new(vec![Fixtures::non_trade_event(1, 61_000)])),
        )
        .await
        .unwrap();

    assert!(restored.rebuilt_from_archive);
    assert_eq!(
        restored.state.watermark,
        Some(TimestampMilliseconds(61_000))
    );
    assert!(restored.state.is_duplicate(&DeduplicationKey::new(
        CexId::Binance,
        "btcusdt@depth",
        1
    )));
    assert_eq!(restored.state.open_candles_len(), 0);
}

#[tokio::test]
async fn rebuild_advances_watermark_and_dedup_for_undecodable_events() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);
    let restored = Fixtures::options(shard, 1)
        .restore(
            &store,
            Fixtures::archive_source(VecArchive::new(vec![Fixtures::undecodable_event(
                1, 61_000,
            )])),
        )
        .await
        .unwrap();

    assert!(restored.rebuilt_from_archive);
    assert_eq!(
        restored.state.watermark,
        Some(TimestampMilliseconds(61_000))
    );
    assert_eq!(
        restored.replay_watermark(),
        Some(TimestampMilliseconds(61_000))
    );
    assert!(restored.state.is_duplicate(&DeduplicationKey::new(
        CexId::Binance,
        "btcusdt@trade",
        1
    )));
    assert_eq!(restored.state.open_candles_len(), 0);
}

#[test]
fn trade_type_still_has_the_expected_shape_for_rebuild_tests() {
    let event = Fixtures::event(7, 61_000);
    let decoder = TradeDecoderFactory::for_exchange(event.exchange).unwrap();
    let trades: Vec<_> = decoder.decode(&event).unwrap();
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].price, Decimal::from_str_exact("100").unwrap());
}
