use super::fixtures_tests::{Fixtures, VecArchive};
use crate::checkpoint::tests::InMemoryStateStore;
use crate::checkpoint::{CheckpointRecord, StateStore};
use crate::processor::{CandleProcessor, ProcessorConfig};
use crate::restore::restored_shard::RestoreSource;
use crate::shard::ShardId;
use crate::state::AggregatorState;
use platform_market_data::candle::{AlignedWindow, Candle, CandleKey};
use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};
use std::collections::BTreeMap;
use stock_trek_types::cex::cex_id::CexId;

#[tokio::test]
async fn an_oversized_checkpoint_is_pruned_to_the_current_caps() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);
    let interval = Fixtures::interval();
    let mut state = AggregatorState::new();
    for timestamp in [60_000, 120_000, 180_000] {
        let mut candle = Candle::open(&Fixtures::trade(timestamp), interval).unwrap();
        candle.close();
        let evicted = state.retain_closed_candle(candle, usize::MAX);
        assert!(evicted.is_empty());
    }
    let checkpoint = CheckpointRecord::new(shard, "aggregator-shards", 11, state);
    store.save(&checkpoint).await.unwrap();

    let mut options = Fixtures::options(shard, 1);
    options.max_closed_candles_per_interval = BTreeMap::from([(interval, 1)]);
    let restored = options
        .restore(
            &store,
            Fixtures::archive_source(VecArchive::new(Vec::new())),
        )
        .await
        .unwrap();

    assert_eq!(restored.restored_from, RestoreSource::Checkpoint);
    assert_eq!(restored.state.closed_candles_len(), 1);
    let key = CandleKey::new(CexId::Binance, Fixtures::pair(), interval);
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
    let one_minute = Fixtures::interval();
    let five_minutes = AlignedWindow::try_from("5m").unwrap();
    let mut state = AggregatorState::new();
    for (interval, timestamp) in [
        (one_minute, 60_000),
        (one_minute, 120_000),
        (five_minutes, 0),
        (five_minutes, 300_000),
    ] {
        let mut candle = Candle::open(&Fixtures::trade(timestamp), interval).unwrap();
        candle.close();
        state.retain_closed_candle(candle, usize::MAX);
    }
    let checkpoint = CheckpointRecord::new(shard, "aggregator-shards", 11, state);
    store.save(&checkpoint).await.unwrap();

    let mut options = Fixtures::options(shard, 1);
    options.intervals = vec![one_minute, five_minutes];
    options.max_closed_candles_per_interval = BTreeMap::from([(one_minute, 1), (five_minutes, 2)]);
    let restored = options
        .restore(
            &store,
            Fixtures::archive_source(VecArchive::new(Vec::new())),
        )
        .await
        .unwrap();

    assert_eq!(restored.restored_from, RestoreSource::Checkpoint);
    let pair = Fixtures::pair();
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
    let interval = Fixtures::interval();
    let shard = ShardId::new(0);
    let caps = BTreeMap::from([(interval, 1)]);
    let cutoff = DurationMilliseconds(1_000);
    let events = vec![
        Fixtures::event(1, 61_000),
        Fixtures::event(2, 121_000),
        Fixtures::event(3, 181_000),
        Fixtures::event(4, 61_500),
    ];
    let config = ProcessorConfig::new(
        vec![interval],
        DurationMilliseconds::ZERO,
        caps.clone(),
        cutoff,
    );
    let mut live = CandleProcessor::with_config(config);
    let mut live_evictions = 0;
    let mut live_out_of_contract = 0;
    let mut live_closed_ids = Vec::new();
    for event in &events {
        let update = live.apply_event(
            event,
            &[Fixtures::trade(event.exchange_timestamp.unwrap().0)],
        );
        live_evictions += update.closed_candle_evictions;
        live_out_of_contract += update.out_of_contract_trades;
        live_closed_ids.extend(update.closed.iter().map(|candle| candle.message_id()));
    }
    let live_state = live.into_state();
    assert_eq!(live_evictions, 1);
    assert_eq!(live_out_of_contract, 1);
    assert_eq!(live_closed_ids.len(), 2);

    let archive_state = AggregatorState::rebuild_from_archive(
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
    let key = CandleKey::new(CexId::Binance, Fixtures::pair(), interval);
    let retained = archive_state
        .closed_candle(&key, TimestampMilliseconds(120_000))
        .expect("the newest candle is retained");
    assert_eq!(retained.trade_count, 1);
    assert!(
        archive_state
            .closed_candle(&key, TimestampMilliseconds(60_000))
            .is_none(),
        "the cap-evicted window must not be recreated by the out-of-contract correction"
    );
}
