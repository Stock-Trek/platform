use crate::checkpoint_record::{CheckpointRecord, StateStore};
use crate::checkpoint_record_tests::InMemoryStateStore;
use crate::config::ReplayStartPolicy;
use crate::restore::restored_shard::RestoreSource;
use crate::restore::tests::fixtures_tests::{Fixtures, VecArchive};
use crate::shard_id::ShardId;
use crate::state::STATE_SCHEMA_VERSION;
use platform_nats::ConsumerStart;
use platform_types::TimestampMilliseconds;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn a_compatible_checkpoint_is_restored_without_replaying_the_archive() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);
    let mut state = crate::state::AggregatorState::new();
    state.mark_processed(10);
    state.advance_watermark(TimestampMilliseconds(61_000));
    let checkpoint = CheckpointRecord::new(shard, "aggregator-shards", 11, state);
    store.save(&checkpoint).await.unwrap();

    let connects = Arc::new(AtomicUsize::new(0));
    let restored = Fixtures::options(shard, 1)
        .restore(
            &store,
            Fixtures::counting_archive(vec![Fixtures::event(1, 1_000)], connects.clone()),
        )
        .await
        .unwrap();

    assert_eq!(restored.start_sequence(), Some(11));
    assert_eq!(restored.start, ConsumerStart::Sequence(11));
    assert!(!restored.rebuilt_from_archive);
    assert_eq!(
        restored.state.watermark,
        Some(TimestampMilliseconds(61_000))
    );
    assert_eq!(connects.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn archive_policy_ignores_a_compatible_checkpoint() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);
    let mut state = crate::state::AggregatorState::new();
    state.mark_processed(10);
    store
        .save(&CheckpointRecord::new(
            shard,
            "aggregator-shards",
            11,
            state,
        ))
        .await
        .unwrap();

    let connects = Arc::new(AtomicUsize::new(0));
    let mut options = Fixtures::options(shard, 1);
    options.replay_start_policy = ReplayStartPolicy::Archive;
    let restored = options
        .restore(
            &store,
            Fixtures::counting_archive(vec![Fixtures::event(1, 61_000)], connects.clone()),
        )
        .await
        .unwrap();

    assert_eq!(restored.restored_from, RestoreSource::Archive);
    assert!(restored.rebuilt_from_archive);
    assert_eq!(restored.start, ConsumerStart::Earliest);
    assert_eq!(restored.state.open_candles_len(), 1);
    assert_eq!(connects.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn earliest_policy_skips_checkpoint_and_archive() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);
    let connects = Arc::new(AtomicUsize::new(0));
    let mut options = Fixtures::options(shard, 1);
    options.replay_start_policy = ReplayStartPolicy::Earliest;

    let restored = options
        .restore(
            &store,
            Fixtures::counting_archive(vec![Fixtures::event(1, 61_000)], connects.clone()),
        )
        .await
        .unwrap();

    assert_eq!(restored.restored_from, RestoreSource::Empty);
    assert!(!restored.rebuilt_from_archive);
    assert_eq!(restored.start, ConsumerStart::Earliest);
    assert_eq!(restored.state.schema_version, STATE_SCHEMA_VERSION);
    assert_eq!(restored.state.open_candles_len(), 0);
    assert_eq!(connects.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn empty_state_uses_the_configured_schema_version() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);
    let mut options = Fixtures::options(shard, 1);
    options.replay_start_policy = ReplayStartPolicy::Earliest;
    options.expected_schema_version = STATE_SCHEMA_VERSION + 2;

    let restored = options
        .restore(
            &store,
            Fixtures::archive_source(VecArchive::new(vec![Fixtures::event(1, 61_000)])),
        )
        .await
        .unwrap();

    assert_eq!(restored.restored_from, RestoreSource::Empty);
    assert_eq!(restored.state.schema_version, STATE_SCHEMA_VERSION + 2);
}

#[tokio::test]
async fn latest_policy_starts_at_the_end_of_the_live_source() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);
    let mut options = Fixtures::options(shard, 1);
    options.replay_start_policy = ReplayStartPolicy::Latest;

    let restored = options
        .restore(
            &store,
            Fixtures::archive_source(VecArchive::new(vec![Fixtures::event(1, 61_000)])),
        )
        .await
        .unwrap();

    assert_eq!(restored.restored_from, RestoreSource::Empty);
    assert_eq!(restored.start, ConsumerStart::Latest);
    assert_eq!(restored.state.open_candles_len(), 0);
}

#[tokio::test]
async fn an_empty_archive_starts_with_explicit_empty_state() {
    for policy in [ReplayStartPolicy::Checkpoint, ReplayStartPolicy::Archive] {
        let store = InMemoryStateStore::new();
        let shard = ShardId::new(0);
        let mut options = Fixtures::options(shard, 1);
        options.replay_start_policy = policy;

        let restored = options
            .restore(
                &store,
                Fixtures::archive_source(VecArchive::new(Vec::new())),
            )
            .await
            .unwrap();

        assert_eq!(restored.restored_from, RestoreSource::Empty);
        assert!(!restored.rebuilt_from_archive);
        assert_eq!(restored.start, ConsumerStart::Earliest);
        assert_eq!(restored.state.schema_version, STATE_SCHEMA_VERSION);
        assert_eq!(restored.state.open_candles_len(), 0);
    }
}
