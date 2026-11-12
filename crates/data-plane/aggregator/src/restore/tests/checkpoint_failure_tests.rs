use crate::checkpoint_record::{CheckpointRecord, StateStore};
use crate::checkpoint_record_tests::InMemoryStateStore;
use crate::restore::restored_shard::RestoreSource;
use crate::restore::tests::fixtures_tests::{FailingStateStore, Fixtures, VecArchive};
use crate::shard_id::ShardId;
use crate::state::{AggregatorState, STATE_SCHEMA_VERSION};
use platform_archive::ICEBERG_CATALOG_URI;
use platform_errors::{BuildError, ConfigError, Retryability};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn configured_schema_mismatch_forces_archive_rebuild() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);
    let mut state = AggregatorState::new();
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

    let mut options = Fixtures::options(shard, 1);
    options.expected_schema_version = STATE_SCHEMA_VERSION + 1;
    let restored = options
        .restore(
            &store,
            Fixtures::archive_source(VecArchive::new(vec![Fixtures::event(1, 61_000)])),
        )
        .await
        .unwrap();

    assert_eq!(restored.restored_from, RestoreSource::Archive);
    assert!(restored.rebuilt_from_archive);
    assert_eq!(restored.state.schema_version, STATE_SCHEMA_VERSION + 1);
}

#[tokio::test]
async fn transient_checkpoint_read_failures_are_returned_instead_of_falling_back() {
    let store = FailingStateStore {
        retryability: Retryability::Retryable,
    };
    let shard = ShardId::new(0);
    let connects = Arc::new(AtomicUsize::new(0));

    let error = Fixtures::options(shard, 1)
        .restore(
            &store,
            Fixtures::counting_archive(vec![Fixtures::event(1, 61_000)], connects.clone()),
        )
        .await
        .unwrap_err();

    assert!(error.retryability().is_retryable());
    assert_eq!(connects.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn permanent_checkpoint_read_failures_still_rebuild_from_the_archive() {
    let store = FailingStateStore {
        retryability: Retryability::Permanent,
    };
    let shard = ShardId::new(0);
    let connects = Arc::new(AtomicUsize::new(0));

    let restored = Fixtures::options(shard, 1)
        .restore(
            &store,
            Fixtures::counting_archive(vec![Fixtures::event(1, 61_000)], connects.clone()),
        )
        .await
        .unwrap();

    assert_eq!(restored.restored_from, RestoreSource::Archive);
    assert!(restored.rebuilt_from_archive);
    assert_eq!(connects.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_failing_archive_connection_fails_fast_during_checkpoint_restore() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(0);

    let error = Fixtures::options(shard, 1)
        .restore(&store, || async {
            Err(BuildError::config_missing(ICEBERG_CATALOG_URI))
        })
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BuildError::Config(ConfigError::Missing { name })
            if name == ICEBERG_CATALOG_URI
    ));
}
