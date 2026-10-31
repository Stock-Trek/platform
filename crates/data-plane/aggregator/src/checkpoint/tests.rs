use async_trait::async_trait;
use crate::{
    checkpoint::{CheckpointRecord, NatsStateStore, StateStore},
    lease::WriterLease,
    shard::ShardId,
    state::{AggregatorState, STATE_SCHEMA_VERSION},
};
use platform_errors::{BpError, BpResult};
use platform_nats::Message;
use platform_types::new_types::TimestampMilliseconds;
use parking_lot::Mutex;
use std::collections::HashMap;

/// In-memory state store used by tests.
#[derive(Debug, Default)]
pub struct InMemoryStateStore {
    checkpoints: Mutex<HashMap<ShardId, (u64, CheckpointRecord)>>,
}

impl InMemoryStateStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, shard: ShardId) -> Option<CheckpointRecord> {
        self.load_with_revision(shard)
            .map(|(checkpoint, _)| checkpoint)
    }

    pub fn load_with_revision(&self, shard: ShardId) -> Option<(CheckpointRecord, u64)> {
        self.checkpoints
            .lock()
            .get(&shard)
            .cloned()
            .map(|(revision, checkpoint)| (checkpoint, revision))
    }
}

#[async_trait]
impl StateStore for InMemoryStateStore {
    async fn load(&self, shard: ShardId) -> BpResult<Option<CheckpointRecord>> {
        Ok(self.get(shard))
    }

    async fn save(&self, checkpoint: &CheckpointRecord) -> BpResult<()> {
        let mut checkpoints = self.checkpoints.lock();
        if let Some((_, existing)) = checkpoints.get(&checkpoint.shard)
            && !existing.is_writable_by(&checkpoint.writer_id, checkpoint.fencing_token)
        {
            return Err(BpError::bad_value(
                "checkpoint fencing token",
                checkpoint.fencing_token.to_string(),
                format!(
                    "writer {} is fenced by token {}",
                    checkpoint.writer_id, existing.fencing_token
                ),
            ));
        }
        let next_revision = checkpoints
            .get(&checkpoint.shard)
            .map(|(revision, _)| revision.saturating_add(1))
            .unwrap_or(1);
        checkpoints.insert(checkpoint.shard, (next_revision, checkpoint.clone()));
        Ok(())
    }
}


fn message() -> Message {
    Message {
        subject: "aggregator.shards.3".to_string(),
        stream: Some("aggregator-shards".to_string()),
        stream_sequence: Some(41),
        consumer_sequence: Some(9),
        ..Message::default()
    }
}

fn checkpoint() -> CheckpointRecord {
    let mut state = AggregatorState::new();
    state.mark_processed(41);
    state.advance_watermark(TimestampMilliseconds(1_000));
    CheckpointRecord::from_message(&message(), ShardId::new(2), state)
        .unwrap()
        .unwrap()
}

#[test]
fn checkpoint_resumes_after_the_last_processed_message() {
    let checkpoint = checkpoint();

    assert_eq!(checkpoint.source_stream, "aggregator-shards");
    assert_eq!(checkpoint.next_sequence, 42);
    assert_eq!(checkpoint.last_sequence(), 41);
    assert_eq!(checkpoint.resume_sequence(), 42);
    assert_eq!(checkpoint.shard, ShardId::new(2));
    assert_eq!(checkpoint.watermark, Some(TimestampMilliseconds(1_000)));
    assert_eq!(checkpoint.state.schema_version, STATE_SCHEMA_VERSION);
    assert!(checkpoint.is_compatible(ShardId::new(2), "aggregator-shards"));
}

#[test]
fn incompatible_checkpoints_are_not_used_for_recovery() {
    let checkpoint = checkpoint();

    assert!(!checkpoint.is_compatible(ShardId::new(3), "aggregator-shards"));
    assert!(!checkpoint.is_compatible(ShardId::new(2), "another-stream"));

    let mut invalid_sequence = checkpoint;
    invalid_sequence.next_sequence = 0;
    assert!(!invalid_sequence.is_compatible(ShardId::new(2), "aggregator-shards"));
}

#[test]
fn messages_without_jetstream_metadata_do_not_produce_checkpoints() {
    let message = Message {
        subject: "test".to_string(),
        ..Message::default()
    };

    assert_eq!(
        CheckpointRecord::from_message(&message, ShardId::new(0), AggregatorState::new()).unwrap(),
        None
    );
}

#[tokio::test]
async fn in_memory_store_round_trips_checkpoints() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(2);
    let checkpoint = checkpoint();

    store.save(&checkpoint).await.unwrap();

    let loaded = store.load(shard).await.unwrap().unwrap();
    assert_eq!(loaded.next_sequence, 42);
    assert_eq!(loaded.watermark, checkpoint.watermark);
    assert_eq!(store.load(ShardId::new(3)).await.unwrap(), None);
    assert_eq!(store.load_with_revision(shard).unwrap().1, 1);
}

#[tokio::test]
async fn in_memory_store_overwrites_are_atomic() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(1);
    let mut checkpoint = checkpoint();
    checkpoint.shard = shard;

    store.save(&checkpoint).await.unwrap();
    checkpoint.next_sequence = 50;
    store.save(&checkpoint).await.unwrap();

    assert_eq!(store.load(shard).await.unwrap().unwrap().next_sequence, 50);
    assert_eq!(store.load_with_revision(shard).unwrap().1, 2);
}

#[tokio::test]
async fn an_older_fencing_token_cannot_overwrite_a_newer_checkpoint() {
    let store = InMemoryStateStore::new();
    let shard = ShardId::new(1);
    let lease = WriterLease {
        shard,
        owner_id: "writer-b".to_string(),
        fencing_token: 2,
        expires_at: TimestampMilliseconds::now(),
    };
    let mut checkpoint = checkpoint();
    checkpoint.shard = shard;
    store
        .save(&checkpoint.clone().with_lease(&lease))
        .await
        .unwrap();

    let stale_lease = WriterLease {
        shard,
        owner_id: "writer-a".to_string(),
        fencing_token: 1,
        expires_at: TimestampMilliseconds::now(),
    };
    let stale = checkpoint.with_lease(&stale_lease);

    assert!(store.save(&stale).await.is_err());
    assert_eq!(
        store.load(shard).await.unwrap().unwrap().writer_id,
        "writer-b"
    );
}

#[test]
fn checkpoint_bucket_is_a_valid_kv_name() {
    let bucket = NatsStateStore::bucket_name("aggregator.shards");
    assert_eq!(bucket, "aggregator_shards_checkpoints");
}
