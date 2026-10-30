//! Durable checkpoints for aggregator shards.
//!
//! A checkpoint contains the source position *and* the serialized keyed state
//! whose effects are included up to that position. The source message is
//! acknowledged only after this record has been written, so a crash can replay
//! work but cannot silently skip it.

use crate::config::AggregatorConfig;
use crate::lease::{WriterLease, lease_key};
use crate::shard::ShardId;
use crate::state::AggregatorState;
use async_trait::async_trait;
#[cfg(test)]
use parking_lot::Mutex;
use platform_errors::{BpError, BpResult};
use platform_nats::{JetStream, KeyValue, Message};
use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::collections::HashMap;

/// A complete checkpoint for one aggregator shard.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CheckpointRecord {
    /// JetStream stream that delivered the source message.
    pub source_stream: String,
    /// First source sequence that must be delivered on resume.
    pub next_sequence: u64,
    /// Shard that owns this state.
    pub shard: ShardId,
    /// Event-time watermark captured with the state.
    pub watermark: Option<TimestampMilliseconds>,
    /// Fencing token of the writer that produced this checkpoint.
    pub fencing_token: u64,
    /// Identity of the writer that produced this checkpoint.
    pub writer_id: String,
    /// Serialized keyed aggregation state.
    pub state: AggregatorState,
    /// Wall-clock time at which the checkpoint was written.
    #[serde(default)]
    pub created_at: TimestampMilliseconds,
}

impl CheckpointRecord {
    pub fn new(
        shard: ShardId,
        source_stream: impl Into<String>,
        next_sequence: u64,
        state: AggregatorState,
    ) -> Self {
        let watermark = state.watermark;
        Self {
            source_stream: source_stream.into(),
            next_sequence,
            shard,
            watermark,
            fencing_token: 0,
            writer_id: String::new(),
            state,
            created_at: TimestampMilliseconds::now(),
        }
    }

    /// Binds the checkpoint to the lease that is allowed to write it.
    pub fn with_lease(mut self, lease: &WriterLease) -> Self {
        self.fencing_token = lease.fencing_token;
        self.writer_id = lease.owner_id.clone();
        self
    }

    /// Builds a checkpoint from a source message and the state after applying
    /// that message.
    ///
    /// Returns `None` for messages that did not come from JetStream (for
    /// example messages created by an in-memory test source), because there is
    /// no source position to persist.
    pub fn from_message(
        message: &Message,
        shard: ShardId,
        state: AggregatorState,
    ) -> BpResult<Option<Self>> {
        let (Some(stream), Some(stream_sequence)) = (&message.stream, message.stream_sequence)
        else {
            return Ok(None);
        };
        if stream.is_empty() {
            return Err(BpError::bad_value(
                "checkpoint",
                "stream",
                "stream name must not be empty",
            ));
        }
        Ok(Some(Self::new(
            shard,
            stream.clone(),
            stream_sequence.saturating_add(1),
            state,
        )))
    }

    /// The first stream sequence that must be delivered on resume.
    pub fn resume_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Whether this record can be used to restore the given shard.
    pub fn is_compatible(&self, shard: ShardId, source_stream: &str) -> bool {
        self.is_compatible_with_schema(shard, source_stream, crate::state::STATE_SCHEMA_VERSION)
    }

    /// Whether this record matches the shard, stream, position, and state
    /// schema version expected by the reader.
    pub fn is_compatible_with_schema(
        &self,
        shard: ShardId,
        source_stream: &str,
        expected_schema_version: u32,
    ) -> bool {
        self.shard == shard
            && self.source_stream == source_stream
            && self.next_sequence > 0
            && self
                .state
                .is_schema_compatible_with(expected_schema_version)
    }

    /// Age of this checkpoint relative to `now`.
    pub fn age(&self, now: TimestampMilliseconds) -> DurationMilliseconds {
        now - self.created_at
    }

    /// Whether a writer with `writer_id` and `fencing_token` may overwrite
    /// this checkpoint. Tokens only move forward, so a stalled writer cannot
    /// commit after a newer writer took the lease.
    pub fn is_writable_by(&self, writer_id: &str, fencing_token: u64) -> bool {
        fencing_token > self.fencing_token
            || (fencing_token == self.fencing_token && writer_id == self.writer_id)
    }

    /// Last source sequence included in this checkpoint.
    pub fn last_sequence(&self) -> u64 {
        self.next_sequence.saturating_sub(1)
    }
}

/// Durable storage for complete shard checkpoints.
#[async_trait]
pub trait StateStore: Send + Sync {
    async fn load(&self, shard: ShardId) -> BpResult<Option<CheckpointRecord>>;

    async fn save(&self, checkpoint: &CheckpointRecord) -> BpResult<()>;
}

/// In-memory state store used by tests.
#[cfg(test)]
#[derive(Debug, Default)]
pub struct InMemoryStateStore {
    checkpoints: Mutex<HashMap<ShardId, (u64, CheckpointRecord)>>,
}

#[cfg(test)]
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

#[cfg(test)]
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

/// Checkpoints stored in a NATS KV bucket owned by the aggregator.
pub struct NatsStateStore {
    values: KeyValue,
    key_prefix: String,
}

impl NatsStateStore {
    pub async fn connect(config: &AggregatorConfig) -> BpResult<Self> {
        let bucket = config.checkpoint_bucket();
        let values =
            KeyValue::connect(&config.nats_url, "platform-aggregator-checkpoints", &bucket).await?;
        Ok(Self {
            values,
            key_prefix: config.checkpoint_key_prefix.clone(),
        })
    }

    pub fn bucket(&self) -> &str {
        self.values.bucket()
    }

    fn key(&self, shard: ShardId) -> String {
        format!("{}-{}", self.key_prefix, shard.get())
    }

    /// Loads a checkpoint and the KV revision it was read from.
    pub async fn load_with_revision(
        &self,
        shard: ShardId,
    ) -> BpResult<Option<(CheckpointRecord, u64)>> {
        let Some((payload, revision)) = self.values.get_with_revision(&self.key(shard)).await?
        else {
            return Ok(None);
        };
        let checkpoint = serde_json::from_slice(&payload)?;
        Ok(Some((checkpoint, revision)))
    }

    /// Atomically writes a checkpoint if the current revision matches.
    ///
    /// Passing `None` as `expected_revision` creates the key. A concurrent
    /// writer causes a revision conflict instead of silently overwriting state.
    pub async fn compare_and_swap(
        &self,
        checkpoint: &CheckpointRecord,
        expected_revision: Option<u64>,
    ) -> BpResult<u64> {
        let payload = serde_json::to_vec(checkpoint)?;
        let key = self.key(checkpoint.shard);
        match expected_revision {
            Some(revision) => self.values.update(&key, &payload, revision).await,
            None => self.values.create(&key, &payload).await,
        }
    }

    async fn ensure_lease_holder(&self, checkpoint: &CheckpointRecord) -> BpResult<()> {
        let Some((payload, _)) = self
            .values
            .get_with_revision(&lease_key(checkpoint.shard))
            .await?
        else {
            return Err(BpError::bad_value(
                "checkpoint lease",
                checkpoint.shard.to_string(),
                "no active writer lease",
            ));
        };
        let lease: WriterLease = serde_json::from_slice(&payload)?;
        if !lease.is_held_by(&checkpoint.writer_id, checkpoint.fencing_token)
            || lease.is_expired(TimestampMilliseconds::now())
        {
            return Err(BpError::bad_value(
                "checkpoint lease",
                checkpoint.fencing_token.to_string(),
                format!(
                    "lease is held by {} with token {}",
                    lease.owner_id, lease.fencing_token
                ),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl StateStore for NatsStateStore {
    async fn load(&self, shard: ShardId) -> BpResult<Option<CheckpointRecord>> {
        Ok(self
            .load_with_revision(shard)
            .await?
            .map(|(checkpoint, _)| checkpoint))
    }

    async fn save(&self, checkpoint: &CheckpointRecord) -> BpResult<()> {
        self.ensure_lease_holder(checkpoint).await?;
        // Read the current revision and conditionally update. If another writer
        // wins the race, `update` fails rather than overwriting its state.
        let expected_revision = self
            .load_with_revision(checkpoint.shard)
            .await?
            .map(|(_, revision)| revision);
        self.compare_and_swap(checkpoint, expected_revision).await?;
        Ok(())
    }
}

/// Derives the KV bucket used for source checkpoints.
pub fn checkpoint_bucket(shard_subject_prefix: &str) -> String {
    JetStream::kv_bucket_name(&format!("{shard_subject_prefix}.checkpoints"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::STATE_SCHEMA_VERSION;

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
            CheckpointRecord::from_message(&message, ShardId::new(0), AggregatorState::new())
                .unwrap(),
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
        let bucket = checkpoint_bucket("aggregator.shards");
        assert_eq!(bucket, "aggregator_shards_checkpoints");
    }
}
