//! Source-position checkpoints for aggregator partitions.
//!
//! A checkpoint records the last source message whose effects were committed by
//! a partition writer. Consumers are acknowledged only after this record is
//! durable, so a crash can replay work but cannot silently skip it.

use crate::config::AggregatorConfig;
use crate::partition::PartitionId;
use async_trait::async_trait;
use platform_errors::{BpError, BpResult};
use platform_nats::{JetStream, KeyValue, Message};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::Mutex as AsyncMutex;

/// Position of the last message processed by a partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceCheckpoint {
    /// JetStream stream that delivered the message.
    pub stream: String,
    /// Last processed sequence in that stream.
    pub stream_sequence: u64,
    /// Consumer sequence of the last processed message, when known.
    pub consumer_sequence: Option<u64>,
    /// Subject of the last processed message.
    pub subject: String,
}

impl SourceCheckpoint {
    /// Builds a checkpoint from a source message.
    ///
    /// Returns `None` for messages that did not come from JetStream (for
    /// example messages created by an in-memory test source), because there is
    /// no source position to persist.
    pub fn from_message(message: &Message) -> BpResult<Option<Self>> {
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
        Ok(Some(Self {
            stream: stream.clone(),
            stream_sequence,
            consumer_sequence: message.consumer_sequence,
            subject: message.subject.clone(),
        }))
    }

    /// The first stream sequence that must be delivered on resume.
    pub fn resume_sequence(&self) -> u64 {
        self.stream_sequence.saturating_add(1)
    }

    /// Alias for [`Self::resume_sequence`].
    pub fn next_stream_sequence(&self) -> u64 {
        self.resume_sequence()
    }
}

/// Durable storage for partition source checkpoints.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn load(&self, partition: PartitionId) -> BpResult<Option<SourceCheckpoint>>;

    async fn save(&self, partition: PartitionId, checkpoint: &SourceCheckpoint) -> BpResult<()>;
}

/// In-memory checkpoint store used by tests and single-process runs.
#[derive(Debug, Default)]
pub struct InMemoryCheckpointStore {
    checkpoints: Mutex<HashMap<PartitionId, SourceCheckpoint>>,
}

impl InMemoryCheckpointStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, partition: PartitionId) -> Option<SourceCheckpoint> {
        self.checkpoints
            .lock()
            .expect("checkpoint store lock")
            .get(&partition)
            .cloned()
    }
}

#[async_trait]
impl CheckpointStore for InMemoryCheckpointStore {
    async fn load(&self, partition: PartitionId) -> BpResult<Option<SourceCheckpoint>> {
        Ok(self.get(partition))
    }

    async fn save(&self, partition: PartitionId, checkpoint: &SourceCheckpoint) -> BpResult<()> {
        self.checkpoints
            .lock()
            .expect("checkpoint store lock")
            .insert(partition, checkpoint.clone());
        Ok(())
    }
}

/// Checkpoints stored in a NATS KV bucket owned by the aggregator.
pub struct NatsCheckpointStore {
    values: KeyValue,
    key_prefix: String,
    writes: AsyncMutex<()>,
}

impl NatsCheckpointStore {
    pub async fn connect(config: &AggregatorConfig) -> BpResult<Self> {
        let bucket = config.checkpoint_bucket();
        let values =
            KeyValue::connect(&config.nats_url, "platform-aggregator-checkpoints", &bucket).await?;
        Ok(Self {
            values,
            key_prefix: "partition".to_string(),
            writes: AsyncMutex::new(()),
        })
    }

    pub fn bucket(&self) -> &str {
        self.values.bucket()
    }

    fn key(&self, partition: PartitionId) -> String {
        format!("{}-{}", self.key_prefix, partition.get())
    }
}

#[async_trait]
impl CheckpointStore for NatsCheckpointStore {
    async fn load(&self, partition: PartitionId) -> BpResult<Option<SourceCheckpoint>> {
        let Some(payload) = self.values.get_last(&self.key(partition)).await? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&payload)?))
    }

    async fn save(&self, partition: PartitionId, checkpoint: &SourceCheckpoint) -> BpResult<()> {
        // TODO: replace this process-local serialisation with a CAS write tied
        // to partition ownership once the full keyed state is checkpointed.
        let _guard = self.writes.lock().await;
        let payload = serde_json::to_vec(checkpoint)?;
        self.values.put(&self.key(partition), &payload).await?;
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

    fn message() -> Message {
        Message {
            subject: "aggregator.shards.3".to_string(),
            stream: Some("aggregator-shards".to_string()),
            stream_sequence: Some(41),
            consumer_sequence: Some(9),
            ..Message::default()
        }
    }

    #[test]
    fn checkpoint_resumes_after_the_last_processed_message() {
        let checkpoint = SourceCheckpoint::from_message(&message()).unwrap().unwrap();

        assert_eq!(checkpoint.stream, "aggregator-shards");
        assert_eq!(checkpoint.stream_sequence, 41);
        assert_eq!(checkpoint.consumer_sequence, Some(9));
        assert_eq!(checkpoint.subject, "aggregator.shards.3");
        assert_eq!(checkpoint.resume_sequence(), 42);
        assert_eq!(checkpoint.next_stream_sequence(), 42);
    }

    #[test]
    fn messages_without_jetstream_metadata_do_not_produce_checkpoints() {
        let message = Message {
            subject: "test".to_string(),
            ..Message::default()
        };

        assert_eq!(SourceCheckpoint::from_message(&message).unwrap(), None);
    }

    #[tokio::test]
    async fn in_memory_store_round_trips_checkpoints() {
        let store = InMemoryCheckpointStore::new();
        let partition = PartitionId::new(2);
        let checkpoint = SourceCheckpoint::from_message(&message()).unwrap().unwrap();

        store.save(partition, &checkpoint).await.unwrap();

        assert_eq!(store.load(partition).await.unwrap(), Some(checkpoint));
        assert_eq!(store.load(PartitionId::new(3)).await.unwrap(), None);
    }

    #[test]
    fn checkpoint_bucket_is_a_valid_kv_name() {
        let bucket = checkpoint_bucket("aggregator.shards");
        assert_eq!(bucket, "aggregator_shards_checkpoints");
    }
}
