use crate::{OwnershipConfig, OwnershipEventSource, OwnershipMessage};
use async_trait::async_trait;
use platform_errors::{BpError, BpResult};
use platform_nats::{JetStreamConsumer, KeyValue, NatsSubject};
use platform_portfolio_ownership::{
    OWNERSHIP_EVENT_SUBJECT, OWNERSHIP_STREAM_PREFIX, OwnershipEvent,
};
use serde::{Deserialize, Serialize};

const CONSUMER_NAME: &str = "platform-portfolio-ownership-consumer";

#[derive(Serialize, Deserialize)]
struct OwnershipCheckpoint {
    sequence: u64,
}

pub struct NatsOwnershipSource {
    consumer: JetStreamConsumer,
    checkpoints: KeyValue,
    checkpoint_key: String,
}

impl NatsOwnershipSource {
    pub async fn connect(config: &OwnershipConfig) -> BpResult<Self> {
        let checkpoints =
            KeyValue::connect(config.nats_url(), CONSUMER_NAME, config.checkpoint_bucket()).await?;
        let checkpoint_key = format!(
            "{}-ownership",
            NatsSubject::normalize_segment(config.durable())
        );
        let start_sequence = match checkpoints.get_last(&checkpoint_key).await? {
            Some(payload) => {
                let checkpoint: OwnershipCheckpoint = serde_json::from_slice(&payload)?;
                Some(checkpoint.sequence.saturating_add(1))
            }
            None => None,
        };
        let consumer = JetStreamConsumer::connect_from(
            config.nats_url(),
            config.client_name(),
            OWNERSHIP_STREAM_PREFIX,
            OWNERSHIP_EVENT_SUBJECT,
            config.durable(),
            start_sequence,
        )
        .await?;
        Ok(Self {
            consumer,
            checkpoints,
            checkpoint_key,
        })
    }
}

#[async_trait]
impl OwnershipEventSource for NatsOwnershipSource {
    async fn next(&self) -> BpResult<Option<OwnershipMessage>> {
        let Some(message) = self.consumer.next_message_unacked().await else {
            return Ok(None);
        };
        let sequence = message.stream_sequence.ok_or_else(|| {
            BpError::bad_value(
                "ownership message",
                message.subject.clone(),
                "missing sequence",
            )
        })?;
        let event: OwnershipEvent = serde_json::from_slice(&message.payload)?;
        Ok(Some(OwnershipMessage::new(
            event,
            Some(sequence),
            Some(message),
        )))
    }

    async fn checkpoint(&self, sequence: u64) -> BpResult<()> {
        let payload = serde_json::to_vec(&OwnershipCheckpoint { sequence })?;
        self.checkpoints.put(&self.checkpoint_key, &payload).await?;
        Ok(())
    }

    async fn acknowledge(&self, message: &OwnershipMessage) -> BpResult<()> {
        match message.raw() {
            Some(raw) => self.consumer.ack(raw).await,
            None => Ok(()),
        }
    }
}
