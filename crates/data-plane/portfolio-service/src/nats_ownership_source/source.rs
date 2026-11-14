use crate::nats_ownership_source::ownership_checkpoint::OwnershipCheckpoint;
use crate::{
    OwnershipConfig,
    ownership_source::{OwnershipEventSource, OwnershipMessage},
};
use async_trait::async_trait;
use platform_errors::{PlatformError, PlatformResult};
use platform_nats::{JetStreamConsumer, KeyValue, NatsSubject};
use platform_portfolio_ownership::{
    OWNERSHIP_EVENT_SUBJECT, OWNERSHIP_STREAM_PREFIX, OwnershipEvent,
};

const CONSUMER_NAME: &str = "platform-portfolio-ownership-consumer";

pub struct NatsOwnershipSource {
    consumer: JetStreamConsumer,
    checkpoints: KeyValue,
    checkpoint_key: String,
}

impl NatsOwnershipSource {
    pub async fn connect(config: &OwnershipConfig) -> PlatformResult<Self> {
        let checkpoints =
            KeyValue::connect(config.nats_url(), CONSUMER_NAME, config.checkpoint_bucket()).await?;
        let checkpoint_key = format!(
            "{}-ownership",
            NatsSubject::normalize_segment(config.durable())
        );
        let start_sequence = match checkpoints.get_last(&checkpoint_key).await? {
            Some(payload) => {
                let checkpoint = OwnershipCheckpoint::from_bytes(&payload)?;
                Some(checkpoint.sequence().saturating_add(1))
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
    async fn next(&self) -> PlatformResult<Option<OwnershipMessage>> {
        let Some(message) = self.consumer.next_message_unacked().await else {
            return Ok(None);
        };
        let sequence = message.stream_sequence.ok_or_else(|| {
            PlatformError::bad_value(
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

    async fn checkpoint(&self, sequence: u64) -> PlatformResult<()> {
        let payload = OwnershipCheckpoint::new(sequence).to_bytes()?;
        self.checkpoints.put(&self.checkpoint_key, &payload).await?;
        Ok(())
    }

    async fn acknowledge(&self, message: &OwnershipMessage) -> PlatformResult<()> {
        match message.raw() {
            Some(raw) => self.consumer.ack(raw).await,
            None => Ok(()),
        }
    }
}
