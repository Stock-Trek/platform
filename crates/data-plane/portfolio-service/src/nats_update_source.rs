use crate::{PortfolioUpdate, PortfolioUpdateMessage, PortfolioUpdateSource, UpdateConfig};
use async_trait::async_trait;
use platform_errors::{BpError, BpResult};
use platform_nats::{JetStreamConsumer, KeyValue, NatsSubject};
use serde::{Deserialize, Serialize};

const CONSUMER_NAME: &str = "platform-portfolio-update-consumer";

#[derive(Serialize, Deserialize)]
struct UpdateCheckpoint {
    sequence: u64,
}

pub struct NatsUpdateSource {
    consumer: JetStreamConsumer,
    checkpoints: KeyValue,
    checkpoint_key: String,
}

impl NatsUpdateSource {
    pub async fn connect(config: &UpdateConfig) -> BpResult<Self> {
        let checkpoints =
            KeyValue::connect(config.nats_url(), CONSUMER_NAME, config.checkpoint_bucket()).await?;
        let checkpoint_key = format!(
            "{}-updates",
            NatsSubject::normalize_segment(config.durable())
        );
        let start_sequence = match checkpoints.get_last(&checkpoint_key).await? {
            Some(payload) => {
                let checkpoint: UpdateCheckpoint = serde_json::from_slice(&payload)?;
                Some(checkpoint.sequence.saturating_add(1))
            }
            None => None,
        };
        let consumer = JetStreamConsumer::connect_from(
            config.nats_url(),
            config.client_name(),
            crate::PORTFOLIO_UPDATE_STREAM_PREFIX,
            crate::PORTFOLIO_UPDATE_SUBJECT,
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
impl PortfolioUpdateSource for NatsUpdateSource {
    async fn next(&self) -> BpResult<Option<PortfolioUpdateMessage>> {
        let Some(message) = self.consumer.next_message_unacked().await else {
            return Ok(None);
        };
        let sequence = message.stream_sequence.ok_or_else(|| {
            BpError::bad_value(
                "portfolio update message",
                message.subject.clone(),
                "missing sequence",
            )
        })?;
        let update = PortfolioUpdate::from_bytes(&message.payload)?;
        Ok(Some(PortfolioUpdateMessage::new(
            update,
            Some(sequence),
            Some(message),
        )))
    }

    async fn checkpoint(&self, sequence: u64) -> BpResult<()> {
        let payload = serde_json::to_vec(&UpdateCheckpoint { sequence })?;
        self.checkpoints.put(&self.checkpoint_key, &payload).await?;
        Ok(())
    }

    async fn acknowledge(&self, message: &PortfolioUpdateMessage) -> BpResult<()> {
        match message.raw() {
            Some(raw) => self.consumer.ack(raw).await,
            None => Ok(()),
        }
    }
}
