use super::update_checkpoint::UpdateCheckpoint;
use crate::{
    portfolio::{
        PortfolioUpdate, PORTFOLIO_UPDATE_STREAM_PREFIX, PORTFOLIO_UPDATE_SUBJECT,
    },
    update_source::{PortfolioUpdateMessage, PortfolioUpdateSource},
    UpdateConfig,
};
use async_trait::async_trait;
use platform_errors::{BpError, BpResult};
use platform_nats::{JetStreamConsumer, KeyValue, NatsSubject};

const CONSUMER_NAME: &str = "platform-portfolio-update-consumer";

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
                let checkpoint = UpdateCheckpoint::from_bytes(&payload)?;
                Some(checkpoint.sequence().saturating_add(1))
            }
            None => None,
        };
        let consumer = JetStreamConsumer::connect_from(
            config.nats_url(),
            config.client_name(),
            PORTFOLIO_UPDATE_STREAM_PREFIX,
            PORTFOLIO_UPDATE_SUBJECT,
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
        let payload = UpdateCheckpoint::new(sequence).to_bytes()?;
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
