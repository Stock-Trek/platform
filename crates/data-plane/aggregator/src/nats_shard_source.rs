use crate::{
    config::AggregatorConfig, market_input::MarketInput, market_source::MarketSource,
    shard_id::ShardId, source_record::SourceRecord,
};
use async_trait::async_trait;
use platform_errors::PlatformResult;
use platform_metrics::MarketDataMetrics;
use platform_metrics::{DECODE_KIND_MARKET_EVENT, STAGE_AGGREGATOR};
use platform_nats::{ConsumerStart, JetStreamConsumer, Message};
use tracing::warn;

pub struct NatsShardSource {
    consumer: JetStreamConsumer,
    in_flight: Option<Message>,
}

impl NatsShardSource {
    pub async fn connect(
        config: &AggregatorConfig,
        shard: ShardId,
        start: ConsumerStart,
    ) -> PlatformResult<Self> {
        let shard_subject = shard.subject(&config.shard_subject_prefix);
        let filter = format!("{shard_subject}.>");
        let durable = config.shard_durable_name(shard);
        let consumer = JetStreamConsumer::connect_with_start(
            &config.nats_url,
            "platform-aggregator-shard",
            &config.shard_subject_prefix,
            &filter,
            &durable,
            start,
        )
        .await?;
        Ok(Self {
            consumer,
            in_flight: None,
        })
    }
}

#[async_trait]
impl MarketSource for NatsShardSource {
    async fn next_input(&mut self) -> PlatformResult<Option<SourceRecord>> {
        loop {
            let Some(message) = self.consumer.next_message_unacked().await else {
                continue;
            };
            match MarketInput::decode(&message.payload) {
                Ok(input) => {
                    self.in_flight = Some(message.clone());
                    return Ok(Some(SourceRecord::new(input, message)));
                }
                Err(error) => {
                    MarketDataMetrics::record_decode_failure(
                        STAGE_AGGREGATOR,
                        DECODE_KIND_MARKET_EVENT,
                    );
                    warn!(%error, "skipping malformed routed aggregator input");
                    self.consumer.ack(&message).await?;
                }
            }
        }
    }

    async fn ack(&mut self, _record: &SourceRecord) -> PlatformResult<()> {
        let Some(message) = self.in_flight.take() else {
            return Ok(());
        };
        self.consumer.ack(&message).await
    }
}
