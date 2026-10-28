use crate::{
    config::IngesterConfig,
    ingester::{IngestOptions, Ingester},
    publisher::EventPublisher,
    source::MarketMessageSource,
};
use platform_errors::BpResult;
use platform_market_data::event::ParsedMessage;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

const MESSAGE_CHANNEL_CAPACITY: usize = 1_024;

pub struct IngesterRunner<S, P> {
    config: IngesterConfig,
    source: S,
    publisher: Arc<P>,
}

impl<S, P> IngesterRunner<S, P>
where
    S: MarketMessageSource + 'static,
    P: EventPublisher + 'static,
{
    pub fn new(config: IngesterConfig, source: S, publisher: Arc<P>) -> Self {
        Self {
            config,
            source,
            publisher,
        }
    }

    pub async fn run(self) -> BpResult<()> {
        let Self {
            config,
            source,
            publisher,
        } = self;
        info!("connected to NATS");

        let (sender, mut receiver) = mpsc::channel(MESSAGE_CHANNEL_CAPACITY);
        let source_task = tokio::spawn(async move {
            if let Err(error) = source.run(sender).await {
                error!(%error, "market data source stopped");
            }
        });

        let mut ingester = Ingester::new(
            publisher,
            IngestOptions {
                exchange: config.exchange,
                subject_prefix: config.nats_subject_prefix.clone(),
            },
        );

        let result = Self::run_until_shutdown(&config, &mut receiver, &mut ingester).await;
        source_task.abort();
        result
    }

    async fn run_until_shutdown(
        config: &IngesterConfig,
        receiver: &mut mpsc::Receiver<ParsedMessage>,
        ingester: &mut Ingester<P>,
    ) -> BpResult<()> {
        loop {
            tokio::select! {
                message = receiver.recv() => {
                    let Some(message) = message else {
                        warn!(exchange = %config.exchange, "exchange websocket message channel closed");
                        break;
                    };
                    if let Err(error) = ingester.ingest(message).await {
                        error!(%error, "failed to ingest market event");
                    }
                }
                signal = platform_runtime::Runtime::shutdown_signal() => {
                    signal?;
                    break;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::StreamFormat;
    use async_trait::async_trait;
    use platform_errors::BpResult;
    use platform_market_data::event::ParsedMessage;
    use stock_trek_types::cex::cex_id::CexId;

    struct FakeSource;

    #[async_trait]
    impl MarketMessageSource for FakeSource {
        async fn run(&self, _sender: mpsc::Sender<ParsedMessage>) -> BpResult<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakePublisher;

    #[async_trait]
    impl EventPublisher for FakePublisher {
        async fn publish(&self, _subject: &str, _payload: &[u8]) -> BpResult<()> {
            Ok(())
        }
    }

    fn config() -> IngesterConfig {
        IngesterConfig {
            log_filter: "info".to_string(),
            exchange: CexId::Binance,
            websocket_url: "wss://example.invalid".to_string(),
            websocket_api_key: None,
            stream_format: StreamFormat::for_exchange("json", CexId::Binance).unwrap(),
            nats_url: "nats://localhost:4222".to_string(),
            nats_subject_prefix: "market".to_string(),
        }
    }

    #[tokio::test]
    async fn runs_with_injected_source_and_publisher() {
        let runner = IngesterRunner::new(config(), FakeSource, Arc::new(FakePublisher));

        runner.run().await.unwrap();
    }
}
