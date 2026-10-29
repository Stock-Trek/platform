use crate::{
    config::IngesterConfig,
    ingester::{IngestOptions, Ingester},
    sequence::SequenceStore,
    source::MarketMessageSource,
};
use platform_errors::BpResult;
use platform_market_data::event::ParsedMessage;
use platform_nats::EventPublisher;
use platform_runtime::Runtime;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

const MESSAGE_CHANNEL_CAPACITY: usize = 1_024;

pub struct IngesterRunner<S, P> {
    config: IngesterConfig,
    source: S,
    publisher: Arc<P>,
    sequences: Option<Arc<dyn SequenceStore>>,
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
            sequences: None,
        }
    }

    pub fn with_sequence_store(mut self, sequences: Arc<dyn SequenceStore>) -> Self {
        self.sequences = Some(sequences);
        self
    }

    pub async fn run(self) -> BpResult<()> {
        let Self {
            config,
            source,
            publisher,
            sequences,
        } = self;
        info!("connected to NATS");

        let (sender, mut receiver) = mpsc::channel(MESSAGE_CHANNEL_CAPACITY);
        let source_task = tokio::spawn(async move { source.run(sender).await });

        let mut ingester = {
            let options = IngestOptions {
                exchange: config.exchange,
                subject_prefix: config.nats_subject_prefix.clone(),
            };
            match sequences {
                Some(sequences) => Ingester::with_sequence_store(publisher, options, sequences),
                None => Ingester::new(publisher, options),
            }
        };

        let result = Runtime::run_until_shutdown(Self::process_messages(
            &config,
            &mut receiver,
            &mut ingester,
        ))
        .await;
        source_task.abort();
        let source_result = source_task.await;
        result?;
        match source_result {
            Ok(result) => result,
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn process_messages(
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

    struct FakeSource {
        fail: bool,
    }

    #[async_trait]
    impl MarketMessageSource for FakeSource {
        async fn run(&self, _sender: mpsc::Sender<ParsedMessage>) -> BpResult<()> {
            if self.fail {
                Err(platform_errors::BpError::external("source failed"))
            } else {
                Ok(())
            }
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
        let runner = IngesterRunner::new(
            config(),
            FakeSource { fail: false },
            Arc::new(FakePublisher),
        );

        runner.run().await.unwrap();
    }

    #[tokio::test]
    async fn propagates_source_error() {
        let runner =
            IngesterRunner::new(config(), FakeSource { fail: true }, Arc::new(FakePublisher));

        let error = runner.run().await.unwrap_err();

        assert_eq!(error.to_string(), "source failed");
    }
}
