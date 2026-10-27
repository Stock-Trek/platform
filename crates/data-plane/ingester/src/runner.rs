use crate::{
    config::IngesterConfig,
    ingester::{IngestOptions, Ingester},
    nats::NatsJetstreamPublisher,
    publisher::EventPublisher,
    source::{ParsedMessage, WebsocketSource},
};
use platform_errors::BpResult;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

const MESSAGE_CHANNEL_CAPACITY: usize = 1_024;

pub struct IngesterRunner {
    config: IngesterConfig,
}

impl IngesterRunner {
    pub fn new(config: IngesterConfig) -> Self {
        Self { config }
    }

    pub async fn run(self) -> BpResult<()> {
        let publisher = Arc::new(
            NatsJetstreamPublisher::connect(&self.config.nats_url, "platform-ingester").await?,
        );
        info!("connected to NATS");

        let (sender, mut receiver) = mpsc::channel(MESSAGE_CHANNEL_CAPACITY);
        let source = WebsocketSource::from_config(&self.config);
        let source_task = tokio::spawn(async move {
            if let Err(error) = source.run(sender).await {
                error!(%error, "market data source stopped");
            }
        });

        let mut ingester = Ingester::new(
            publisher,
            IngestOptions {
                exchange: self.config.exchange,
                subject_prefix: self.config.nats_subject_prefix.clone(),
            },
        );

        let result = self.run_until_shutdown(&mut receiver, &mut ingester).await;
        source_task.abort();
        result
    }

    async fn run_until_shutdown<P: EventPublisher>(
        &self,
        receiver: &mut mpsc::Receiver<ParsedMessage>,
        ingester: &mut Ingester<P>,
    ) -> BpResult<()> {
        loop {
            tokio::select! {
                message = receiver.recv() => {
                    let Some(message) = message else {
                        warn!(exchange = %self.config.exchange, "exchange websocket message channel closed");
                        break;
                    };
                    if let Err(error) = ingester.ingest(message).await {
                        error!(%error, "failed to ingest market event");
                    }
                }
                signal = tokio::signal::ctrl_c() => {
                    signal?;
                    info!("shutdown requested");
                    break;
                }
            }
        }
        Ok(())
    }
}
