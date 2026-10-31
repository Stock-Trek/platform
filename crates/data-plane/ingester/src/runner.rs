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
use tracing::{error, info};

const MESSAGE_CHANNEL_CAPACITY: usize = 1_024;

pub struct IngesterRunner<S, P> {
    config: IngesterConfig,
    source: S,
    publisher: Arc<P>,
    sequences: Arc<dyn SequenceStore>,
}

impl<S, P> IngesterRunner<S, P>
where
    S: MarketMessageSource + 'static,
    P: EventPublisher + 'static,
{
    pub fn new(
        config: IngesterConfig,
        source: S,
        publisher: Arc<P>,
        sequences: Arc<dyn SequenceStore>,
    ) -> Self {
        Self {
            config,
            source,
            publisher,
            sequences,
        }
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
            Ingester::new(publisher, options, sequences)
        };

        let result = Runtime::run_until_shutdown(Self::process_messages(
            &config,
            &mut receiver,
            &mut ingester,
        ))
        .await;
        source_task.abort();
        match source_task.await {
            Ok(Err(error)) => Err(error),
            Err(error) if error.is_cancelled() => result,
            Err(error) => result.and_then(|()| Err(error.into())),
            Ok(Ok(())) => result.and_then(|()| {
                Err(platform_errors::BpError::external(
                    "exchange websocket source ended before shutdown was requested",
                ))
            }),
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
                        return Err(platform_errors::BpError::external(format!(
                            "{} exchange websocket message channel closed before shutdown was requested",
                            config.exchange
                        )));
                    };
                    if let Err(error) = ingester.ingest(message).await {
                        error!(%error, "failed to ingest market event");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
