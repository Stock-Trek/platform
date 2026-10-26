pub mod config;
pub mod event;
pub mod ingester;
pub mod nats;
pub mod publisher;
pub mod sequence;
pub mod source;

pub use config::IngesterConfig;
pub use event::{GapMarker, MarketEvent};
pub use ingester::{IngestOptions, Ingester};
pub use nats::NatsJetstreamPublisher;
pub use publisher::EventPublisher;
pub use sequence::{SequenceObservation, SequenceTracker};
pub use source::{ParseError, ParsedMessage, binance_client, parse_binance_message};

use platform_errors::{BpError, BpResult};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

const MESSAGE_CHANNEL_CAPACITY: usize = 1_024;

pub async fn run(config: IngesterConfig) -> BpResult<()> {
    let publisher =
        Arc::new(NatsJetstreamPublisher::connect(&config.nats_url, "platform-ingester").await?);
    info!("connected to NATS");

    let (sender, mut receiver) = mpsc::channel(MESSAGE_CHANNEL_CAPACITY);
    let client = binance_client(&config.binance_websocket_url, sender);
    client.connect().await.map_err(BpError::external)?;
    info!("connected to Binance websocket");

    let mut ingester = Ingester::new(
        publisher,
        IngestOptions {
            exchange: "binance".to_string(),
            subject_prefix: config.subject_prefix,
        },
    );

    let run_result = run_until_shutdown(&mut receiver, &mut ingester).await;
    let disconnect_result = client.disconnect().await.map_err(BpError::external);
    run_result.and(disconnect_result)
}

async fn run_until_shutdown<P: EventPublisher>(
    receiver: &mut mpsc::Receiver<Value>,
    ingester: &mut Ingester<P>,
) -> BpResult<()> {
    loop {
        tokio::select! {
            message = receiver.recv() => {
                let Some(message) = message else {
                    warn!("binance websocket message channel closed");
                    break;
                };
                match parse_binance_message(message) {
                    Ok(message) => {
                        if let Err(error) = ingester.ingest(message).await {
                            error!(%error, "failed to ingest market event");
                        }
                    }
                    Err(error) => warn!(%error, "discarding invalid binance message"),
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
