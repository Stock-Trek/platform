use crate::{
    config::IngesterConfig,
    decoders::{
        decoder::{BinaryDecoder, JsonDecoder},
        factory::DecoderFactory,
    },
};
use async_trait::async_trait;
use iris::{
    Client as IrisClient, Config as IrisConfig, ConnectionError as IrisConnectionError,
    Listener as IrisListener, ServerCloseBehavior,
};
use platform_errors::{BpError, BpResult, IngesterError};
use platform_types::new_types::TimestampMilliseconds;
use serde_json::Value;
use std::{fmt, str::FromStr, sync::Arc};
use stock_trek_types::cex::cex_id::CexId;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedMessage {
    pub stream: String,
    pub sequence: Option<u64>,
    pub exchange_timestamp: Option<TimestampMilliseconds>,
    /// Base64 encoded raw frame from the exchange.
    pub payload: String,
}

/// Wire format used by an exchange websocket.
///
/// Each variant carries the decoder that understands that format for the
/// configured exchange, so the transport does not need exchange knowledge.
#[derive(Clone)]
pub enum StreamFormat {
    Json(Arc<dyn JsonDecoder>),
    Binary(Arc<dyn BinaryDecoder>),
}

impl StreamFormat {
    pub fn for_exchange(value: &str, exchange: CexId) -> Result<Self, IngesterError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json(DecoderFactory::json_decoder(exchange))),
            "binary" | "binance_sbe" | "binance-sbe" => {
                Ok(Self::Binary(DecoderFactory::binary_decoder(exchange)?))
            }
            _ => Err(IngesterError::UnsupportedStreamFormat {
                format: value.to_string(),
            }),
        }
    }
}

impl FromStr for StreamFormat {
    type Err = IngesterError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::for_exchange(value, CexId::Binance)
    }
}

impl fmt::Debug for StreamFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(decoder) => formatter
                .debug_tuple("Json")
                .field(&decoder.exchange())
                .finish(),
            Self::Binary(decoder) => formatter
                .debug_tuple("Binary")
                .field(&decoder.exchange())
                .finish(),
        }
    }
}

impl PartialEq for StreamFormat {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Json(left), Self::Json(right)) => left.exchange() == right.exchange(),
            (Self::Binary(left), Self::Binary(right)) => left.exchange() == right.exchange(),
            _ => false,
        }
    }
}

impl Eq for StreamFormat {}

/// Websocket transport shared by all exchange decoders.
pub struct WebsocketSource {
    exchange: CexId,
    url: String,
    api_key: Option<String>,
    stream_format: StreamFormat,
}

impl WebsocketSource {
    pub fn new(
        exchange: CexId,
        url: impl Into<String>,
        api_key: Option<String>,
        stream_format: StreamFormat,
    ) -> Self {
        Self {
            exchange,
            url: url.into(),
            api_key,
            stream_format,
        }
    }

    pub fn from_config(config: &IngesterConfig) -> Self {
        Self::new(
            config.exchange,
            config.websocket_url.clone(),
            config.websocket_api_key.clone(),
            config.stream_format.clone(),
        )
    }

    pub async fn run(&self, sender: mpsc::Sender<ParsedMessage>) -> BpResult<()> {
        match &self.stream_format {
            StreamFormat::Json(decoder) => self.run_iris(decoder.clone(), sender).await,
            // TODO: iris deserializes every frame as JSON, so binary streams
            // cannot be transported until it exposes raw frames.
            StreamFormat::Binary(_) => {
                Err(BpError::Ingester(IngesterError::UnsupportedStreamFormat {
                    format: "binary frames are not supported by iris".to_string(),
                }))
            }
        }
    }

    async fn run_iris(
        &self,
        decoder: Arc<dyn JsonDecoder>,
        sender: mpsc::Sender<ParsedMessage>,
    ) -> BpResult<()> {
        if self.api_key.is_some() {
            // TODO: iris does not currently expose custom websocket headers.
            warn!(
                exchange = %self.exchange,
                "ignoring websocket api key because iris does not support custom headers"
            );
        }

        let listener = Arc::new(IrisJsonListener::new(self.exchange, decoder, sender));
        let config = IrisConfig::new().with_server_close_behavior(ServerCloseBehavior::Reconnect);
        let client = IrisClient::<(), Value>::new(config, listener, &self.url);
        client.connect().await.map_err(BpError::external)?;
        info!(exchange = %self.exchange, "connected to exchange websocket through iris");

        // iris owns reconnect, ping and pong handling. The runner aborts this
        // task when the process is shutting down.
        std::future::pending::<()>().await;
        Ok(())
    }
}

struct IrisJsonListener {
    exchange: CexId,
    decoder: Arc<dyn JsonDecoder>,
    sender: mpsc::Sender<ParsedMessage>,
}

impl IrisJsonListener {
    fn new(
        exchange: CexId,
        decoder: Arc<dyn JsonDecoder>,
        sender: mpsc::Sender<ParsedMessage>,
    ) -> Self {
        Self {
            exchange,
            decoder,
            sender,
        }
    }
}

impl fmt::Debug for IrisJsonListener {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IrisJsonListener")
            .field("exchange", &self.exchange)
            .field("decoder", &self.decoder.exchange())
            .finish()
    }
}

#[async_trait]
impl IrisListener<Value> for IrisJsonListener {
    async fn on_message(&self, message: Value) {
        // iris hands us a parsed value rather than the original frame, so the
        // payload is the canonical re-serialised message.
        // TODO: preserve the exact raw frame once iris exposes it.
        let frame = match serde_json::to_vec(&message) {
            Ok(frame) => frame,
            Err(error) => {
                warn!(
                    exchange = %self.exchange,
                    %error,
                    "discarding websocket message that could not be serialised"
                );
                return;
            }
        };

        match self.decoder.decode(&frame) {
            Ok(parsed) => {
                if self.sender.send(parsed).await.is_err() {
                    debug!(exchange = %self.exchange, "ingester message channel closed");
                }
            }
            Err(error) => {
                warn!(exchange = %self.exchange, %error, "discarding undecodable frame");
            }
        }
    }

    async fn on_error(&self, error: &IrisConnectionError) {
        warn!(exchange = %self.exchange, %error, "exchange websocket error");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_json_stream_format() {
        assert!(matches!(
            " json ".parse::<StreamFormat>().unwrap(),
            StreamFormat::Json(_)
        ));
    }

    #[test]
    fn parses_binary_stream_format() {
        assert!(matches!(
            "binary".parse::<StreamFormat>().unwrap(),
            StreamFormat::Binary(_)
        ));
    }

    #[test]
    fn rejects_unknown_stream_formats() {
        assert!(matches!(
            "protobuf".parse::<StreamFormat>(),
            Err(IngesterError::UnsupportedStreamFormat { .. })
        ));
    }
}
