use crate::{
    config::IngesterConfig,
    decoders::{
        binance::codec::{BinanceBinaryCodec, BinanceBinaryFrame},
        decoder::{BinaryDecoder, FrameDecoder, JsonDecoder},
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

const BINANCE_API_KEY_HEADER: &str = "X-MBX-APIKEY";
const DEFAULT_API_KEY_HEADER: &str = "X-API-KEY";

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
            Self::Json(_) => formatter.write_str("Json"),
            Self::Binary(_) => formatter.write_str("Binary"),
        }
    }
}

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
            StreamFormat::Json(decoder) => self.run_json(decoder.clone(), sender).await,
            StreamFormat::Binary(decoder) => self.run_binary(decoder.clone(), sender).await,
        }
    }

    async fn run_json(
        &self,
        decoder: Arc<dyn JsonDecoder>,
        sender: mpsc::Sender<ParsedMessage>,
    ) -> BpResult<()> {
        let listener = Arc::new(IrisFrameListener::new(self.exchange, decoder, sender));
        let client = IrisClient::<(), Value>::new(self.iris_config()?, listener, &self.url);
        client.connect().await.map_err(BpError::external)?;
        self.wait_for_shutdown().await
    }

    async fn run_binary(
        &self,
        decoder: Arc<dyn BinaryDecoder>,
        sender: mpsc::Sender<ParsedMessage>,
    ) -> BpResult<()> {
        let listener = Arc::new(IrisFrameListener::new(self.exchange, decoder, sender));
        let client = IrisClient::<(), BinanceBinaryFrame, BinanceBinaryCodec>::new_with_codec(
            self.iris_config()?,
            listener,
            &self.url,
            BinanceBinaryCodec,
        );
        client.connect().await.map_err(BpError::external)?;
        self.wait_for_shutdown().await
    }

    async fn wait_for_shutdown(&self) -> BpResult<()> {
        info!(
            exchange = %self.exchange,
            format = ?self.stream_format,
            "connected to exchange websocket through iris"
        );

        // iris owns reconnect, ping and pong handling. The runner aborts this
        // task when the process is shutting down.
        std::future::pending::<()>().await;
        Ok(())
    }

    fn iris_config(&self) -> BpResult<IrisConfig> {
        let mut config =
            IrisConfig::new().with_server_close_behavior(ServerCloseBehavior::Reconnect);
        if let Some(api_key) = &self.api_key {
            // TODO: map authentication headers to each exchange explicitly.
            let header = match self.exchange {
                CexId::Binance => BINANCE_API_KEY_HEADER,
                _ => DEFAULT_API_KEY_HEADER,
            };
            config = config
                .with_header_str(header, api_key)
                .map_err(BpError::external)?;
        }
        Ok(config)
    }
}

struct IrisFrameListener {
    exchange: CexId,
    decoder: Arc<dyn FrameDecoder>,
    sender: mpsc::Sender<ParsedMessage>,
}

impl IrisFrameListener {
    fn new(
        exchange: CexId,
        decoder: Arc<dyn FrameDecoder>,
        sender: mpsc::Sender<ParsedMessage>,
    ) -> Self {
        Self {
            exchange,
            decoder,
            sender,
        }
    }

    async fn handle_frame(&self, frame: Vec<u8>) {
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

    fn handle_error(&self, error: &IrisConnectionError) {
        warn!(exchange = %self.exchange, %error, "exchange websocket error");
    }
}

impl fmt::Debug for IrisFrameListener {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IrisFrameListener")
            .field("exchange", &self.exchange)
            .finish()
    }
}

#[async_trait]
impl IrisListener<Value> for IrisFrameListener {
    async fn on_message(&self, message: Value) {
        let frame = match serde_json::to_vec(&message) {
            Ok(frame) => frame,
            Err(error) => {
                warn!(exchange = %self.exchange, %error, "discarding unencodable JSON frame");
                return;
            }
        };
        self.handle_frame(frame).await;
    }

    async fn on_error(&self, error: &IrisConnectionError) {
        self.handle_error(error);
    }
}

#[async_trait]
impl IrisListener<BinanceBinaryFrame> for IrisFrameListener {
    async fn on_message(&self, frame: BinanceBinaryFrame) {
        self.handle_frame(frame.into_bytes()).await;
    }

    async fn on_error(&self, error: &IrisConnectionError) {
        self.handle_error(error);
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
