use crate::config::IngesterConfig;
use async_trait::async_trait;
use iris::{
    Client as IrisClient, Config as IrisConfig, ConnectionError as IrisConnectionError,
    Listener as IrisListener, ServerCloseBehavior,
};
use platform_errors::{BpError, BpResult, ExchangeError, IngesterError};
use platform_market_data::{
    decoders::{
        codec::{CodecFactory, CodecHandle, SharedCodec},
        frame::DecodedFrame,
    },
    event::ParsedMessage,
};
use platform_metrics::descriptions::{
    DECODE_KIND_FRAME, DROP_REASON_CHANNEL_CLOSED, STAGE_INGESTER,
};
use platform_metrics::market_data::MarketDataMetrics;
use std::{fmt, str::FromStr, sync::Arc};
use stock_trek_types::cex::cex_id::CexId;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

const BINANCE_API_KEY_HEADER: &str = "X-MBX-APIKEY";

/// Wire format used by an exchange websocket.
///
/// Each variant carries the codec that understands that format for the
/// configured exchange, so the transport does not need exchange knowledge.
#[derive(Clone)]
pub enum StreamFormat {
    Json(SharedCodec),
    Binary(SharedCodec),
}

impl StreamFormat {
    pub fn for_exchange(value: &str, exchange: CexId) -> Result<Self, IngesterError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json(CodecFactory::json_codec(exchange))),
            "binary" | "binance_sbe" | "binance-sbe" => {
                Ok(Self::Binary(CodecFactory::binary_codec(exchange)?))
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

/// Source of parsed exchange websocket messages.
#[async_trait]
pub trait MarketMessageSource: Send + Sync {
    async fn run(&self, sender: mpsc::Sender<ParsedMessage>) -> BpResult<()>;
}

/// Websocket transport shared by all exchange codecs.
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
        IngesterConfig::validate_trade_streams(self.exchange, &self.url)?;
        let codec = match &self.stream_format {
            StreamFormat::Json(codec) | StreamFormat::Binary(codec) => codec.clone(),
        };
        let listener = Arc::new(IrisFrameListener::new(self.exchange, sender));
        let client = IrisClient::<(), DecodedFrame, CodecHandle>::new_with_codec(
            self.iris_config()?,
            listener,
            &self.url,
            CodecHandle::new(codec),
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
            let header = Self::api_key_header(self.exchange)?;
            config = config
                .with_header_str(header, api_key)
                .map_err(BpError::external)?;
        }
        Ok(config)
    }

    fn api_key_header(exchange: CexId) -> BpResult<&'static str> {
        match exchange {
            CexId::Binance => Ok(BINANCE_API_KEY_HEADER),
            exchange => Err(BpError::Exchange(ExchangeError::UnsupportedExchange {
                exchange: exchange.to_string(),
            })),
        }
    }
}

#[async_trait]
impl MarketMessageSource for WebsocketSource {
    async fn run(&self, sender: mpsc::Sender<ParsedMessage>) -> BpResult<()> {
        WebsocketSource::run(self, sender).await
    }
}

struct IrisFrameListener {
    exchange: CexId,
    sender: mpsc::Sender<ParsedMessage>,
}

impl IrisFrameListener {
    fn new(exchange: CexId, sender: mpsc::Sender<ParsedMessage>) -> Self {
        Self { exchange, sender }
    }

    async fn handle_frame(&self, frame: DecodedFrame) {
        match frame.result {
            Ok(parsed) => {
                if self.sender.send(parsed).await.is_err() {
                    MarketDataMetrics::record_dropped_event(
                        STAGE_INGESTER,
                        DROP_REASON_CHANNEL_CLOSED,
                    );
                    debug!(exchange = %self.exchange, "ingester message channel closed");
                }
            }
            Err(error) => {
                MarketDataMetrics::record_decode_failure(STAGE_INGESTER, DECODE_KIND_FRAME);
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
impl IrisListener<DecodedFrame> for IrisFrameListener {
    async fn on_message(&self, frame: DecodedFrame) {
        self.handle_frame(frame).await;
    }

    async fn on_error(&self, error: &IrisConnectionError) {
        self.handle_error(error);
    }
}

#[cfg(test)]
mod tests;
