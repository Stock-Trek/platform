use crate::{
    config::IngesterConfig,
    decoders::{BinanceSbeDecoder, BinaryDecoder, JsonDecoder},
};
use futures::{SinkExt, StreamExt};
use http::{HeaderName, HeaderValue, Uri};
use platform_errors::{BpError, BpResult, IngesterError};
use platform_types::new_types::TimestampMilliseconds;
use std::{fmt, str::FromStr, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::sleep};
use tokio_websockets::{ClientBuilder, Message};
use tracing::{debug, info, warn};

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const DEFAULT_BINARY_EXCHANGE: &str = "binance";

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
/// TODO: select the binary decoder from configuration so exchanges other than
/// Binance can expose binary streams.
#[derive(Clone)]
pub enum StreamFormat {
    Json,
    Binary(Arc<dyn BinaryDecoder>),
}

impl StreamFormat {
    pub fn for_exchange(value: &str, exchange: &str) -> Result<Self, IngesterError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "binary" | "binance_sbe" | "binance-sbe" => Ok(Self::Binary(binary_decoder(exchange))),
            _ => Err(IngesterError::UnsupportedStreamFormat {
                format: value.to_string(),
            }),
        }
    }

    fn accepts_text(&self) -> bool {
        matches!(self, Self::Json)
    }

    fn decode(&self, exchange: &str, frame: &[u8]) -> Result<ParsedMessage, IngesterError> {
        match self {
            Self::Json => JsonDecoder::new().decode(exchange, frame),
            Self::Binary(decoder) => decoder.decode(frame),
        }
    }
}

fn binary_decoder(exchange: &str) -> Arc<dyn BinaryDecoder> {
    // TODO: support additional exchange binary schemas.
    Arc::new(BinanceSbeDecoder::new(exchange))
}

impl FromStr for StreamFormat {
    type Err = IngesterError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::for_exchange(value, DEFAULT_BINARY_EXCHANGE)
    }
}

impl fmt::Debug for StreamFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json => formatter.write_str("Json"),
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
            (Self::Json, Self::Json) => true,
            (Self::Binary(left), Self::Binary(right)) => left.exchange() == right.exchange(),
            _ => false,
        }
    }
}

impl Eq for StreamFormat {}

/// Websocket transport shared by all exchange decoders.
pub struct WebsocketSource {
    exchange: String,
    url: String,
    api_key: Option<String>,
    stream_format: StreamFormat,
}

impl WebsocketSource {
    pub fn new(
        exchange: impl Into<String>,
        url: impl Into<String>,
        api_key: Option<String>,
        stream_format: StreamFormat,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            url: url.into(),
            api_key,
            stream_format,
        }
    }

    pub fn from_config(config: &IngesterConfig) -> Self {
        Self::new(
            config.exchange.clone(),
            config.websocket_url.clone(),
            config.websocket_api_key.clone(),
            config.stream_format.clone(),
        )
    }

    pub async fn run(&self, sender: mpsc::Sender<ParsedMessage>) -> BpResult<()> {
        loop {
            match self.run_connection(&sender).await {
                Ok(true) => warn!(exchange = %self.exchange, "exchange websocket closed"),
                Ok(false) => return Ok(()),
                Err(error) => warn!(exchange = %self.exchange, %error, "exchange websocket failed"),
            }
            sleep(RECONNECT_DELAY).await;
        }
    }

    async fn run_connection(&self, sender: &mpsc::Sender<ParsedMessage>) -> BpResult<bool> {
        let uri: Uri = self.url.parse().map_err(BpError::external)?;
        let mut builder = ClientBuilder::from_uri(uri);
        if let Some(api_key) = &self.api_key {
            let name = HeaderName::from_static("x-mbx-apikey");
            let value = HeaderValue::from_str(api_key).map_err(BpError::external)?;
            builder = builder.add_header(name, value).map_err(BpError::external)?;
        }
        let (mut websocket, _) = builder.connect().await.map_err(BpError::external)?;
        info!(exchange = %self.exchange, "connected to exchange websocket");

        while let Some(message) = websocket.next().await {
            let message = message.map_err(BpError::external)?;
            if message.is_ping() {
                let payload = message.into_payload();
                websocket
                    .send(Message::pong(payload))
                    .await
                    .map_err(BpError::external)?;
                continue;
            }
            if message.is_pong() {
                continue;
            }
            if message.is_close() {
                return Ok(true);
            }

            let frame = if message.is_binary() {
                message.into_payload().to_vec()
            } else if message.is_text() {
                if !self.stream_format.accepts_text() {
                    debug!(exchange = %self.exchange, "ignoring text control frame");
                    continue;
                }
                message.into_payload().to_vec()
            } else {
                continue;
            };

            match self.stream_format.decode(&self.exchange, &frame) {
                Ok(parsed) => {
                    if sender.send(parsed).await.is_err() {
                        return Ok(false);
                    }
                }
                Err(error) => {
                    warn!(exchange = %self.exchange, %error, "discarding undecodable frame");
                }
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_json_stream_format() {
        assert_eq!(
            " json ".parse::<StreamFormat>().unwrap(),
            StreamFormat::Json
        );
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
