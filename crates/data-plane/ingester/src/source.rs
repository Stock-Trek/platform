use async_trait::async_trait;
use iris::{Config, DisconnectedBehavior, Listener, ServerCloseBehavior};
use platform_types::new_types::TimestampMilliseconds;
use serde_json::{Map, Value};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{error, warn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedMessage {
    pub stream: String,
    pub sequence: Option<u64>,
    pub exchange_timestamp: Option<TimestampMilliseconds>,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ParseError {
    #[error("expected a JSON object")]
    NotAnObject,
    #[error("expected message payload to be a JSON object")]
    PayloadNotAnObject,
    #[error("missing field {0}")]
    MissingField(&'static str),
    #[error("field {0} has the wrong type")]
    InvalidField(&'static str),
    #[error("sequenced event is missing sequence field {field}")]
    MissingSequence { field: &'static str },
}

pub fn parse_binance_message(message: Value) -> Result<ParsedMessage, ParseError> {
    let (stream, data) = match message {
        Value::Object(mut object) => match object.remove("stream") {
            Some(Value::String(stream)) => {
                let data = object
                    .remove("data")
                    .ok_or(ParseError::MissingField("data"))?;
                (Some(stream), data)
            }
            Some(_) => return Err(ParseError::InvalidField("stream")),
            None => (None, Value::Object(object)),
        },
        _ => return Err(ParseError::NotAnObject),
    };

    let object = data.as_object().ok_or(ParseError::PayloadNotAnObject)?;
    let event_type = object.get("e").and_then(Value::as_str);
    let stream = match stream {
        Some(stream) => stream,
        None => {
            let event_type = event_type.ok_or(ParseError::MissingField("e"))?;
            let symbol = object
                .get("s")
                .and_then(Value::as_str)
                .ok_or(ParseError::MissingField("s"))?;
            format!("{}@{event_type}", symbol.to_ascii_lowercase())
        }
    };
    let sequence = parse_sequence(object, event_type)?;
    let exchange_timestamp = object
        .get("E")
        .and_then(Value::as_i64)
        .map(TimestampMilliseconds);

    Ok(ParsedMessage {
        stream,
        sequence,
        exchange_timestamp,
        payload: data,
    })
}

fn parse_sequence(
    object: &Map<String, Value>,
    event_type: Option<&str>,
) -> Result<Option<u64>, ParseError> {
    // TODO: validate depth updates using the `U`/`pu` range. A single depth
    // event can span many update ids, so `u` alone is not always contiguous.
    let field = match event_type {
        Some("trade") => Some("t"),
        Some("aggTrade") => Some("a"),
        Some("depthUpdate" | "bookTicker") => Some("u"),
        _ => None,
    };
    let Some(field) = field else {
        return Ok(None);
    };
    object
        .get(field)
        .and_then(Value::as_u64)
        .map(Some)
        .ok_or(ParseError::MissingSequence { field })
}

#[derive(Debug)]
pub struct ChannelListener {
    sender: mpsc::Sender<Value>,
}

impl ChannelListener {
    pub fn new(sender: mpsc::Sender<Value>) -> Self {
        Self { sender }
    }
}

#[async_trait]
impl Listener<Value> for ChannelListener {
    async fn on_message(&self, message: Value) {
        if self.sender.send(message).await.is_err() {
            warn!("ingester message channel closed");
        }
    }

    async fn on_error(&self, error: &iris::ConnectionError) {
        error!(%error, "binance websocket error");
    }

    async fn on_disconnected(&self) {
        warn!("binance websocket disconnected");
    }
}

pub fn binance_client(url: &str, sender: mpsc::Sender<Value>) -> iris::Client<String, Value> {
    let listener = Arc::new(ChannelListener::new(sender));
    let config = Config::new()
        .with_disconnected_behavior(DisconnectedBehavior::DropAllQueued)
        .with_server_close_behavior(ServerCloseBehavior::Reconnect);
    iris::Client::new(config, listener, url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_combined_trade_stream() {
        let parsed = parse_binance_message(json!({
            "stream": "btcusdt@trade",
            "data": {
                "e": "trade",
                "E": 1_700_000_000_123_i64,
                "s": "BTCUSDT",
                "t": 42,
                "p": "100.5",
                "q": "1"
            }
        }))
        .unwrap();

        assert_eq!(parsed.stream, "btcusdt@trade");
        assert_eq!(parsed.sequence, Some(42));
        assert_eq!(
            parsed.exchange_timestamp,
            Some(TimestampMilliseconds(1_700_000_000_123))
        );
        assert_eq!(parsed.payload["p"], "100.5");
    }

    #[test]
    fn derives_a_stream_name_for_raw_messages() {
        let parsed = parse_binance_message(json!({
            "e": "depthUpdate",
            "E": 1_700_000_000_123_i64,
            "s": "BTCUSDT",
            "u": 99
        }))
        .unwrap();

        assert_eq!(parsed.stream, "btcusdt@depthUpdate");
        assert_eq!(parsed.sequence, Some(99));
    }

    #[test]
    fn events_without_a_sequence_are_accepted() {
        let parsed = parse_binance_message(json!({
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "E": 1_700_000_000_123_i64,
                "s": "BTCUSDT"
            }
        }))
        .unwrap();

        assert_eq!(parsed.sequence, None);
    }

    #[test]
    fn sequenced_events_must_include_their_sequence() {
        let error = parse_binance_message(json!({
            "stream": "btcusdt@trade",
            "data": {
                "e": "trade",
                "s": "BTCUSDT"
            }
        }))
        .unwrap_err();

        assert_eq!(error, ParseError::MissingSequence { field: "t" });
    }
}
