use crate::config::IngesterConfig;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use futures::{SinkExt, StreamExt};
use http::{HeaderName, HeaderValue, Uri};
use platform_errors::{BpError, BpResult, IngesterError};
use platform_types::new_types::TimestampMilliseconds;
use serde_json::{Map, Value};
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::sleep};
use tokio_websockets::{ClientBuilder, Message};
use tracing::{debug, info, warn};

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const BINANCE_SBE_SCHEMA_ID: u16 = 1;

const BINANCE_TRADES_TEMPLATE_ID: u16 = 10_000;
const BINANCE_BEST_BID_ASK_TEMPLATE_ID: u16 = 10_001;
const BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID: u16 = 10_002;
const BINANCE_DEPTH_DIFF_TEMPLATE_ID: u16 = 10_003;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedMessage {
    pub stream: String,
    pub sequence: Option<u64>,
    pub exchange_timestamp: Option<TimestampMilliseconds>,
    /// Base64 encoded raw frame from the exchange.
    pub payload: String,
}

/// Wire format used by an exchange websocket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamFormat {
    BinanceSbe,
    Json,
}

impl StreamFormat {
    pub fn decoder(self, exchange: &str) -> Arc<dyn StreamDecoder> {
        match self {
            Self::BinanceSbe => Arc::new(BinanceSbeDecoder::new(exchange)),
            Self::Json => Arc::new(JsonDecoder::new(exchange)),
        }
    }
}

impl FromStr for StreamFormat {
    type Err = IngesterError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "binance_sbe" | "binance-sbe" => Ok(Self::BinanceSbe),
            "json" => Ok(Self::Json),
            _ => Err(IngesterError::UnsupportedStreamFormat {
                format: value.to_string(),
            }),
        }
    }
}

/// Decodes a websocket frame into the exchange-independent ingester envelope.
///
/// Add an implementation for each exchange-specific wire format.
pub trait StreamDecoder: Send + Sync {
    fn exchange(&self) -> &str;

    /// Whether text frames should be passed to [`StreamDecoder::decode`].
    ///
    /// Binance SBE sends market data in binary frames and control messages as
    /// JSON text, so it ignores text frames.
    fn accepts_text(&self) -> bool;

    fn decode(&self, frame: &[u8]) -> Result<ParsedMessage, IngesterError>;
}

/// Decoder for Binance's SBE market data streams.
///
/// Schema: <https://github.com/binance/binance-spot-api-docs/blob/master/sbe/schemas/stream_1_0.xml>
pub struct BinanceSbeDecoder {
    exchange: String,
}

impl BinanceSbeDecoder {
    pub fn new(exchange: impl Into<String>) -> Self {
        Self {
            exchange: exchange.into(),
        }
    }

    fn decode_frame(&self, frame: &[u8]) -> Result<ParsedMessage, IngesterError> {
        let cursor = SbeCursor::new(frame, &self.exchange);
        let schema_id = cursor.read_u16(4)?;
        if schema_id != BINANCE_SBE_SCHEMA_ID {
            return Err(IngesterError::malformed_sbe(
                &self.exchange,
                format!("expected schema id {BINANCE_SBE_SCHEMA_ID}, got {schema_id}"),
            ));
        }

        match cursor.read_u16(2)? {
            BINANCE_TRADES_TEMPLATE_ID => self.trades_event(&cursor, frame),
            BINANCE_BEST_BID_ASK_TEMPLATE_ID => self.best_bid_ask_event(&cursor, frame),
            BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID => self.depth_snapshot_event(&cursor, frame),
            BINANCE_DEPTH_DIFF_TEMPLATE_ID => self.depth_diff_event(&cursor, frame),
            template_id => Err(IngesterError::unknown_sbe_template(
                &self.exchange,
                template_id,
            )),
        }
    }

    fn trades_event(
        &self,
        cursor: &SbeCursor<'_>,
        frame: &[u8],
    ) -> Result<ParsedMessage, IngesterError> {
        let event_time_us = cursor.read_i64(8)?;
        let (block_length, count, entries) = cursor.group_size32(26)?;
        let sequence = if count == 0 {
            None
        } else {
            let last_trade = cursor.advance(entries, count - 1, block_length)?;
            Some(cursor.read_sequence(last_trade)?)
        };
        let symbol_offset = cursor.advance(entries, count, block_length)?;
        let symbol = cursor.read_var_string(symbol_offset)?;
        Ok(self.parsed(frame, &symbol, "trade", sequence, Some(event_time_us)))
    }

    fn best_bid_ask_event(
        &self,
        cursor: &SbeCursor<'_>,
        frame: &[u8],
    ) -> Result<ParsedMessage, IngesterError> {
        let event_time_us = cursor.read_i64(8)?;
        let sequence = Some(cursor.read_sequence(16)?);
        let symbol = cursor.read_var_string(58)?;
        Ok(self.parsed(frame, &symbol, "bestBidAsk", sequence, Some(event_time_us)))
    }

    fn depth_snapshot_event(
        &self,
        cursor: &SbeCursor<'_>,
        frame: &[u8],
    ) -> Result<ParsedMessage, IngesterError> {
        let event_time_us = cursor.read_i64(8)?;
        let after_bids = cursor.end_of_group16(26)?;
        let after_asks = cursor.end_of_group16(after_bids)?;
        let symbol = cursor.read_var_string(after_asks)?;
        // TODO: snapshots are independent of the incremental update sequence.
        Ok(self.parsed(frame, &symbol, "depth20", None, Some(event_time_us)))
    }

    fn depth_diff_event(
        &self,
        cursor: &SbeCursor<'_>,
        frame: &[u8],
    ) -> Result<ParsedMessage, IngesterError> {
        let event_time_us = cursor.read_i64(8)?;
        let sequence = Some(cursor.read_sequence(24)?);
        let after_bids = cursor.end_of_group16(34)?;
        let after_asks = cursor.end_of_group16(after_bids)?;
        let symbol = cursor.read_var_string(after_asks)?;
        Ok(self.parsed(frame, &symbol, "depth", sequence, Some(event_time_us)))
    }

    fn parsed(
        &self,
        frame: &[u8],
        symbol: &str,
        suffix: &str,
        sequence: Option<u64>,
        event_time_us: Option<i64>,
    ) -> ParsedMessage {
        ParsedMessage {
            stream: format!("{}@{suffix}", symbol.to_ascii_lowercase()),
            sequence,
            exchange_timestamp: event_time_us
                .map(|microseconds| TimestampMilliseconds(microseconds / 1_000)),
            payload: BASE64.encode(frame),
        }
    }
}

impl StreamDecoder for BinanceSbeDecoder {
    fn exchange(&self) -> &str {
        &self.exchange
    }

    fn accepts_text(&self) -> bool {
        false
    }

    fn decode(&self, frame: &[u8]) -> Result<ParsedMessage, IngesterError> {
        self.decode_frame(frame)
    }
}

/// Generic JSON stream decoder.
///
/// TODO: add exchange-specific field mappings. This decoder supports the
/// Binance-style `stream`/`data`, `e`, `E`, `s` and sequence fields and can be
/// replaced per exchange without changing the transport or ingester.
pub struct JsonDecoder {
    exchange: String,
}

impl JsonDecoder {
    pub fn new(exchange: impl Into<String>) -> Self {
        Self {
            exchange: exchange.into(),
        }
    }

    fn decode_frame(&self, frame: &[u8]) -> Result<ParsedMessage, IngesterError> {
        let message = serde_json::from_slice(frame)
            .map_err(|source| IngesterError::json(&self.exchange, source))?;
        self.decode_value(message, frame)
    }

    fn decode_value(&self, message: Value, frame: &[u8]) -> Result<ParsedMessage, IngesterError> {
        let (stream, data) = match message {
            Value::Object(mut object) => match object.remove("stream") {
                Some(Value::String(stream)) => {
                    let data = object
                        .remove("data")
                        .ok_or(IngesterError::MissingField("data"))?;
                    (Some(stream), data)
                }
                Some(_) => return Err(IngesterError::InvalidField("stream")),
                None => (None, Value::Object(object)),
            },
            _ => return Err(IngesterError::NotAnObject),
        };

        let object = data.as_object().ok_or(IngesterError::PayloadNotAnObject)?;
        let event_type = object.get("e").and_then(Value::as_str);
        let stream = match stream {
            Some(stream) => stream,
            None => {
                let event_type = event_type.ok_or(IngesterError::MissingField("e"))?;
                let symbol = object
                    .get("s")
                    .and_then(Value::as_str)
                    .ok_or(IngesterError::MissingField("s"))?;
                format!("{}@{event_type}", symbol.to_ascii_lowercase())
            }
        };
        let sequence = Self::parse_sequence(object, event_type)?;
        let exchange_timestamp = object
            .get("E")
            .and_then(Value::as_i64)
            .map(TimestampMilliseconds);

        Ok(ParsedMessage {
            stream,
            sequence,
            exchange_timestamp,
            payload: BASE64.encode(frame),
        })
    }

    fn parse_sequence(
        object: &Map<String, Value>,
        event_type: Option<&str>,
    ) -> Result<Option<u64>, IngesterError> {
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
            .ok_or(IngesterError::MissingSequence { field })
    }
}

impl StreamDecoder for JsonDecoder {
    fn exchange(&self) -> &str {
        &self.exchange
    }

    fn accepts_text(&self) -> bool {
        true
    }

    fn decode(&self, frame: &[u8]) -> Result<ParsedMessage, IngesterError> {
        self.decode_frame(frame)
    }
}

struct SbeCursor<'a> {
    frame: &'a [u8],
    exchange: &'a str,
}

impl<'a> SbeCursor<'a> {
    fn new(frame: &'a [u8], exchange: &'a str) -> Self {
        Self { frame, exchange }
    }

    fn get(&self, offset: usize, length: usize) -> Result<&'a [u8], IngesterError> {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| IngesterError::malformed_sbe(self.exchange, "frame offset overflow"))?;
        self.frame.get(offset..end).ok_or_else(|| {
            IngesterError::malformed_sbe(
                self.exchange,
                format!("frame is too short for {length} bytes at offset {offset}"),
            )
        })
    }

    fn read_u8(&self, offset: usize) -> Result<u8, IngesterError> {
        Ok(self.get(offset, 1)?[0])
    }

    fn read_u16(&self, offset: usize) -> Result<u16, IngesterError> {
        let bytes: [u8; 2] = self
            .get(offset, 2)?
            .try_into()
            .map_err(|_| IngesterError::malformed_sbe(self.exchange, "invalid u16"))?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u32(&self, offset: usize) -> Result<u32, IngesterError> {
        let bytes: [u8; 4] = self
            .get(offset, 4)?
            .try_into()
            .map_err(|_| IngesterError::malformed_sbe(self.exchange, "invalid u32"))?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_i64(&self, offset: usize) -> Result<i64, IngesterError> {
        let bytes: [u8; 8] = self
            .get(offset, 8)?
            .try_into()
            .map_err(|_| IngesterError::malformed_sbe(self.exchange, "invalid i64"))?;
        Ok(i64::from_le_bytes(bytes))
    }

    fn read_sequence(&self, offset: usize) -> Result<u64, IngesterError> {
        let value = self.read_i64(offset)?;
        u64::try_from(value).map_err(|_| {
            IngesterError::malformed_sbe(
                self.exchange,
                format!("negative sequence {value} at offset {offset}"),
            )
        })
    }

    fn read_var_string(&self, offset: usize) -> Result<String, IngesterError> {
        let length = self.read_u8(offset)? as usize;
        let start = offset
            .checked_add(1)
            .ok_or_else(|| IngesterError::malformed_sbe(self.exchange, "string offset overflow"))?;
        let bytes = self.get(start, length)?;
        let value = std::str::from_utf8(bytes)
            .map_err(|source| IngesterError::utf8(self.exchange, source))?;
        Ok(value.to_string())
    }

    fn group_size32(&self, offset: usize) -> Result<(usize, usize, usize), IngesterError> {
        let block_length = self.read_u16(offset)? as usize;
        let count = self.read_u32(offset + 2)? as usize;
        let entries = offset
            .checked_add(6)
            .ok_or_else(|| IngesterError::malformed_sbe(self.exchange, "group offset overflow"))?;
        Ok((block_length, count, entries))
    }

    fn end_of_group16(&self, offset: usize) -> Result<usize, IngesterError> {
        let block_length = self.read_u16(offset)? as usize;
        let count = self.read_u16(offset + 2)? as usize;
        let entries = offset
            .checked_add(4)
            .ok_or_else(|| IngesterError::malformed_sbe(self.exchange, "group offset overflow"))?;
        self.advance(entries, count, block_length)
    }

    fn advance(&self, base: usize, count: usize, stride: usize) -> Result<usize, IngesterError> {
        let length = count
            .checked_mul(stride)
            .and_then(|length| base.checked_add(length))
            .ok_or_else(|| IngesterError::malformed_sbe(self.exchange, "group size overflow"))?;
        if length > self.frame.len() {
            return Err(IngesterError::malformed_sbe(
                self.exchange,
                format!(
                    "group ends at {length} beyond frame length {}",
                    self.frame.len()
                ),
            ));
        }
        Ok(length)
    }
}

/// Websocket transport shared by all exchange decoders.
pub struct WebsocketSource {
    exchange: String,
    url: String,
    api_key: Option<String>,
    decoder: Arc<dyn StreamDecoder>,
}

impl WebsocketSource {
    pub fn new(
        exchange: impl Into<String>,
        url: impl Into<String>,
        api_key: Option<String>,
        decoder: Arc<dyn StreamDecoder>,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            url: url.into(),
            api_key,
            decoder,
        }
    }

    pub fn from_config(config: &IngesterConfig) -> Self {
        Self::new(
            config.exchange.clone(),
            config.websocket_url.clone(),
            config.websocket_api_key.clone(),
            config.stream_format.decoder(&config.exchange),
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
                if !self.decoder.accepts_text() {
                    debug!(exchange = %self.exchange, "ignoring text control frame");
                    continue;
                }
                message.into_payload().to_vec()
            } else {
                continue;
            };

            match self.decoder.decode(&frame) {
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
    use serde_json::json;

    fn sbe_header(template_id: u16) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&18u16.to_le_bytes());
        frame.extend_from_slice(&template_id.to_le_bytes());
        frame.extend_from_slice(&BINANCE_SBE_SCHEMA_ID.to_le_bytes());
        frame.extend_from_slice(&0u16.to_le_bytes());
        frame
    }

    fn append_var_string(frame: &mut Vec<u8>, value: &str) {
        frame.push(value.len() as u8);
        frame.extend_from_slice(value.as_bytes());
    }

    fn sbe_trades_frame() -> Vec<u8> {
        let event_time_us = 1_700_000_000_123_000_i64;
        let mut frame = sbe_header(BINANCE_TRADES_TEMPLATE_ID);
        frame.extend_from_slice(&event_time_us.to_le_bytes());
        frame.extend_from_slice(&event_time_us.to_le_bytes());
        frame.push((-8_i8) as u8);
        frame.push((-8_i8) as u8);
        frame.extend_from_slice(&25u16.to_le_bytes());
        frame.extend_from_slice(&2u32.to_le_bytes());
        for trade_id in [42_i64, 43_i64] {
            frame.extend_from_slice(&trade_id.to_le_bytes());
            frame.extend_from_slice(&100_i64.to_le_bytes());
            frame.extend_from_slice(&1_i64.to_le_bytes());
            frame.push(0);
        }
        append_var_string(&mut frame, "btcusdt");
        frame
    }

    fn sbe_best_bid_ask_frame() -> Vec<u8> {
        let mut frame = sbe_header(BINANCE_BEST_BID_ASK_TEMPLATE_ID);
        frame.extend_from_slice(&1_700_000_000_123_000_i64.to_le_bytes());
        frame.extend_from_slice(&99_i64.to_le_bytes());
        frame.push(0);
        frame.push(0);
        frame.extend_from_slice(&[0u8; 32]);
        append_var_string(&mut frame, "btcusdt");
        frame
    }

    fn sbe_depth_diff_frame() -> Vec<u8> {
        let mut frame = sbe_header(BINANCE_DEPTH_DIFF_TEMPLATE_ID);
        frame.extend_from_slice(&1_700_000_000_123_000_i64.to_le_bytes());
        frame.extend_from_slice(&100_i64.to_le_bytes());
        frame.extend_from_slice(&123_i64.to_le_bytes());
        frame.push(0);
        frame.push(0);
        for _ in 0..2 {
            frame.extend_from_slice(&16u16.to_le_bytes());
            frame.extend_from_slice(&1u16.to_le_bytes());
            frame.extend_from_slice(&[0u8; 16]);
        }
        append_var_string(&mut frame, "btcusdt");
        frame
    }

    #[test]
    fn decodes_binance_sbe_trades() {
        let frame = sbe_trades_frame();
        let decoder = BinanceSbeDecoder::new("binance");

        let parsed = decoder.decode(&frame).unwrap();

        assert_eq!(parsed.stream, "btcusdt@trade");
        assert_eq!(parsed.sequence, Some(43));
        assert_eq!(
            parsed.exchange_timestamp,
            Some(TimestampMilliseconds(1_700_000_000_123))
        );
        assert_eq!(BASE64.decode(&parsed.payload).unwrap(), frame);
    }

    #[test]
    fn decodes_binance_sbe_best_bid_ask() {
        let frame = sbe_best_bid_ask_frame();
        let decoder = BinanceSbeDecoder::new("binance");

        let parsed = decoder.decode(&frame).unwrap();

        assert_eq!(parsed.stream, "btcusdt@bestBidAsk");
        assert_eq!(parsed.sequence, Some(99));
    }

    #[test]
    fn decodes_binance_sbe_depth_diff_and_skips_groups() {
        let frame = sbe_depth_diff_frame();
        let decoder = BinanceSbeDecoder::new("binance");

        let parsed = decoder.decode(&frame).unwrap();

        assert_eq!(parsed.stream, "btcusdt@depth");
        assert_eq!(parsed.sequence, Some(123));
    }

    #[test]
    fn rejects_unknown_sbe_templates() {
        let frame = sbe_header(1);
        let decoder = BinanceSbeDecoder::new("binance");

        let error = decoder.decode(&frame).unwrap_err();

        assert!(matches!(
            error,
            IngesterError::UnknownSbeTemplate { template_id: 1, .. }
        ));
    }

    #[test]
    fn parses_combined_json_streams() {
        let frame = serde_json::to_vec(&json!({
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
        let decoder = JsonDecoder::new("binance");

        let parsed = decoder.decode(&frame).unwrap();

        assert_eq!(parsed.stream, "btcusdt@trade");
        assert_eq!(parsed.sequence, Some(42));
        assert_eq!(
            parsed.exchange_timestamp,
            Some(TimestampMilliseconds(1_700_000_000_123))
        );
    }

    #[test]
    fn derives_a_stream_name_for_raw_json_messages() {
        let frame = serde_json::to_vec(&json!({
            "e": "depthUpdate",
            "E": 1_700_000_000_123_i64,
            "s": "BTCUSDT",
            "u": 99
        }))
        .unwrap();
        let decoder = JsonDecoder::new("binance");

        let parsed = decoder.decode(&frame).unwrap();

        assert_eq!(parsed.stream, "btcusdt@depthUpdate");
        assert_eq!(parsed.sequence, Some(99));
    }

    #[test]
    fn json_events_without_a_sequence_are_accepted() {
        let frame = serde_json::to_vec(&json!({
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "E": 1_700_000_000_123_i64,
                "s": "BTCUSDT"
            }
        }))
        .unwrap();
        let decoder = JsonDecoder::new("binance");

        let parsed = decoder.decode(&frame).unwrap();

        assert_eq!(parsed.sequence, None);
    }

    #[test]
    fn sequenced_json_events_must_include_their_sequence() {
        let frame = serde_json::to_vec(&json!({
            "stream": "btcusdt@trade",
            "data": {
                "e": "trade",
                "s": "BTCUSDT"
            }
        }))
        .unwrap();
        let decoder = JsonDecoder::new("binance");

        let error = decoder.decode(&frame).unwrap_err();

        assert!(matches!(
            error,
            IngesterError::MissingSequence { field: "t" }
        ));
    }
}
