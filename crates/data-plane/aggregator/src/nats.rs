use crate::{
    aggregator::ClosedCandle,
    candle::Candle,
    config::AggregatorConfig,
    sink::{ClosedCandlePublisher, OpenCandleStore},
    source::MarketEventSource,
};
use async_trait::async_trait;
use platform_errors::{BpError, BpResult, IngesterError};
use platform_market_data::event::MarketEvent;
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::Mutex,
};
use url::Url;

const DEFAULT_NATS_PORT: u16 = 4222;

#[derive(Debug, Clone)]
struct NatsAddress {
    host: String,
    port: u16,
    username: Option<String>,
    password: Option<String>,
}

impl NatsAddress {
    fn parse(value: &str) -> BpResult<Self> {
        let url =
            Url::parse(value).map_err(|error| BpError::config_invalid_source("NATS_URL", error))?;
        if url.scheme() != "nats" {
            return Err(BpError::config_invalid(
                "NATS_URL",
                format!("must use the nats scheme, got {}", url.scheme()),
            ));
        }
        let host = url
            .host_str()
            .ok_or_else(|| BpError::config_invalid("NATS_URL", "missing host"))?;
        Ok(Self {
            host: host.to_string(),
            port: url.port().unwrap_or(DEFAULT_NATS_PORT),
            username: (!url.username().is_empty()).then(|| url.username().to_string()),
            password: url.password().map(str::to_string),
        })
    }
}

/// Reads ingester events from a NATS wildcard subscription.
pub struct NatsMarketEventSource {
    connection: NatsConnection,
}

impl NatsMarketEventSource {
    pub async fn connect(config: &AggregatorConfig) -> BpResult<Self> {
        let address = NatsAddress::parse(&config.nats_url)?;
        let mut connection =
            NatsConnection::connect(&address, "platform-aggregator-events").await?;
        connection
            .subscribe(&format!("{}.>", config.event_subject_prefix), "1")
            .await?;
        Ok(Self { connection })
    }
}

#[async_trait]
impl MarketEventSource for NatsMarketEventSource {
    async fn next_event(&mut self) -> BpResult<Option<MarketEvent>> {
        let Some((_subject, payload)) = self.connection.next_message().await? else {
            return Ok(None);
        };
        let event = serde_json::from_slice(&payload).map_err(IngesterError::json)?;
        Ok(Some(event))
    }
}

/// Publishes open and closed candles to NATS subjects.
///
/// TODO: open candles should be written to a NATS KV bucket, and closed candles
/// should use JetStream acknowledgements with a durable message id. Core NATS
/// publishing is used here as a first-cut transport.
pub struct NatsCandleSink {
    connection: Mutex<NatsConnection>,
    open_subject_prefix: String,
    closed_subject_prefix: String,
}

impl NatsCandleSink {
    pub async fn connect(config: &AggregatorConfig) -> BpResult<Self> {
        let address = NatsAddress::parse(&config.nats_url)?;
        let connection = NatsConnection::connect(&address, "platform-aggregator-candles").await?;
        Ok(Self {
            connection: Mutex::new(connection),
            open_subject_prefix: config.open_candle_subject_prefix.clone(),
            closed_subject_prefix: config.closed_candle_subject_prefix.clone(),
        })
    }

    async fn publish(&self, subject: &str, payload: &[u8]) -> BpResult<()> {
        let mut connection = self.connection.lock().await;
        // TODO: reconnect and replay unwritten updates if the connection drops.
        connection.publish(subject, payload).await
    }

    fn subject(prefix: &str, suffix: &str) -> String {
        format!("{prefix}.{suffix}")
    }
}

#[async_trait]
impl OpenCandleStore for NatsCandleSink {
    async fn update_open(&self, candles: &[Candle]) -> BpResult<()> {
        for candle in candles {
            let payload = serde_json::to_vec(candle).map_err(IngesterError::json)?;
            let subject = Self::subject(&self.open_subject_prefix, &candle.key.subject_suffix());
            self.publish(&subject, &payload).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ClosedCandlePublisher for NatsCandleSink {
    async fn publish_closed(&self, candles: &[ClosedCandle]) -> BpResult<()> {
        for closed in candles {
            let payload = serde_json::to_vec(closed).map_err(IngesterError::json)?;
            let subject = Self::subject(
                &self.closed_subject_prefix,
                &closed.candle.key.subject_suffix(),
            );
            self.publish(&subject, &payload).await?;
        }
        Ok(())
    }
}

struct NatsConnection {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl NatsConnection {
    async fn connect(address: &NatsAddress, name: &str) -> BpResult<Self> {
        let stream = TcpStream::connect((address.host.as_str(), address.port)).await?;
        let (read_half, write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        let info = read_protocol_line(&mut reader)
            .await?
            .ok_or_else(|| protocol_error("NATS connection closed during handshake"))?;
        if !info.starts_with("INFO ") {
            return Err(protocol_error(format!(
                "expected INFO from NATS server, got {info:?}"
            )));
        }

        let mut connect = Map::new();
        connect.insert("verbose".into(), Value::Bool(false));
        connect.insert("pedantic".into(), Value::Bool(false));
        connect.insert("tls_required".into(), Value::Bool(false));
        connect.insert("name".into(), Value::String(name.to_string()));
        connect.insert("lang".into(), Value::String("rust".to_string()));
        connect.insert(
            "version".into(),
            Value::String(env!("CARGO_PKG_VERSION").to_string()),
        );
        connect.insert("protocol".into(), json!(1));
        if let Some(username) = &address.username {
            connect.insert("user".into(), Value::String(username.clone()));
        }
        if let Some(password) = &address.password {
            connect.insert("pass".into(), Value::String(password.clone()));
        }

        let mut writer = write_half;
        writer
            .write_all(Value::Object(connect).to_string().as_bytes())
            .await?;
        writer.write_all(b"\r\nPING\r\n").await?;
        writer.flush().await?;

        loop {
            let line = read_protocol_line(&mut reader)
                .await?
                .ok_or_else(|| protocol_error("NATS connection closed during handshake"))?;
            if line == "PONG" {
                break;
            }
            if line.starts_with("-ERR") {
                return Err(protocol_error(line));
            }
        }

        Ok(Self { reader, writer })
    }

    async fn subscribe(&mut self, subject: &str, sid: &str) -> BpResult<()> {
        let command = format!("SUB {subject} {sid}\r\n");
        self.writer.write_all(command.as_bytes()).await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn next_message(&mut self) -> BpResult<Option<(String, Vec<u8>)>> {
        loop {
            let Some(line) = read_protocol_line(&mut self.reader).await? else {
                return Ok(None);
            };
            if line == "PING" {
                self.writer.write_all(b"PONG\r\n").await?;
                self.writer.flush().await?;
                continue;
            }
            if line.starts_with("MSG ") {
                let tokens: Vec<&str> = line.split_whitespace().collect();
                if tokens.len() < 4 {
                    return Err(protocol_error(format!("malformed MSG line {line:?}")));
                }
                let subject = tokens[1].to_string();
                // MSG <subject> <sid> [reply-to] <#bytes>
                let size = tokens
                    .last()
                    .and_then(|size| size.parse::<usize>().ok())
                    .ok_or_else(|| protocol_error(format!("malformed MSG size in {line:?}")))?;
                let mut payload = vec![0_u8; size];
                self.reader.read_exact(&mut payload).await?;
                let mut terminator = [0_u8; 2];
                self.reader.read_exact(&mut terminator).await?;
                return Ok(Some((subject, payload)));
            }
            if line.starts_with("-ERR") {
                return Err(protocol_error(line));
            }
            if line.is_empty()
                || line.starts_with("+OK")
                || line.starts_with("INFO")
                || line == "PONG"
            {
                continue;
            }
            // TODO: support header messages and reconnects.
            return Err(protocol_error(format!(
                "unsupported NATS protocol message {line:?}"
            )));
        }
    }

    async fn publish(&mut self, subject: &str, payload: &[u8]) -> BpResult<()> {
        let header = format!("PUB {subject} {}\r\n", payload.len());
        self.writer.write_all(header.as_bytes()).await?;
        self.writer.write_all(payload).await?;
        self.writer.write_all(b"\r\n").await?;
        self.writer.flush().await?;
        Ok(())
    }
}

async fn read_protocol_line(reader: &mut BufReader<OwnedReadHalf>) -> BpResult<Option<String>> {
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(None);
    }
    Ok(Some(line.trim_end_matches(['\r', '\n']).to_string()))
}

fn protocol_error(message: impl Into<String>) -> BpError {
    BpError::Ingester(IngesterError::protocol(message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nats_url_with_auth_and_default_port() {
        let address = NatsAddress::parse("nats://user:secret@localhost").unwrap();

        assert_eq!(address.host, "localhost");
        assert_eq!(address.port, DEFAULT_NATS_PORT);
        assert_eq!(address.username.as_deref(), Some("user"));
        assert_eq!(address.password.as_deref(), Some("secret"));
    }

    #[test]
    fn rejects_non_nats_urls() {
        assert!(NatsAddress::parse("http://localhost:4222").is_err());
    }
}
