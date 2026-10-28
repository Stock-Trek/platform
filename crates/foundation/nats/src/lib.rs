//! Minimal NATS client shared by the market data services.

use platform_errors::{BpError, BpResult, IngesterError, ValidationError};
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
pub struct NatsAddress {
    host: String,
    port: u16,
    username: Option<String>,
    password: Option<String>,
}

impl NatsAddress {
    pub fn validate(value: &str) -> BpResult<()> {
        Self::validate_named("NATS_URL", value)
    }

    /// Validates a NATS address, reporting errors against `name` so callers can
    /// use the environment variable that actually held the value.
    pub fn validate_named(name: &str, value: &str) -> BpResult<()> {
        Self::parse_named(name, value).map(|_| ())
    }

    fn parse(value: &str) -> BpResult<Self> {
        Self::parse_named("NATS_URL", value)
    }

    fn parse_named(name: &str, value: &str) -> BpResult<Self> {
        let url = Url::parse(value).map_err(|error| BpError::config_invalid_source(name, error))?;
        if url.scheme() != "nats" {
            return Err(BpError::config_invalid(
                name,
                format!("must use the nats scheme, got {}", url.scheme()),
            ));
        }
        let host = url
            .host_str()
            .ok_or_else(|| BpError::config_invalid(name, "missing host"))?;
        Ok(Self {
            host: host.to_string(),
            port: url.port().unwrap_or(DEFAULT_NATS_PORT),
            username: (!url.username().is_empty()).then(|| url.username().to_string()),
            password: url.password().map(str::to_string),
        })
    }
}

pub struct NatsSubject;

impl NatsSubject {
    /// Converts an arbitrary value into a safe single-token subject segment.
    pub fn normalize_segment(value: &str) -> String {
        value
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                    character.to_ascii_lowercase()
                } else {
                    '.'
                }
            })
            .collect()
    }

    pub fn validate(subject: &str) -> BpResult<()> {
        Self::validate_named("NATS subject", subject)
    }

    /// Validates a NATS subject, reporting errors against `name` so callers can
    /// identify the configuration value that was invalid.
    pub fn validate_named(name: &str, subject: &str) -> BpResult<()> {
        let invalid = subject.is_empty()
            || subject.split('.').any(|token| {
                token.is_empty()
                    || token.chars().any(|character| {
                        character.is_whitespace() || matches!(character, '>' | '*')
                    })
            });
        if invalid {
            return Err(BpError::Validation(ValidationError::InvalidValue {
                name: name.to_string(),
                value: subject.to_string(),
            }));
        }
        Ok(())
    }
}

/// Reconnecting publisher for core NATS subjects.
pub struct NatsPublisher {
    address: NatsAddress,
    name: String,
    connection: Mutex<Option<NatsConnection>>,
}

impl NatsPublisher {
    pub async fn connect(url: &str, name: &str) -> BpResult<Self> {
        let address = NatsAddress::parse(url)?;
        let connection = NatsConnection::connect(&address, name).await?;
        Ok(Self {
            address,
            name: name.to_string(),
            connection: Mutex::new(Some(connection)),
        })
    }

    // TODO: use JetStream publish acknowledgements and a deterministic message
    // id so a reconnect cannot duplicate or silently drop events.
    pub async fn publish(&self, subject: &str, payload: &[u8]) -> BpResult<()> {
        NatsSubject::validate(subject)?;
        let mut connection = self.connection.lock().await;
        if connection.is_none() {
            *connection = Some(NatsConnection::connect(&self.address, &self.name).await?);
        }
        let result = match connection.as_mut() {
            Some(connection) => connection.publish(subject, payload).await,
            None => unreachable!("NATS connection was initialised above"),
        };
        if result.is_err() {
            *connection = None;
        }
        result
    }
}

/// Subscription that yields the raw subject and payload for each message.
pub struct NatsSubscriber {
    connection: NatsConnection,
}

impl NatsSubscriber {
    pub async fn connect(url: &str, name: &str, subject: &str, sid: &str) -> BpResult<Self> {
        let address = NatsAddress::parse(url)?;
        let mut connection = NatsConnection::connect(&address, name).await?;
        connection.subscribe(subject, sid).await?;
        Ok(Self { connection })
    }

    pub async fn next_message(&mut self) -> BpResult<Option<(String, Vec<u8>)>> {
        // TODO: reconnect and resubscribe if the connection drops.
        self.connection.next_message().await
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
        let info = Self::read_protocol_line(&mut reader)
            .await?
            .ok_or_else(|| Self::protocol_error("NATS connection closed during handshake"))?;
        if !info.starts_with("INFO ") {
            return Err(Self::protocol_error(format!(
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
            let line = Self::read_protocol_line(&mut reader)
                .await?
                .ok_or_else(|| Self::protocol_error("NATS connection closed during handshake"))?;
            if line == "PONG" {
                break;
            }
            if line.starts_with("-ERR") {
                return Err(Self::protocol_error(line));
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
            let Some(line) = Self::read_protocol_line(&mut self.reader).await? else {
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
                    return Err(Self::protocol_error(format!("malformed MSG line {line:?}")));
                }
                let subject = tokens[1].to_string();
                // MSG <subject> <sid> [reply-to] <#bytes>
                let size = tokens
                    .last()
                    .and_then(|size| size.parse::<usize>().ok())
                    .ok_or_else(|| {
                        Self::protocol_error(format!("malformed MSG size in {line:?}"))
                    })?;
                let mut payload = vec![0_u8; size];
                self.reader.read_exact(&mut payload).await?;
                let mut terminator = [0_u8; 2];
                self.reader.read_exact(&mut terminator).await?;
                return Ok(Some((subject, payload)));
            }
            if line.starts_with("-ERR") {
                return Err(Self::protocol_error(line));
            }
            if line.is_empty()
                || line.starts_with("+OK")
                || line.starts_with("INFO")
                || line == "PONG"
            {
                continue;
            }
            // TODO: support header messages and reconnects.
            return Err(Self::protocol_error(format!(
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

    #[test]
    fn rejects_publish_subjects_with_wildcards_or_empty_tokens() {
        assert!(NatsSubject::validate("market.events.binance.btcusdt.trade").is_ok());
        assert!(NatsSubject::validate("market..trade").is_err());
        assert!(NatsSubject::validate("market.*").is_err());
        assert!(NatsSubject::validate("market.>").is_err());
        assert!(NatsSubject::validate("market trade").is_err());
    }
}
