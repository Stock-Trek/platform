use crate::publisher::EventPublisher;
use async_trait::async_trait;
use platform_errors::{BpError, BpResult, ValidationError};
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::Mutex,
};
use url::Url;

const DEFAULT_NATS_PORT: u16 = 4222;

pub(crate) fn validate_url(value: &str) -> BpResult<()> {
    NatsAddress::parse(value).map(|_| ())
}

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
        let username = (!url.username().is_empty()).then(|| url.username().to_string());
        let password = url.password().map(str::to_string);
        Ok(Self {
            host: host.to_string(),
            port: url.port().unwrap_or(DEFAULT_NATS_PORT),
            username,
            password,
        })
    }
}

pub struct NatsJetstreamPublisher {
    address: NatsAddress,
    name: String,
    connection: Mutex<Option<NatsConnection>>,
}

impl NatsJetstreamPublisher {
    pub async fn connect(url: &str, name: &str) -> BpResult<Self> {
        let address = NatsAddress::parse(url)?;
        let connection = NatsConnection::connect(&address, name).await?;
        Ok(Self {
            address,
            name: name.to_string(),
            connection: Mutex::new(Some(connection)),
        })
    }
}

#[async_trait]
impl EventPublisher for NatsJetstreamPublisher {
    async fn publish(&self, subject: &str, payload: &[u8]) -> BpResult<()> {
        validate_subject(subject)?;
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

struct NatsConnection {
    _reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl NatsConnection {
    async fn connect(address: &NatsAddress, name: &str) -> BpResult<Self> {
        let stream = TcpStream::connect((address.host.as_str(), address.port)).await?;
        let (read_half, write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        let info = read_protocol_line(&mut reader).await?;
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
            let line = read_protocol_line(&mut reader).await?;
            if line == "PONG" {
                break;
            }
            if line.starts_with("-ERR") {
                return Err(protocol_error(line));
            }
        }

        Ok(Self {
            _reader: reader,
            writer,
        })
    }

    // TODO: use JetStream publish acknowledgements and a deterministic message
    // id so a reconnect cannot duplicate or silently drop events.
    async fn publish(&mut self, subject: &str, payload: &[u8]) -> BpResult<()> {
        let header = format!("PUB {subject} {}\r\n", payload.len());
        self.writer.write_all(header.as_bytes()).await?;
        self.writer.write_all(payload).await?;
        self.writer.write_all(b"\r\n").await?;
        self.writer.flush().await?;
        Ok(())
    }
}

async fn read_protocol_line(reader: &mut BufReader<OwnedReadHalf>) -> BpResult<String> {
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Err(protocol_error("NATS connection closed during handshake"));
    }
    Ok(line.trim_end_matches(['\r', '\n']).to_string())
}

fn protocol_error(message: impl Into<String>) -> BpError {
    BpError::external(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.into(),
    ))
}

pub(crate) fn validate_subject(subject: &str) -> BpResult<()> {
    let invalid = subject.is_empty()
        || subject.split('.').any(|token| {
            token.is_empty()
                || token
                    .chars()
                    .any(|character| character.is_whitespace() || matches!(character, '>' | '*'))
        });
    if invalid {
        return Err(BpError::Validation(ValidationError::InvalidValue {
            name: "NATS subject".to_string(),
            value: subject.to_string(),
        }));
    }
    Ok(())
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
        assert!(validate_subject("market.events.binance.btcusdt.trade").is_ok());
        assert!(validate_subject("market..trade").is_err());
        assert!(validate_subject("market.*").is_err());
        assert!(validate_subject("market.>").is_err());
        assert!(validate_subject("market trade").is_err());
    }
}
