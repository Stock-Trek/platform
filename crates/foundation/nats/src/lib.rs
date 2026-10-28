//! Minimal NATS client shared by the market data services.
//!
//! Publishes and subscriptions run over JetStream so that events survive
//! reconnects and restarts, with a core-NATS fallback in [`NatsSubscriber`].

mod connection;
mod consumer;
mod jetstream;
mod kv;
mod message;
mod publisher;

pub use consumer::JetStreamConsumer;
pub use jetstream::{kv_bucket_name, stream_name};
pub use kv::KeyValue;
pub use message::Message;
pub use publisher::{NatsPublisher, PublishAck};

use platform_errors::{BpError, BpResult, ValidationError};
use tokio::{net::TcpStream, sync::Mutex};
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

    pub(crate) fn parse(value: &str) -> BpResult<Self> {
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

impl NatsAddress {
    pub(crate) async fn tcp_connect(&self) -> BpResult<TcpStream> {
        Ok(TcpStream::connect((self.host.as_str(), self.port)).await?)
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

/// Subscription that yields the raw subject and payload for each message,
/// reconnecting and resubscribing when the connection drops.
pub struct NatsSubscriber {
    address: NatsAddress,
    name: String,
    subject: String,
    connection: Mutex<Option<connection::NatsConnection>>,
}

impl NatsSubscriber {
    pub async fn connect(url: &str, name: &str, subject: &str) -> BpResult<Self> {
        let address = NatsAddress::parse(url)?;
        NatsSubject::validate(subject)?;
        let mut connection = connection::NatsConnection::connect(&address, name).await?;
        connection.subscribe(subject, 1).await?;
        Ok(Self {
            address,
            name: name.to_string(),
            subject: subject.to_string(),
            connection: Mutex::new(Some(connection)),
        })
    }

    pub async fn next_message(&mut self) -> BpResult<Option<(String, Vec<u8>)>> {
        loop {
            let result = {
                let mut guard = self.connection.lock().await;
                match guard.as_mut() {
                    Some(connection) => connection.next_message().await,
                    None => unreachable!("NATS connection was initialised above"),
                }
            };
            match result {
                Ok(message) => return Ok(message),
                Err(error) => {
                    tracing::warn!(%error, "NATS connection lost; reconnecting");
                    *self.connection.lock().await = None;
                    let mut connection =
                        connection::NatsConnection::connect(&self.address, &self.name).await?;
                    connection.subscribe(&self.subject, 1).await?;
                    *self.connection.lock().await = Some(connection);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
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
