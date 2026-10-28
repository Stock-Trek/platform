//! JetStream-backed publisher with acknowledgements and idempotent message ids.

use crate::{NatsAddress, NatsSubject, connection::NatsConnection, jetstream::JetStream};
use async_trait::async_trait;
use platform_errors::{BpError, BpResult, IngesterError};
use serde_json::Value;
use std::sync::Mutex;
use tokio::sync::Mutex as AsyncMutex;

/// Abstraction over event publication, allowing services to be tested with an
/// in-memory publisher instead of a live NATS connection.
#[async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(&self, subject: &str, payload: &[u8]) -> BpResult<()>;
}

/// Acknowledgement returned by JetStream for a stored message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishAck {
    pub stream: String,
    pub sequence: u64,
    pub duplicate: bool,
}

impl PublishAck {
    /// Parses a JetStream publish acknowledgement payload.
    pub(crate) fn parse(payload: &[u8]) -> BpResult<PublishAck> {
        let value: Value = serde_json::from_slice(payload)?;
        if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
            return Err(BpError::Ingester(IngesterError::protocol(format!(
                "JetStream publish rejected: {}",
                error
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
            ))));
        }
        let stream = value
            .get("stream")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                BpError::Ingester(IngesterError::protocol(
                    "JetStream ack is missing the stream name",
                ))
            })?
            .to_string();
        let sequence = value.get("seq").and_then(Value::as_u64).ok_or_else(|| {
            BpError::Ingester(IngesterError::protocol(
                "JetStream ack is missing the sequence",
            ))
        })?;
        let duplicate = value
            .get("duplicate")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        Ok(PublishAck {
            stream,
            sequence,
            duplicate,
        })
    }
}

/// Publisher that stores every message on a JetStream stream and waits for the
/// server acknowledgement.
///
/// Messages carry a deterministic id derived from the subject and payload, so a
/// reconnect cannot duplicate or silently drop events: a redelivered publish is
/// de-duplicated by the server within the duplicate window.
pub struct NatsPublisher {
    address: NatsAddress,
    name: String,
    connection: AsyncMutex<Option<NatsConnection>>,
    streams: Mutex<Vec<String>>,
}

#[async_trait]
impl EventPublisher for NatsPublisher {
    async fn publish(&self, subject: &str, payload: &[u8]) -> BpResult<()> {
        NatsPublisher::publish(self, subject, payload).await
    }
}

impl NatsPublisher {
    pub async fn connect(url: &str, name: &str) -> BpResult<Self> {
        let address = NatsAddress::parse(url)?;
        Ok(Self {
            address,
            name: name.to_string(),
            connection: AsyncMutex::new(None),
            streams: Mutex::new(Vec::new()),
        })
    }

    /// Ensures a JetStream stream covering every subject below `prefix` exists,
    /// creating it on first use.
    pub async fn ensure_stream(&self, subject_prefix: &str) -> BpResult<()> {
        NatsSubject::validate_named("subject prefix", subject_prefix)?;
        let stream = JetStream::stream_name(subject_prefix);
        if self
            .streams
            .lock()
            .expect("publisher streams lock")
            .contains(&stream)
        {
            return Ok(());
        }
        let mut connection = self.connection.lock().await;
        if connection.is_none() {
            *connection = Some(NatsConnection::connect(&self.address, &self.name).await?);
        }
        let result = match connection.as_mut() {
            Some(connection) => connection.ensure_stream(&stream, subject_prefix).await,
            None => unreachable!("NATS connection was initialised above"),
        };
        match result {
            Ok(()) => {
                self.streams
                    .lock()
                    .expect("publisher streams lock")
                    .push(stream);
                Ok(())
            }
            Err(error) => {
                *connection = None;
                Err(error)
            }
        }
    }

    /// Publishes with a deterministic message id derived from the subject and
    /// payload, waiting for the JetStream acknowledgement.
    pub async fn publish(&self, subject: &str, payload: &[u8]) -> BpResult<()> {
        self.publish_with_message_id(subject, &JetStream::message_id(subject, payload), payload)
            .await
            .map(|_| ())
    }

    /// Publishes with an explicit deterministic message id and waits for the
    /// JetStream acknowledgement.
    pub async fn publish_with_message_id(
        &self,
        subject: &str,
        message_id: &str,
        payload: &[u8],
    ) -> BpResult<PublishAck> {
        NatsSubject::validate(subject)?;
        let mut connection = self.connection.lock().await;
        if connection.is_none() {
            *connection = Some(NatsConnection::connect(&self.address, &self.name).await?);
        }
        let result = match connection.as_mut() {
            Some(connection) => {
                connection
                    .publish_and_wait(subject, message_id, payload)
                    .await
            }
            None => unreachable!("NATS connection was initialised above"),
        };
        if result.is_err() {
            *connection = None;
        }
        result
    }
}
