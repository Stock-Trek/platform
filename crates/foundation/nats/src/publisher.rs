//! JetStream-backed publisher with acknowledgements and idempotent message ids.

use crate::{
    NatsAddress, NatsSubject,
    connection::NatsConnection,
    jetstream::{PUBLISH_ACK_TIMEOUT, ensure_stream, message_id, stream_name},
};
use platform_errors::{BpError, BpResult, IngesterError};
use serde_json::Value;
use std::sync::Mutex;
use tokio::sync::Mutex as AsyncMutex;

/// Acknowledgement returned by JetStream for a stored message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishAck {
    pub stream: String,
    pub sequence: u64,
    pub duplicate: bool,
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
        let stream = stream_name(subject_prefix);
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
            Some(connection) => ensure_stream(connection, &stream, subject_prefix).await,
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
        self.publish_with_message_id(subject, &message_id(subject, payload), payload)
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
            Some(connection) => publish_and_wait(connection, subject, message_id, payload).await,
            None => unreachable!("NATS connection was initialised above"),
        };
        if result.is_err() {
            *connection = None;
        }
        result.map(|(stream, sequence, duplicate)| PublishAck {
            stream,
            sequence,
            duplicate,
        })
    }
}

/// Publishes with a message id header and waits for the JetStream
/// acknowledgement, returning `(stream, sequence, duplicate)`.
pub(crate) async fn publish_and_wait(
    connection: &mut NatsConnection,
    subject: &str,
    id: &str,
    payload: &[u8],
) -> BpResult<(String, u64, bool)> {
    let headers = vec![("Nats-Msg-Id".to_string(), id.to_string())];
    let response = match connection
        .request_with_headers(subject, &headers, payload, PUBLISH_ACK_TIMEOUT)
        .await
    {
        Ok(response) => response,
        Err(error) => {
            return Err(match no_responders(&error) {
                Some(()) => missing_stream_error(subject),
                None => error,
            });
        }
    };
    if response.is_status(503) {
        return Err(missing_stream_error(subject));
    }
    parse_publish_ack(&response.payload)
}

fn no_responders(error: &BpError) -> Option<()> {
    let BpError::Ingester(IngesterError::Protocol { message, .. }) = error else {
        return None;
    };
    message.contains("no responders").then_some(())
}

fn missing_stream_error(subject: &str) -> BpError {
    BpError::Ingester(IngesterError::protocol(format!(
        "no JetStream stream stores {subject}"
    )))
}

/// Parses a JetStream publish acknowledgement payload.
pub(crate) fn parse_publish_ack(payload: &[u8]) -> BpResult<(String, u64, bool)> {
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
    Ok((stream, sequence, duplicate))
}
