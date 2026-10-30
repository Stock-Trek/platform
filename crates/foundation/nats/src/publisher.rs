//! JetStream-backed publisher with acknowledgements and idempotent message ids.

use crate::{NatsAddress, NatsSubject, jetstream::JetStream};
use async_nats::{
    HeaderMap,
    jetstream::{self, Context},
};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::BpResult;
use std::sync::Arc;
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

impl From<async_nats::jetstream::publish::PublishAck> for PublishAck {
    fn from(ack: async_nats::jetstream::publish::PublishAck) -> Self {
        Self {
            stream: ack.stream,
            sequence: ack.sequence,
            duplicate: ack.duplicate,
        }
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
    context: AsyncMutex<Option<Context>>,
    streams: Mutex<Vec<String>>,
}

#[async_trait]
impl EventPublisher for NatsPublisher {
    async fn publish(&self, subject: &str, payload: &[u8]) -> BpResult<()> {
        NatsPublisher::publish(self, subject, payload).await
    }
}

#[async_trait]
impl<T> EventPublisher for Arc<T>
where
    T: EventPublisher + ?Sized,
{
    async fn publish(&self, subject: &str, payload: &[u8]) -> BpResult<()> {
        (**self).publish(subject, payload).await
    }
}

impl NatsPublisher {
    pub async fn connect(url: &str, name: &str) -> BpResult<Self> {
        let address = NatsAddress::parse(url)?;
        Ok(Self {
            address,
            name: name.to_string(),
            context: AsyncMutex::new(None),
            streams: Mutex::new(Vec::new()),
        })
    }

    /// Ensures a JetStream stream covering every subject below `prefix` exists,
    /// creating it on first use.
    pub async fn ensure_stream(&self, subject_prefix: &str) -> BpResult<()> {
        NatsSubject::validate_named("subject prefix", subject_prefix)?;
        let stream = JetStream::stream_name(subject_prefix);
        if self.streams.lock().contains(&stream) {
            return Ok(());
        }

        let context = self.context().await?;
        let result = context
            .get_or_create_stream(JetStream::stream_config(subject_prefix))
            .await
            .map(|_| ())
            .map_err(|error| JetStream::protocol_error("failed to ensure JetStream stream", error));
        match result {
            Ok(()) => {
                self.streams.lock().push(stream);
                Ok(())
            }
            Err(error) => {
                *self.context.lock().await = None;
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
        let context = self.context().await?;
        let mut headers = HeaderMap::new();
        headers.insert("Nats-Msg-Id", message_id);
        let result = async {
            let ack = context
                .publish_with_headers(subject.to_string(), headers, payload.to_vec().into())
                .await
                .map_err(|error| {
                    JetStream::protocol_error("failed to publish to JetStream", error)
                })?
                .await
                .map_err(|error| {
                    JetStream::protocol_error("failed to receive JetStream publish ack", error)
                })?;
            Ok(PublishAck::from(ack))
        }
        .await;
        if result.is_err() {
            *self.context.lock().await = None;
        }
        result
    }

    async fn context(&self) -> BpResult<Context> {
        let mut context = self.context.lock().await;
        if let Some(initialised) = context.as_ref() {
            return Ok(initialised.clone());
        }
        let client = self.address.connect(&self.name).await?;
        let initialised = jetstream::new(client);
        *context = Some(initialised.clone());
        Ok(initialised)
    }
}
