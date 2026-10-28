//! Durable JetStream pull consumer with explicit acknowledgements.

use crate::{
    NatsAddress, NatsSubject, connection::NatsConnection, jetstream::JetStream, message::Message,
};
use platform_errors::BpResult;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;

const DEFAULT_EXPIRES: Duration = Duration::from_secs(1);

struct State {
    connection: Option<NatsConnection>,
    fetch_sid: Option<u64>,
}

/// A durable pull consumer over a JetStream stream.
///
/// Messages are fetched in batches and explicitly acknowledged, so a restart
/// resumes from the last acknowledged message instead of missing everything
/// that happened while the process was down.
pub struct JetStreamConsumer {
    address: NatsAddress,
    name: String,
    stream: String,
    durable: String,
    filter: String,
    state: AsyncMutex<State>,
}

impl JetStreamConsumer {
    /// Connects and ensures the stream covering `stream_subject_prefix` and the
    /// durable consumer with `filter_subject` exist.
    pub async fn connect(
        url: &str,
        name: &str,
        stream_subject_prefix: &str,
        filter_subject: &str,
        durable: &str,
    ) -> BpResult<Self> {
        let address = NatsAddress::parse(url)?;
        NatsSubject::validate_named("filter subject", filter_subject)?;
        let stream = JetStream::stream_name(stream_subject_prefix);
        let mut connection = NatsConnection::connect(&address, name).await?;
        connection
            .ensure_stream(&stream, stream_subject_prefix)
            .await?;
        connection
            .ensure_consumer(&stream, durable, filter_subject)
            .await?;
        Ok(Self {
            address,
            name: name.to_string(),
            stream,
            durable: durable.to_string(),
            filter: filter_subject.to_string(),
            state: AsyncMutex::new(State {
                connection: Some(connection),
                fetch_sid: None,
            }),
        })
    }

    pub fn stream(&self) -> &str {
        &self.stream
    }

    pub fn durable(&self) -> &str {
        &self.durable
    }

    pub fn filter_subject(&self) -> &str {
        &self.filter
    }

    /// Fetches up to `batch` messages, waiting up to `expires` for the first
    /// one. An empty result means nothing was available in the window.
    pub async fn fetch(&self, batch: usize, expires: Duration) -> BpResult<Vec<Message>> {
        let expires = if expires.is_zero() {
            DEFAULT_EXPIRES
        } else {
            expires
        };
        let mut state = self.state.lock().await;
        let result = self.fetch_locked(&mut state, batch, expires).await;
        if result.is_err() {
            state.connection = None;
            state.fetch_sid = None;
        }
        result
    }

    async fn fetch_locked(
        &self,
        state: &mut State,
        batch: usize,
        expires: Duration,
    ) -> BpResult<Vec<Message>> {
        if state.connection.is_none() {
            state.connection = Some(NatsConnection::connect(&self.address, &self.name).await?);
            state.fetch_sid = None;
        }
        let connection = state
            .connection
            .as_mut()
            .expect("NATS connection was initialised above");
        connection
            .pull_fetch(
                &mut state.fetch_sid,
                &self.stream,
                &self.durable,
                batch,
                expires,
            )
            .await
    }

    /// Acknowledges a fetched message. Redelivery after a lost acknowledgement
    /// keeps processing restart-safe and idempotent.
    pub async fn ack(&self, message: &Message) -> BpResult<()> {
        let Some(reply) = &message.reply_to else {
            return Ok(());
        };
        let mut state = self.state.lock().await;
        if state.connection.is_none() {
            state.connection = Some(NatsConnection::connect(&self.address, &self.name).await?);
            state.fetch_sid = None;
        }
        let connection = state
            .connection
            .as_mut()
            .expect("NATS connection was initialised above");
        let result = connection.publish(reply, b"").await;
        if result.is_err() {
            state.connection = None;
        }
        result
    }

    /// Fetches with retries so transient connection loss does not surface to
    /// callers.
    pub async fn fetch_retrying(
        &self,
        batch: usize,
        expires: Duration,
        retry_backoff: Duration,
    ) -> Vec<Message> {
        loop {
            match self.fetch(batch, expires).await {
                Ok(messages) => return messages,
                Err(error) => {
                    tracing::warn!(%error, "JetStream fetch failed; retrying");
                    tokio::time::sleep(retry_backoff).await;
                }
            }
        }
    }
}
