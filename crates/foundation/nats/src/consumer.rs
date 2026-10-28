//! Durable JetStream pull consumer with explicit acknowledgements.

use crate::{
    NatsAddress, NatsSubject,
    jetstream::{ACK_WAIT, JetStream, protocol_error},
    message::Message,
};
use async_nats::{
    Client,
    jetstream::{
        self,
        consumer::{AckPolicy, DeliverPolicy, pull},
        stream,
    },
};
use futures::StreamExt;
use platform_errors::BpResult;
use std::time::Duration;

const DEFAULT_EXPIRES: Duration = Duration::from_secs(1);
const FETCH_BATCH: usize = 1;
const FETCH_WINDOW: Duration = Duration::from_secs(30);
const RETRY_BACKOFF: Duration = Duration::from_secs(1);

/// A durable pull consumer over a JetStream stream.
///
/// Messages are fetched in batches and explicitly acknowledged, so a restart
/// resumes from the last acknowledged message instead of missing everything
/// that happened while the process was down.
pub struct JetStreamConsumer {
    client: Client,
    consumer: async_nats::jetstream::consumer::Consumer<pull::Config>,
    stream: String,
    durable: String,
    filter: String,
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
        let client = address.connect(name).await?;
        let context = jetstream::new(client.clone());
        let stream_handle = context
            .get_or_create_stream(stream::Config {
                name: stream.clone(),
                subjects: vec![format!("{stream_subject_prefix}.>")],
                storage: stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|error| protocol_error("failed to ensure JetStream stream", error))?;
        let consumer = stream_handle
            .get_or_create_consumer(
                durable,
                pull::Config {
                    durable_name: Some(durable.to_string()),
                    filter_subject: filter_subject.to_string(),
                    deliver_policy: DeliverPolicy::All,
                    ack_policy: AckPolicy::Explicit,
                    ack_wait: ACK_WAIT,
                    max_deliver: -1,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| protocol_error("failed to ensure JetStream consumer", error))?;
        Ok(Self {
            client,
            consumer,
            stream,
            durable: durable.to_string(),
            filter: filter_subject.to_string(),
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
        let mut stream = self
            .consumer
            .fetch()
            .max_messages(batch)
            .expires(expires)
            .messages()
            .await
            .map_err(|error| protocol_error("failed to fetch JetStream messages", error))?;
        let mut messages = Vec::with_capacity(batch);
        while let Some(message) = stream.next().await {
            let message = message
                .map_err(|error| protocol_error("failed to read JetStream message", error))?;
            messages.push(Message::from(message.message));
        }
        Ok(messages)
    }

    /// Acknowledges a fetched message. Redelivery after a lost acknowledgement
    /// keeps processing restart-safe and idempotent.
    pub async fn ack(&self, message: &Message) -> BpResult<()> {
        let Some(reply) = &message.reply_to else {
            return Ok(());
        };
        self.client
            .publish(reply.clone(), Vec::new().into())
            .await
            .map_err(|error| protocol_error("failed to acknowledge JetStream message", error))?;
        self.client
            .flush()
            .await
            .map_err(|error| protocol_error("failed to flush JetStream acknowledgement", error))
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

    /// Fetches the next message, waiting up to [`FETCH_WINDOW`] and retrying
    /// transient failures with [`RETRY_BACKOFF`]. The message is left
    /// unacknowledged so callers can acknowledge it after durable processing.
    ///
    /// Returns `None` when the fetch window expired or all messages were
    /// negatively acknowledged.
    pub async fn next_message_unacked(&self) -> Option<Message> {
        let messages = self
            .fetch_retrying(FETCH_BATCH, FETCH_WINDOW, RETRY_BACKOFF)
            .await;
        messages.into_iter().next()
    }

    /// Fetches the next message and acknowledges it once fetched.
    ///
    /// Returns `None` when the fetch window expired without a message or the
    /// acknowledgement failed; callers should simply retry.
    pub async fn next_message(&self, log_label: &str) -> Option<Message> {
        let message = self.next_message_unacked().await?;
        if let Err(error) = self.ack(&message).await {
            tracing::warn!(%error, "failed to acknowledge {log_label}; waiting for redelivery");
            return None;
        }
        Some(message)
    }
}
