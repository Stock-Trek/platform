//! Durable JetStream pull consumer with explicit acknowledgements.

use crate::{
    NatsAddress, NatsSubject,
    jetstream::{ACK_WAIT, JetStream},
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
        Self::connect_from(
            url,
            name,
            stream_subject_prefix,
            filter_subject,
            durable,
            None,
        )
        .await
    }

    /// Connects a durable consumer that resumes from `start_sequence` when it
    /// is set.
    ///
    /// `DeliverPolicy::ByStartSequence` is inclusive, so callers normally pass
    /// the sequence immediately after their last checkpointed message. Passing
    /// `None` keeps the original `DeliverPolicy::All` behaviour. The consumer
    /// config is part of the durable's identity in JetStream; changing the start
    /// sequence recreates the consumer at the requested position.
    pub async fn connect_from(
        url: &str,
        name: &str,
        stream_subject_prefix: &str,
        filter_subject: &str,
        durable: &str,
        start_sequence: Option<u64>,
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
            .map_err(|error| {
                JetStream::protocol_error("failed to ensure JetStream stream", error)
            })?;
        let consumer = stream_handle
            .get_or_create_consumer(
                durable,
                pull::Config {
                    durable_name: Some(durable.to_string()),
                    filter_subject: filter_subject.to_string(),
                    deliver_policy: Self::deliver_policy(start_sequence),
                    ack_policy: AckPolicy::Explicit,
                    ack_wait: ACK_WAIT,
                    max_deliver: -1,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| {
                JetStream::protocol_error("failed to ensure JetStream consumer", error)
            })?;
        Ok(Self {
            client,
            consumer,
            stream,
            durable: durable.to_string(),
            filter: filter_subject.to_string(),
        })
    }

    /// Builds the delivery policy used when a consumer is created.
    ///
    /// Checkpoints store the last processed sequence, while JetStream's
    /// `ByStartSequence` policy is inclusive, so callers pass the next sequence.
    pub fn deliver_policy(start_sequence: Option<u64>) -> DeliverPolicy {
        match start_sequence {
            Some(start_sequence) if start_sequence > 0 => {
                DeliverPolicy::ByStartSequence { start_sequence }
            }
            _ => DeliverPolicy::All,
        }
    }

    /// Alias for [`Self::connect_from`] used by services that describe the
    /// argument as a resume sequence.
    pub async fn connect_with_start_sequence(
        url: &str,
        name: &str,
        stream_subject_prefix: &str,
        filter_subject: &str,
        durable: &str,
        start_sequence: Option<u64>,
    ) -> BpResult<Self> {
        Self::connect_from(
            url,
            name,
            stream_subject_prefix,
            filter_subject,
            durable,
            start_sequence,
        )
        .await
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
            .map_err(|error| {
                JetStream::protocol_error("failed to fetch JetStream messages", error)
            })?;
        let mut messages = Vec::with_capacity(batch);
        while let Some(message) = stream.next().await {
            let message = message.map_err(|error| {
                JetStream::protocol_error("failed to read JetStream message", error)
            })?;
            messages.push(Message::from(message));
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
            .map_err(|error| {
                JetStream::protocol_error("failed to acknowledge JetStream message", error)
            })?;
        self.client.flush().await.map_err(|error| {
            JetStream::protocol_error("failed to flush JetStream acknowledgement", error)
        })
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_policy_starts_at_the_requested_sequence() {
        assert_eq!(
            JetStreamConsumer::deliver_policy(Some(42)),
            DeliverPolicy::ByStartSequence { start_sequence: 42 }
        );
    }

    #[test]
    fn missing_sequence_starts_from_the_beginning() {
        assert_eq!(JetStreamConsumer::deliver_policy(None), DeliverPolicy::All);
        assert_eq!(
            JetStreamConsumer::deliver_policy(Some(0)),
            DeliverPolicy::All
        );
    }
}
