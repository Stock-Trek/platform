//! JetStream helpers shared by the publisher, durable consumer, and KV store.

use crate::{connection::NatsConnection, message::Message, publisher::PublishAck};
use platform_errors::{BpError, BpResult, IngesterError};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::time::{Instant, timeout_at};

pub(crate) const API_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const PUBLISH_ACK_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const FETCH_GRACE: Duration = Duration::from_secs(5);
pub(crate) const ACK_WAIT: Duration = Duration::from_secs(30);

/// JetStream and KV naming conventions shared by the publisher, consumer, and
/// KV store.
pub struct JetStream;

impl JetStream {
    pub(crate) fn api_subject(operation: &str) -> String {
        format!("$JS.API.{operation}")
    }

    /// Derives the JetStream stream name covering every subject below `prefix`.
    pub fn stream_name(subject_prefix: &str) -> String {
        Self::sanitize(subject_prefix, '-')
    }

    /// Derives a NATS KV bucket name from a subject prefix.
    pub fn kv_bucket_name(subject_prefix: &str) -> String {
        Self::sanitize(subject_prefix, '_')
    }

    fn sanitize(value: &str, replacement: char) -> String {
        let name: String = value
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                    character
                } else {
                    replacement
                }
            })
            .collect();
        if name.is_empty() {
            replacement.to_string()
        } else {
            name
        }
    }

    /// The JetStream stream name that stores `bucket`, following NATS KV naming.
    pub(crate) fn kv_stream(bucket: &str) -> String {
        format!("KV_{bucket}")
    }

    /// The subject a KV key is stored under.
    pub(crate) fn kv_subject(bucket: &str, key: &str) -> String {
        format!("$KV.{bucket}.{key}")
    }

    pub(crate) fn message_id(subject: &str, payload: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(subject.as_bytes());
        hasher.update([0_u8]);
        hasher.update(payload);
        hex::encode(hasher.finalize())
    }

    /// The subject used to pull the next messages from a durable consumer.
    pub(crate) fn fetch_subject(stream: &str, durable: &str) -> String {
        Self::api_subject(&format!("CONSUMER.MSG.NEXT.{stream}.{durable}"))
    }
}

/// An error returned by a JetStream API request.
#[derive(Debug, Clone)]
pub(crate) struct JetStreamApiError {
    pub code: Option<u16>,
    pub description: String,
}

impl JetStreamApiError {
    pub(crate) fn not_found(&self) -> bool {
        self.code == Some(404) || self.description.contains("not found")
    }
}

impl From<JetStreamApiError> for BpError {
    fn from(error: JetStreamApiError) -> Self {
        BpError::Ingester(IngesterError::protocol(format!(
            "JetStream request failed: {}",
            error.description
        )))
    }
}

impl NatsConnection {
    /// Performs a JetStream API request and returns the decoded response payload.
    pub(crate) async fn api_request(
        &mut self,
        subject: &str,
        payload: &[u8],
    ) -> Result<Value, JetStreamApiError> {
        let response = self
            .request(subject, payload, API_TIMEOUT)
            .await
            .map_err(|error| JetStreamApiError {
                code: None,
                description: error.to_string(),
            })?;
        if response.is_status(503) {
            return Err(JetStreamApiError {
                code: Some(503),
                description: format!("no responders for {subject}"),
            });
        }
        let value: Value =
            serde_json::from_slice(&response.payload).map_err(|error| JetStreamApiError {
                code: None,
                description: format!(
                    "JetStream response from {subject} is not valid JSON: {error}"
                ),
            })?;
        if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
            return Err(JetStreamApiError {
                code: error
                    .get("code")
                    .and_then(Value::as_u64)
                    .map(|code| code as u16),
                description: error
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown JetStream error")
                    .to_string(),
            });
        }
        Ok(value)
    }

    /// Creates the stream covering `{prefix}.>` when it does not exist yet.
    pub(crate) async fn ensure_stream(
        &mut self,
        stream: &str,
        subject_prefix: &str,
    ) -> BpResult<()> {
        let info = JetStream::api_subject(&format!("STREAM.INFO.{stream}"));
        match self.api_request(&info, b"").await {
            Ok(_) => Ok(()),
            Err(error) if error.not_found() => {
                let create = JetStream::api_subject(&format!("STREAM.CREATE.{stream}"));
                let config = json!({
                    "name": stream,
                    "subjects": [format!("{subject_prefix}.>")],
                    "storage": "file",
                });
                let payload = serde_json::to_vec(&config)?;
                self.api_request(&create, &payload).await?;
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Creates a durable pull consumer when it does not exist yet.
    pub(crate) async fn ensure_consumer(
        &mut self,
        stream: &str,
        durable: &str,
        filter_subject: &str,
    ) -> BpResult<()> {
        let info = JetStream::api_subject(&format!("CONSUMER.INFO.{stream}.{durable}"));
        match self.api_request(&info, b"").await {
            Ok(_) => Ok(()),
            Err(error) if error.not_found() => {
                let create =
                    JetStream::api_subject(&format!("CONSUMER.DURABLE.CREATE.{stream}.{durable}"));
                let config = json!({
                    "stream_name": stream,
                    "config": {
                        "durable_name": durable,
                        "filter_subject": filter_subject,
                        "deliver_policy": "all",
                        "ack_policy": "explicit",
                        "ack_wait": ACK_WAIT.as_nanos(),
                        "max_deliver": -1,
                    }
                });
                let payload = serde_json::to_vec(&config)?;
                self.api_request(&create, &payload).await?;
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Pulls the next batch of messages from a consumer, unsubscribing the inbox
    /// subscription left over by a previously cancelled fetch.
    pub(crate) async fn pull_fetch(
        &mut self,
        fetch_sid: &mut Option<u64>,
        stream: &str,
        consumer: &str,
        batch: usize,
        expires: Duration,
    ) -> BpResult<Vec<Message>> {
        if let Some(previous_sid) = fetch_sid.take() {
            let _ = self.unsubscribe(previous_sid).await;
        }
        let sid = self.allocate_sid();
        *fetch_sid = Some(sid);
        let inbox = self.next_inbox();
        self.subscribe(&inbox, sid).await?;
        let subject = JetStream::fetch_subject(stream, consumer);
        let request = json!({
            "batch": batch,
            "expires": expires.as_nanos(),
        })
        .to_string()
        .into_bytes();
        let result = match self
            .publish_with_reply(&subject, Some(&inbox), &request)
            .await
        {
            Ok(()) => {
                let deadline = Instant::now() + expires + FETCH_GRACE;
                self.collect_messages(sid, batch, deadline).await
            }
            Err(error) => Err(error),
        };
        let _ = self.unsubscribe(sid).await;
        *fetch_sid = None;
        result
    }

    async fn collect_messages(
        &mut self,
        sid: u64,
        batch: usize,
        deadline: Instant,
    ) -> BpResult<Vec<Message>> {
        let mut messages = Vec::new();
        loop {
            match timeout_at(deadline, self.next_frame()).await {
                Err(_) => {
                    return Err(Self::protocol_error(
                        "timed out waiting for fetched JetStream messages",
                    ));
                }
                Ok(Err(error)) => return Err(error),
                Ok(Ok((frame_sid, message))) => {
                    if frame_sid != sid {
                        continue;
                    }
                    if message.is_status(404) || message.is_status(408) {
                        return Ok(messages);
                    }
                    messages.push(message);
                    if messages.len() >= batch {
                        return Ok(messages);
                    }
                }
            }
        }
    }

    /// Publishes with a message id header and waits for the JetStream
    /// acknowledgement.
    pub(crate) async fn publish_and_wait(
        &mut self,
        subject: &str,
        id: &str,
        payload: &[u8],
    ) -> BpResult<PublishAck> {
        let headers = vec![("Nats-Msg-Id".to_string(), id.to_string())];
        let response = match self
            .request_with_headers(subject, &headers, payload, PUBLISH_ACK_TIMEOUT)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                return Err(match Self::no_responders(&error) {
                    Some(()) => Self::missing_stream_error(subject),
                    None => error,
                });
            }
        };
        if response.is_status(503) {
            return Err(Self::missing_stream_error(subject));
        }
        PublishAck::parse(&response.payload)
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

    /// Creates the stream backing a KV bucket when it does not exist yet.
    pub(crate) async fn ensure_kv_stream(&mut self, bucket: &str) -> BpResult<()> {
        let stream = JetStream::kv_stream(bucket);
        let info = JetStream::api_subject(&format!("STREAM.INFO.{stream}"));
        match self.api_request(&info, b"").await {
            Ok(_) => Ok(()),
            Err(error) if error.not_found() => {
                let create = JetStream::api_subject(&format!("STREAM.CREATE.{stream}"));
                let config = json!({
                    "name": stream,
                    "subjects": [format!("$KV.{bucket}.>")],
                    "storage": "file",
                    "max_msgs_per_subject": 1,
                    "allow_direct": true,
                });
                let payload = serde_json::to_vec(&config)?;
                self.api_request(&create, &payload).await?;
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_names_are_sanitize_valid_tokens() {
        assert_eq!(JetStream::stream_name("market.events"), "market-events");
        assert_eq!(
            JetStream::stream_name("market.candles.closed"),
            "market-candles-closed"
        );
        assert_eq!(
            JetStream::kv_bucket_name("market.candles.open"),
            "market_candles_open"
        );
        assert_eq!(JetStream::kv_bucket_name("market-events"), "market-events");
    }

    #[test]
    fn message_ids_are_deterministic_and_payload_sensitive() {
        let first = JetStream::message_id("subject.a", b"payload");
        let again = JetStream::message_id("subject.a", b"payload");
        let other_payload = JetStream::message_id("subject.a", b"different");
        let other_subject = JetStream::message_id("subject.b", b"payload");

        assert_eq!(first, again);
        assert_ne!(first, other_payload);
        assert_ne!(first, other_subject);
    }
}
