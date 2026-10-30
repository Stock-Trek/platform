//! JetStream naming and error helpers shared by the publisher, consumer, and KV store.

use async_nats::jetstream::stream;
use platform_errors::{BpError, NatsError};
use std::fmt::Display;
use std::time::Duration;

/// Default time JetStream waits for an acknowledgement before redelivering.
pub const ACK_WAIT: Duration = Duration::from_secs(30);

/// Maximum age of a message retained in a market-data stream.
pub const STREAM_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Maximum number of bytes retained in a market-data stream (10 GiB).
pub const STREAM_MAX_BYTES: i64 = 10 * 1024 * 1024 * 1024;

/// Maximum number of messages retained in a market-data stream.
pub const STREAM_MAX_MESSAGES: i64 = 100_000_000;

/// JetStream and KV naming conventions shared by the publisher, consumer, and
/// KV store.
pub struct JetStream;

impl JetStream {
    /// Derives the JetStream stream name covering every subject below `prefix`.
    pub fn stream_name(subject_prefix: &str) -> String {
        Self::sanitize(subject_prefix, '-')
    }

    /// Derives a NATS KV bucket name from a subject prefix.
    pub fn kv_bucket_name(subject_prefix: &str) -> String {
        Self::sanitize(subject_prefix, '_')
    }

    /// Builds the configuration used for every market-data event stream.
    ///
    /// Market data is a durable log read by several independent consumers, so
    /// the stream keeps `Limits` retention and bounds storage with explicit
    /// age, byte, and message limits. `WorkQueue` would remove a message after
    /// the first consumer acknowledges it, and `Interest` would drop messages
    /// published before any consumer exists.
    pub fn stream_config(subject_prefix: &str) -> stream::Config {
        stream::Config {
            name: Self::stream_name(subject_prefix),
            subjects: vec![format!("{subject_prefix}.>")],
            storage: stream::StorageType::File,
            retention: stream::RetentionPolicy::Limits,
            max_age: STREAM_MAX_AGE,
            max_bytes: STREAM_MAX_BYTES,
            max_messages: STREAM_MAX_MESSAGES,
            ..Default::default()
        }
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

    pub(crate) fn protocol_error(context: &str, error: impl Display) -> BpError {
        BpError::Nats(NatsError::protocol(format!("{context}: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_data_streams_have_bounded_retention_and_storage() {
        let config = JetStream::stream_config("market.events");

        assert_eq!(config.name, "market-events");
        assert_eq!(config.subjects, vec!["market.events.>".to_string()]);
        assert_eq!(config.storage, stream::StorageType::File);
        assert_eq!(config.retention, stream::RetentionPolicy::Limits);
        assert_eq!(config.max_age, STREAM_MAX_AGE);
        assert_eq!(config.max_bytes, STREAM_MAX_BYTES);
        assert_eq!(config.max_messages, STREAM_MAX_MESSAGES);
        assert!(!config.max_age.is_zero());
        assert!(config.max_bytes > 0);
        assert!(config.max_messages > 0);
    }

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
    fn protocol_errors_are_retryable_nats_errors() {
        let error = JetStream::protocol_error("test", "connection lost");

        assert!(matches!(error, BpError::Nats(NatsError::Protocol { .. })));
        assert!(error.retryability().is_retryable());
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
