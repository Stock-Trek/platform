//! JetStream naming and error helpers shared by the publisher, consumer, and KV store.

use platform_errors::{BpError, IngesterError};
use std::fmt::Display;

pub(crate) const ACK_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

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
}

pub(crate) fn protocol_error(context: &str, error: impl Display) -> BpError {
    BpError::Ingester(IngesterError::protocol(format!("{context}: {error}")))
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
