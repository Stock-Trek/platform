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
