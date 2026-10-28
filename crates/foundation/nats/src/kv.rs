//! NATS KV bucket access built on JetStream.

use crate::{
    NatsAddress, NatsSubject, connection::NatsConnection, jetstream::JetStream, message::Message,
    publisher::PublishAck,
};
use platform_errors::{BpError, BpResult, IngesterError};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;

const KEYS_FETCH_BATCH: usize = 1_024;
const KEYS_FETCH_EXPIRES: Duration = Duration::from_secs(1);
const KEYS_CONSUMER_INACTIVITY: Duration = Duration::from_secs(60);

/// A NATS KV bucket storing the latest value per key.
///
/// Writes go through JetStream with a deterministic message id, so republishing
/// the same key and value is a no-op and newer values win.
pub struct KeyValue {
    address: NatsAddress,
    name: String,
    bucket: String,
    connection: AsyncMutex<Option<NatsConnection>>,
}

impl KeyValue {
    /// Connects and ensures the KV bucket's underlying stream exists.
    pub async fn connect(url: &str, name: &str, bucket: &str) -> BpResult<Self> {
        Self::validate_bucket(bucket)?;
        let address = NatsAddress::parse(url)?;
        let mut connection = NatsConnection::connect(&address, name).await?;
        connection.ensure_kv_stream(bucket).await?;
        Ok(Self {
            address,
            name: name.to_string(),
            bucket: bucket.to_string(),
            connection: AsyncMutex::new(Some(connection)),
        })
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Stores `value` under `key` and returns the KV revision assigned by the
    /// server. Repeating an identical write is idempotent.
    pub async fn put(&self, key: &str, value: &[u8]) -> BpResult<u64> {
        NatsSubject::validate_named("KV key", key)?;
        let subject = JetStream::kv_subject(&self.bucket, key);
        let ack = self.publish(&subject, value).await?;
        Ok(ack.sequence)
    }

    /// Returns the latest value stored under `key`, or `None` when the key has
    /// never been written.
    pub async fn get_last(&self, key: &str) -> BpResult<Option<Vec<u8>>> {
        NatsSubject::validate_named("KV key", key)?;
        let result = match self.locked_connection().await?.as_mut() {
            Some(connection) => self.get_last_on(connection, key).await,
            None => unreachable!("NATS connection was initialised above"),
        };
        if result.is_err() {
            *self.connection.lock().await = None;
        }
        result
    }

    /// Lists every key in the bucket.
    pub async fn keys(&self) -> BpResult<Vec<String>> {
        let result = match self.locked_connection().await?.as_mut() {
            Some(connection) => self.keys_on(connection).await,
            None => unreachable!("NATS connection was initialised above"),
        };
        if result.is_err() {
            *self.connection.lock().await = None;
        }
        result
    }

    async fn get_last_on(
        &self,
        connection: &mut NatsConnection,
        key: &str,
    ) -> BpResult<Option<Vec<u8>>> {
        let subject = JetStream::api_subject(&format!(
            "DIRECT.GET.{}.{}",
            JetStream::kv_stream(&self.bucket),
            JetStream::kv_subject(&self.bucket, key)
        ));
        let response = connection
            .request(&subject, b"", Duration::from_secs(5))
            .await?;
        if response.is_status(404) || response.is_status(408) {
            return Ok(None);
        }
        Ok(Some(response.payload))
    }

    async fn keys_on(&self, connection: &mut NatsConnection) -> BpResult<Vec<String>> {
        let stream = JetStream::kv_stream(&self.bucket);
        let create = JetStream::api_subject(&format!("CONSUMER.CREATE.{stream}"));
        let config = json!({
            "stream_name": stream,
            "config": {
                "filter_subject": format!("$KV.{}.>", self.bucket),
                "deliver_policy": "last_per_subject",
                "ack_policy": "none",
                "headers_only": true,
                "inactive_threshold": KEYS_CONSUMER_INACTIVITY.as_nanos(),
            }
        });
        let payload = serde_json::to_vec(&config)?;
        let response = connection.api_request(&create, &payload).await?;
        let ephemeral = response
            .pointer("/consumer/name")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                BpError::Ingester(IngesterError::protocol(
                    "KV key listing did not return an ephemeral consumer name",
                ))
            })?
            .to_string();

        let key_prefix = format!("$KV.{}.", self.bucket);
        let mut keys = Vec::new();
        let mut fetch_sid = None;
        loop {
            let messages = connection
                .pull_fetch(
                    &mut fetch_sid,
                    &stream,
                    &ephemeral,
                    KEYS_FETCH_BATCH,
                    KEYS_FETCH_EXPIRES,
                )
                .await?;
            let exhausted = messages.len() < KEYS_FETCH_BATCH;
            for Message { subject, .. } in messages {
                if let Some(key) = subject.strip_prefix(&key_prefix) {
                    keys.push(key.to_string());
                }
            }
            if exhausted {
                break;
            }
        }
        let _ = connection
            .api_request(
                &JetStream::api_subject(&format!("CONSUMER.DELETE.{stream}.{ephemeral}")),
                b"{}",
            )
            .await;
        Ok(keys)
    }

    async fn publish(&self, subject: &str, value: &[u8]) -> BpResult<PublishAck> {
        let id = JetStream::message_id(subject, value);
        let result = match self.locked_connection().await?.as_mut() {
            Some(connection) => connection.publish_and_wait(subject, &id, value).await,
            None => unreachable!("NATS connection was initialised above"),
        };
        if result.is_err() {
            *self.connection.lock().await = None;
        }
        result
    }

    async fn locked_connection(
        &self,
    ) -> BpResult<tokio::sync::MutexGuard<'_, Option<NatsConnection>>> {
        let mut connection = self.connection.lock().await;
        if connection.is_none() {
            *connection = Some(NatsConnection::connect(&self.address, &self.name).await?);
        }
        Ok(connection)
    }

    fn validate_bucket(bucket: &str) -> BpResult<()> {
        let valid = !bucket.is_empty()
            && bucket.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            });
        if valid {
            Ok(())
        } else {
            Err(BpError::Ingester(IngesterError::protocol(format!(
                "invalid KV bucket name {bucket:?}: must only contain alphanumeric characters, dashes, and underscores"
            ))))
        }
    }
}
