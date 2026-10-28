//! NATS KV bucket access built on JetStream.

use crate::{
    NatsAddress, NatsSubject,
    jetstream::{JetStream, protocol_error},
};
use async_nats::{
    HeaderMap,
    jetstream::{
        self, Context,
        kv::{self, Store},
    },
};
use futures::TryStreamExt;
use platform_errors::{BpError, BpResult, IngesterError};

/// A NATS KV bucket storing the latest value per key.
///
/// Writes go through JetStream with a deterministic message id, so republishing
/// the same key and value is a no-op and newer values win.
pub struct KeyValue {
    bucket: String,
    context: Context,
    store: Store,
}

impl KeyValue {
    /// Connects and ensures the KV bucket's underlying stream exists.
    pub async fn connect(url: &str, name: &str, bucket: &str) -> BpResult<Self> {
        Self::validate_bucket(bucket)?;
        let address = NatsAddress::parse(url)?;
        let client = address.connect(name).await?;
        let context = jetstream::new(client);
        let store = match context.get_key_value(bucket).await {
            Ok(store) => store,
            Err(get_error) => {
                let config = kv::Config {
                    bucket: bucket.to_string(),
                    history: 1,
                    ..Default::default()
                };
                match context.create_key_value(config).await {
                    Ok(store) => store,
                    Err(create_error) => match context.get_key_value(bucket).await {
                        Ok(store) => store,
                        Err(_) => {
                            return Err(protocol_error(
                                "failed to ensure KV bucket",
                                format!("get failed: {get_error}; create failed: {create_error}"),
                            ));
                        }
                    },
                }
            }
        };
        Ok(Self {
            bucket: bucket.to_string(),
            context,
            store,
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
        let mut headers = HeaderMap::new();
        headers.insert(
            "Nats-Msg-Id",
            JetStream::message_id(&subject, value).as_str(),
        );
        let ack = self
            .context
            .publish_with_headers(subject, headers, value.to_vec().into())
            .await
            .map_err(|error| protocol_error("failed to publish KV value", error))?
            .await
            .map_err(|error| protocol_error("failed to receive KV publish ack", error))?;
        Ok(ack.sequence)
    }

    /// Returns the latest value stored under `key`, or `None` when the key has
    /// never been written.
    pub async fn get_last(&self, key: &str) -> BpResult<Option<Vec<u8>>> {
        NatsSubject::validate_named("KV key", key)?;
        self.store
            .get(key)
            .await
            .map(|value| value.map(|value| value.to_vec()))
            .map_err(|error| protocol_error("failed to read KV value", error))
    }

    /// Deletes the value stored under `key`, if any. The deletion is a
    /// non-destructive KV marker so consumers can observe the removal.
    pub async fn delete(&self, key: &str) -> BpResult<()> {
        NatsSubject::validate_named("KV key", key)?;
        self.store
            .delete(key)
            .await
            .map_err(|error| protocol_error("failed to delete KV value", error))
    }

    /// Lists every key in the bucket.
    pub async fn keys(&self) -> BpResult<Vec<String>> {
        self.store
            .keys()
            .await
            .map_err(|error| protocol_error("failed to list KV keys", error))?
            .try_collect()
            .await
            .map_err(|error| protocol_error("failed to read KV key", error))
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
