use crate::{NatsAddress, NatsSubject};
use async_nats::{Client, Subscriber};
use futures::StreamExt;
use platform_errors::BpResult;

/// Subscription that yields the raw subject and payload for each message.
///
/// The underlying client reconnects and resubscribes automatically, so callers
/// do not need to manage connection state.
pub struct NatsSubscriber {
    _client: Client,
    subscriber: Subscriber,
}

impl NatsSubscriber {
    pub async fn connect(url: &str, name: &str, subject: &str) -> BpResult<Self> {
        let address = NatsAddress::parse(url)?;
        NatsSubject::validate(subject)?;
        let client = address.connect(name).await?;
        let subscriber = client
            .subscribe(subject.to_string())
            .await
            .map_err(|error| {
                crate::jetstream::JetStream::protocol_error(
                    "failed to subscribe to NATS subject",
                    error,
                )
            })?;
        Ok(Self {
            _client: client,
            subscriber,
        })
    }

    pub async fn next_message(&mut self) -> BpResult<Option<(String, Vec<u8>)>> {
        Ok(self
            .subscriber
            .next()
            .await
            .map(|message| (message.subject.to_string(), message.payload.to_vec())))
    }
}
