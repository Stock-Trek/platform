use crate::{address::NatsAddress, connection, subject::NatsSubject};
use platform_errors::BpResult;
use tokio::sync::Mutex;

/// Subscription that yields the raw subject and payload for each message,
/// reconnecting and resubscribing when the connection drops.
pub struct NatsSubscriber {
    address: NatsAddress,
    name: String,
    subject: String,
    connection: Mutex<Option<connection::NatsConnection>>,
}

impl NatsSubscriber {
    pub async fn connect(url: &str, name: &str, subject: &str) -> BpResult<Self> {
        let address = NatsAddress::parse(url)?;
        NatsSubject::validate(subject)?;
        let mut connection = connection::NatsConnection::connect(&address, name).await?;
        connection.subscribe(subject, 1).await?;
        Ok(Self {
            address,
            name: name.to_string(),
            subject: subject.to_string(),
            connection: Mutex::new(Some(connection)),
        })
    }

    pub async fn next_message(&mut self) -> BpResult<Option<(String, Vec<u8>)>> {
        loop {
            let result = {
                let mut guard = self.connection.lock().await;
                match guard.as_mut() {
                    Some(connection) => connection.next_message().await,
                    None => unreachable!("NATS connection was initialised above"),
                }
            };
            match result {
                Ok(message) => return Ok(message),
                Err(error) => {
                    tracing::warn!(%error, "NATS connection lost; reconnecting");
                    *self.connection.lock().await = None;
                    let mut connection =
                        connection::NatsConnection::connect(&self.address, &self.name).await?;
                    connection.subscribe(&self.subject, 1).await?;
                    *self.connection.lock().await = Some(connection);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }
}
