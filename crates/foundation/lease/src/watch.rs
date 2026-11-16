use crate::Lease;
use platform_errors::PlatformResult;
use platform_nats::{KeyValueChange, KeyValueWatch};
use tokio::sync::watch;

pub enum LeaseWatch {
    Memory(watch::Receiver<Option<Lease>>),
    Nats(Box<KeyValueWatch>),
}

impl LeaseWatch {
    pub async fn next(&mut self) -> PlatformResult<KeyValueChange> {
        match self {
            Self::Memory(receiver) => {
                receiver.changed().await.map_err(|_| {
                    platform_errors::PlatformError::external(std::io::Error::other(
                        "lease watch closed",
                    ))
                })?;
                let lease = receiver.borrow_and_update().clone();
                Ok(KeyValueChange {
                    revision: 0,
                    value: serde_json::to_vec(&lease).ok(),
                })
            }
            Self::Nats(watch) => watch.next().await,
        }
    }
}
