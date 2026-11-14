use crate::OwnershipStore;
use platform_errors::{PlatformError, PlatformResult};
use platform_nats::EventPublisher;
use platform_portfolio_ownership::OWNERSHIP_EVENT_SUBJECT;
use std::{future::Future, sync::Arc, time::Duration};
use tokio::time::{MissedTickBehavior, interval};

const BATCH_SIZE: usize = 100;

pub struct OwnershipOutboxRelay {
    store: Arc<dyn OwnershipStore>,
    publisher: Arc<dyn EventPublisher>,
    subject: String,
    interval: Duration,
}

impl OwnershipOutboxRelay {
    pub fn new(
        store: Arc<dyn OwnershipStore>,
        publisher: Arc<dyn EventPublisher>,
        interval: Duration,
    ) -> PlatformResult<Self> {
        if interval.is_zero() {
            return Err(PlatformError::config_invalid(
                "outbox interval",
                "must be greater than zero",
            ));
        }
        Ok(Self {
            store,
            publisher,
            subject: OWNERSHIP_EVENT_SUBJECT.to_string(),
            interval,
        })
    }

    pub async fn flush_once(&self) -> PlatformResult<usize> {
        let events = self.store.pending_events(BATCH_SIZE).await?;
        let count = events.len();
        for event in events {
            let payload = serde_json::to_vec(&event)?;
            self.publisher.publish(&self.subject, &payload).await?;
            self.store.acknowledge(&event).await?;
        }
        Ok(count)
    }

    pub async fn run<F>(&self, shutdown: F) -> PlatformResult<()>
    where
        F: Future<Output = PlatformResult<()>> + Send,
    {
        let mut ticker = interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                signal = &mut shutdown => return signal,
                _ = ticker.tick() => {
                    if let Err(error) = self.flush_once().await {
                        tracing::warn!(%error, "ownership outbox flush failed");
                    }
                }
            }
        }
    }
}
