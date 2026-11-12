use crate::health::{CONNECTOR_HEALTH_SUBJECT, ConnectorHealth, ConnectorState};
use chrono::Utc;
use platform_metrics::PortfolioMetrics;
use platform_nats::EventPublisher;
use std::{sync::Arc, time::Instant};
use stock_trek_types::cex::cex_id::CexId;

const CEX: &str = "binance";

#[derive(Clone)]
pub(super) struct HealthReporter {
    publisher: Option<Arc<dyn EventPublisher>>,
    subject: String,
}

impl HealthReporter {
    pub fn new(publisher: Option<Arc<dyn EventPublisher>>, subject: impl Into<String>) -> Self {
        Self {
            publisher,
            subject: subject.into(),
        }
    }

    pub async fn publish(
        &self,
        state: ConnectorState,
        subscriptions: usize,
        last_update: Option<Instant>,
    ) {
        if let Some(last_update) = last_update {
            PortfolioMetrics::record_connector_update_age(CEX, last_update.elapsed());
        }
        let Some(publisher) = &self.publisher else {
            return;
        };
        let age: Option<u64> = last_update
            .map(|instant| instant.elapsed())
            .map(|duration| duration.as_secs());
        let health =
            ConnectorHealth::new(CexId::Binance, state, subscriptions as u64, age, Utc::now());
        if let Ok(payload) = health.to_bytes()
            && let Err(error) = publisher.publish(&self.subject, &payload).await
        {
            tracing::warn!(%error, "failed to publish portfolio connector health");
        }
    }
}

impl Default for HealthReporter {
    fn default() -> Self {
        Self::new(None, CONNECTOR_HEALTH_SUBJECT)
    }
}
