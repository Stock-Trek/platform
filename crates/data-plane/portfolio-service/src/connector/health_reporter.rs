use super::{CONNECTOR_HEALTH_SUBJECT, ConnectorHealth, ConnectorHealthSink, ConnectorState};
use chrono::Utc;
use platform_metrics::PortfolioMetrics;
use std::{sync::Arc, time::Instant};
use stock_trek_types::cex::cex_id::CexId;

#[derive(Clone)]
pub(super) struct HealthReporter {
    cex_id: CexId,
    sink: Arc<dyn ConnectorHealthSink>,
}

impl HealthReporter {
    pub(super) fn new(cex_id: CexId, sink: Arc<dyn ConnectorHealthSink>) -> Self {
        Self { cex_id, sink }
    }

    pub(super) async fn publish(
        &self,
        state: ConnectorState,
        subscriptions: usize,
        last_update: Option<Instant>,
    ) {
        if let Some(last_update) = last_update {
            PortfolioMetrics::record_connector_update_age(
                self.cex_id.to_string(),
                last_update.elapsed(),
            );
        }
        let age = last_update.map(|instant| instant.elapsed().as_secs());
        let health =
            ConnectorHealth::new(self.cex_id, state, subscriptions as u64, age, Utc::now());
        if let Err(error) = self.sink.publish(health).await {
            tracing::warn!(%error, subject = CONNECTOR_HEALTH_SUBJECT, "failed to publish portfolio connector health");
        }
    }
}
