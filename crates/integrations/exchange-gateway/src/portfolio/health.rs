use async_trait::async_trait;
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};
use serde::{Deserialize, Serialize};
use stock_trek_types::cex::cex_id::CexId;

pub const CONNECTOR_HEALTH_STREAM_PREFIX: &str = "portfolio.connector.health.v1";
pub const CONNECTOR_HEALTH_SUBJECT: &str = "portfolio.connector.health.v1.events";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorState {
    Connecting,
    Connected,
    Disconnected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorHealth {
    cex: CexId,
    state: ConnectorState,
    subscriptions: u64,
    last_update_age_seconds: Option<u64>,
    occurred_at: DateTime<Utc>,
}

impl ConnectorHealth {
    pub fn new(
        cex: CexId,
        state: ConnectorState,
        subscriptions: u64,
        last_update_age_seconds: Option<u64>,
        occurred_at: DateTime<Utc>,
    ) -> Self {
        Self {
            cex,
            state,
            subscriptions,
            last_update_age_seconds,
            occurred_at,
        }
    }

    pub fn cex(&self) -> CexId {
        self.cex
    }

    pub fn state(&self) -> ConnectorState {
        self.state
    }

    pub fn subscriptions(&self) -> u64 {
        self.subscriptions
    }

    pub fn last_update_age_seconds(&self) -> Option<u64> {
        self.last_update_age_seconds
    }

    pub fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }

    pub fn to_bytes(&self) -> BpResult<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn from_bytes(payload: &[u8]) -> BpResult<Self> {
        serde_json::from_slice(payload).map_err(BpError::external)
    }
}

#[async_trait]
pub trait ConnectorHealthSink: Send + Sync {
    async fn publish(&self, health: ConnectorHealth) -> BpResult<()>;
}

#[derive(Debug, Default)]
pub struct NoopConnectorHealthSink;

impl NoopConnectorHealthSink {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ConnectorHealthSink for NoopConnectorHealthSink {
    async fn publish(&self, _health: ConnectorHealth) -> BpResult<()> {
        Ok(())
    }
}
