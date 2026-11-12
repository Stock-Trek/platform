use super::ConnectorState;
use chrono::{DateTime, Utc};
use platform_errors::{BuildError, BuildResult};
use serde::{Deserialize, Serialize};
use stock_trek_types::cex::cex_id::CexId;

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

    pub fn to_bytes(&self) -> BuildResult<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn from_bytes(payload: &[u8]) -> BuildResult<Self> {
        serde_json::from_slice(payload).map_err(BuildError::external)
    }
}
