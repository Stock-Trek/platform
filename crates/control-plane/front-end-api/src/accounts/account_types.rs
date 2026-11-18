use crate::api::Problem;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorState {
    Connecting,
    Connected,
    #[allow(dead_code)]
    Disconnected,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorReason {
    InvalidCredentials,
    #[allow(dead_code)]
    InsufficientPermissions,
    #[allow(dead_code)]
    ExchangeUnavailable,
    #[allow(dead_code)]
    Unknown,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Connector {
    pub state: ConnectorState,
    pub reason: Option<ConnectorReason>,
    pub since: DateTime<Utc>,
}

impl Connector {
    pub fn starting(now: DateTime<Utc>) -> Self {
        Self {
            state: ConnectorState::Connecting,
            reason: None,
            since: now,
        }
    }

    pub fn connected(now: DateTime<Utc>) -> Self {
        Self {
            state: ConnectorState::Connected,
            reason: None,
            since: now,
        }
    }

    pub fn failed(reason: ConnectorReason, now: DateTime<Utc>) -> Self {
        Self {
            state: ConnectorState::Error,
            reason: Some(reason),
            since: now,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountSummary {
    pub account_id: String,
    pub cex_id: String,
    pub label: String,
    pub active: bool,
    pub connector: Connector,
    pub version: u64,
    pub updated_at: DateTime<Utc>,
}

impl AccountSummary {
    pub fn etag_version(&self) -> u64 {
        self.version
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LinkAccountRequest {
    pub cex_id: String,
    pub label: String,
    pub api_key: String,
    pub api_secret: String,
}

impl LinkAccountRequest {
    pub fn validated(self) -> Result<Self, Problem> {
        let length = self.label.chars().count();
        if (1..=64).contains(&length) {
            Ok(self)
        } else {
            Err(Problem::validation_failed(
                "label must be 1 to 64 characters",
            ))
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CredentialsRequest {
    pub api_key: String,
    pub api_secret: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ForceUnlinkResponse {
    pub disabled_bot_ids: Vec<String>,
    pub canceled_run_ids: Vec<String>,
}
