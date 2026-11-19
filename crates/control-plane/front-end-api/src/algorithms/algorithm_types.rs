use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AlgorithmState {
    Active,
    #[allow(dead_code)]
    Disconnected,
    Archived,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Repository {
    pub provider: String,
    pub host: String,
    pub repo: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AlgorithmSummary {
    pub algorithm_id: String,
    pub name: String,
    pub repository: Repository,
    pub default_ref: String,
    pub state: AlgorithmState,
    pub version: u64,
    pub latest_ready_build_id: Option<String>,
}

impl AlgorithmSummary {
    pub fn default_git_ref(&self) -> String {
        let default_ref = &self.default_ref;
        if default_ref.starts_with("heads/") || default_ref.starts_with("tags/") {
            default_ref.clone()
        } else {
            format!("heads/{default_ref}")
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildState {
    Queued,
    Building,
    Ready,
    Failed,
}

impl BuildState {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "building" => Some(Self::Building),
            "ready" => Some(Self::Ready),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    pub fn is_incomplete(&self) -> bool {
        matches!(self, Self::Queued | Self::Building)
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildSummary {
    pub build_id: String,
    pub algorithm_id: String,
    #[serde(rename = "ref")]
    pub git_ref: String,
    pub commit: String,
    pub state: BuildState,
    pub error: Option<String>,
    pub log_available: bool,
    #[serde(with = "crate::api::timestamp::milliseconds")]
    pub created_at: DateTime<Utc>,
    #[serde(with = "crate::api::timestamp::milliseconds::option")]
    pub completed_at: Option<DateTime<Utc>>,
    pub stream_sequence: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TriggerBuildRequest {
    #[serde(rename = "ref")]
    pub git_ref: Option<String>,
    pub commit: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Strategy {
    pub name: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildLogLink {
    pub url: String,
    #[serde(with = "crate::api::timestamp::milliseconds")]
    pub expires_at: DateTime<Utc>,
}
