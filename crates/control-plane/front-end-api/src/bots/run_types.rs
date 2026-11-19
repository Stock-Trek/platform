use crate::api::Page;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RunState {
    Pending,
    Running,
    Completed,
    Failed,
    Canceled,
    Skipped,
}

impl RunState {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "canceled" => Some(Self::Canceled),
            "skipped" => Some(Self::Skipped),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RunTrigger {
    #[allow(dead_code)]
    Schedule,
    AdHoc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SkipReason {
    #[allow(dead_code)]
    Overlap,
    #[allow(dead_code)]
    Misfire,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RunErrorKind {
    #[allow(dead_code)]
    Client,
    #[allow(dead_code)]
    Platform,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RunErrorCode {
    InvalidContext,
    InvalidPortfolio,
    InvalidRequest,
    Timeout,
    Panic,
    ResourceLimit,
    Internal,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct RunError {
    pub kind: RunErrorKind,
    pub code: RunErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Run {
    pub run_id: String,
    pub bot_id: String,
    pub algorithm_id: String,
    pub build_id: Option<String>,
    pub strategy_name: Option<String>,
    pub trigger: RunTrigger,
    #[serde(with = "crate::api::timestamp::milliseconds")]
    pub scheduled_at: DateTime<Utc>,
    pub state: RunState,
    #[serde(with = "crate::api::timestamp::milliseconds::option")]
    pub completed_at: Option<DateTime<Utc>>,
    pub log_available: bool,
    #[serde(skip_serializing)]
    pub cancel_requested: bool,
    pub error: Option<RunError>,
    pub skip_reason: Option<SkipReason>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RunLogLink {
    pub url: String,
    #[serde(with = "crate::api::timestamp::milliseconds")]
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CancelResponse {
    pub cancel_requested: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct RunPage {
    #[serde(flatten)]
    pub page: Page<Run>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_sequence: Option<u64>,
}

#[derive(Debug, Default)]
pub(crate) struct RunFilter {
    pub bot_id: Option<String>,
    pub state: Option<RunState>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
}
