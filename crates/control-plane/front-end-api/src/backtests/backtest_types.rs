use crate::api::Page;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BacktestState {
    Queued,
    Running,
    Completed,
    Failed,
    Canceled,
}

impl BacktestState {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "canceled" => Some(Self::Canceled),
            _ => None,
        }
    }

    pub(crate) fn is_active(&self) -> bool {
        matches!(self, Self::Queued | Self::Running)
    }

    pub(crate) fn is_terminal(&self) -> bool {
        !self.is_active()
    }

    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CreateBacktestRequest {
    pub algorithm_id: String,
    pub build_id: String,
    pub strategy_name: String,
    pub seed: Option<i64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BacktestCreated {
    pub backtest_id: String,
    pub state: BacktestState,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BacktestError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BacktestProgress {
    pub completed: u64,
    pub total: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BacktestStatus {
    pub backtest_id: String,
    pub algorithm_id: String,
    pub build_id: String,
    pub strategy_name: String,
    pub state: BacktestState,
    pub error: Option<BacktestError>,
    pub seed: i64,
    pub progress: BacktestProgress,
    #[serde(with = "crate::api::timestamp::milliseconds")]
    pub created_at: DateTime<Utc>,
    #[serde(with = "crate::api::timestamp::milliseconds::option")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(with = "crate::api::timestamp::milliseconds::option")]
    pub completed_at: Option<DateTime<Utc>>,
    pub stream_sequence: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BacktestResultItem {
    pub sequence: u64,
    pub simulation: u64,
    pub result: serde_json::Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BacktestResultsPage {
    #[serde(flatten)]
    pub page: Page<BacktestResultItem>,
    pub state: BacktestState,
    pub complete: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportLink {
    pub url: String,
    #[serde(with = "crate::api::timestamp::milliseconds")]
    pub expires_at: DateTime<Utc>,
    pub format: &'static str,
}
