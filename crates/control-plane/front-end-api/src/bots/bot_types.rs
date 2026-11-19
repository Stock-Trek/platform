use crate::api::Problem;
use crate::bots::cron::{CronExpression, TimezoneName};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const MIN_INTERVAL_SECONDS: u32 = 10;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum Schedule {
    Interval {
        every_seconds: u32,
        misfire_grace_seconds: Option<u32>,
    },
    Cron {
        expression: String,
        timezone: String,
        misfire_grace_seconds: Option<u32>,
    },
}

impl Schedule {
    pub fn validated(self) -> Result<Self, Problem> {
        match &self {
            Self::Interval {
                every_seconds,
                misfire_grace_seconds,
            } => {
                if *every_seconds < MIN_INTERVAL_SECONDS {
                    return Err(Problem::validation_failed(
                        "interval schedules need at least 10 seconds",
                    ));
                }
                Ok(Self::Interval {
                    every_seconds: *every_seconds,
                    misfire_grace_seconds: Some(
                        misfire_grace_seconds
                            .unwrap_or_else(|| Self::interval_grace(*every_seconds)),
                    ),
                })
            }
            Self::Cron {
                expression,
                timezone,
                misfire_grace_seconds,
            } => {
                CronExpression::validated(expression)?;
                TimezoneName::validated(timezone)?;
                Ok(Self::Cron {
                    expression: expression.clone(),
                    timezone: timezone.clone(),
                    misfire_grace_seconds: Some(misfire_grace_seconds.unwrap_or(60)),
                })
            }
        }
    }

    fn interval_grace(every_seconds: u32) -> u32 {
        every_seconds.min(60)
    }

    pub fn runs_per_day(&self) -> u64 {
        match self {
            Self::Interval { every_seconds, .. } => (86_400 / u64::from(*every_seconds)).max(1),
            Self::Cron { expression, .. } => CronExpression::runs_per_day(expression),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BotState {
    Enabled,
    Disabled,
    Archived,
}

impl BotState {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "enabled" => Some(Self::Enabled),
            "disabled" => Some(Self::Disabled),
            "archived" => Some(Self::Archived),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Bot {
    pub bot_id: String,
    pub name: String,
    pub algorithm_id: String,
    pub build_id: String,
    pub strategy_name: String,
    pub account_ids: Vec<String>,
    pub schedule: Schedule,
    pub state: BotState,
    pub active_run_id: Option<String>,
    pub version: u64,
    #[serde(with = "crate::api::timestamp::milliseconds")]
    pub created_at: DateTime<Utc>,
    #[serde(with = "crate::api::timestamp::milliseconds")]
    pub updated_at: DateTime<Utc>,
    pub stream_sequence: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateBotRequest {
    pub name: String,
    pub algorithm_id: String,
    pub build_id: String,
    pub strategy_name: String,
    pub account_ids: Vec<String>,
    pub schedule: Schedule,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PatchBotRequest {
    pub name: Option<String>,
    pub build_id: Option<String>,
    pub strategy_name: Option<String>,
    pub account_ids: Option<Vec<String>>,
    pub schedule: Option<Schedule>,
}

impl PatchBotRequest {
    const REQUIRED_FIELDS: [&str; 5] =
        ["name", "buildId", "strategyName", "accountIds", "schedule"];

    pub fn merge_patch(value: &serde_json::Value) -> Result<Self, Problem> {
        for field in Self::REQUIRED_FIELDS {
            if value.get(field).is_some_and(|field| field.is_null()) {
                return Err(Problem::validation_failed(format!(
                    "{field} cannot be set to null"
                )));
            }
        }
        serde_json::from_value(value.clone())
            .map_err(|error| Problem::validation_failed(error.to_string()))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KillRequest {
    pub account_ids: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KillResponse {
    pub disabled_bot_ids: Vec<String>,
    pub canceled_run_ids: Vec<String>,
}
