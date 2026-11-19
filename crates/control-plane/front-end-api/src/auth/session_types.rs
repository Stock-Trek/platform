use crate::auth::Scope;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LoginRequest {
    pub login_id: String,
    pub login_secret: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RefreshRequest {
    pub refresh_token: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct StepUpRequest {
    pub login_secret: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SessionTokens {
    pub token: String,
    #[serde(with = "crate::api::timestamp::milliseconds")]
    pub expires_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "crate::api::timestamp::milliseconds::option"
    )]
    pub refresh_expires_at: Option<DateTime<Utc>>,
    pub owner: String,
    pub scope: Vec<Scope>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StepUpTokens {
    pub token: String,
    #[serde(with = "crate::api::timestamp::milliseconds")]
    pub expires_at: DateTime<Utc>,
}
