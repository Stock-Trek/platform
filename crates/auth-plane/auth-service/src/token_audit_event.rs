use crate::token_audit_action::TokenAuditAction;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TokenAuditEvent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub action: TokenAuditAction,
    pub token_id: Option<String>,
    pub owner: Option<String>,
    pub success: bool,
    pub detail: Option<String>,
}

impl TokenAuditEvent {
    pub fn new(action: TokenAuditAction, timestamp: DateTime<Utc>) -> Self {
        Self {
            event_id: None,
            timestamp,
            action,
            token_id: None,
            owner: None,
            success: true,
            detail: None,
        }
    }

    pub fn with_token(mut self, token_id: impl Into<String>, owner: Option<&str>) -> Self {
        self.token_id = Some(token_id.into());
        self.owner = owner.map(str::to_string);
        self
    }

    pub fn with_event_id(mut self, event_id: impl Into<String>) -> Self {
        self.event_id = Some(event_id.into());
        self
    }

    pub fn failure(mut self, detail: impl Into<String>) -> Self {
        self.success = false;
        self.detail = Some(detail.into());
        self
    }

    pub fn event_id(&self) -> Option<&str> {
        self.event_id.as_deref()
    }
}
