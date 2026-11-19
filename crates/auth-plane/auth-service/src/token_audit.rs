use crate::random_string::RandomString;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use platform_errors::{PlatformError, PlatformResult};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use strum::{Display, EnumString};

static EVENT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(crate) const AUDIT_EVENT_FIELD: &str = "__platform_audit_event__";
pub(crate) const AUDIT_EVENT_PREFIX: &str = "audit/events/";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum TokenAuditAction {
    Issued,
    Deleted,
    Authenticated,
    AuthenticationFailed,
    Signed,
    SignatureVerified,
    SignatureRejected,
    Rotated,
    Revoked,
}

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

#[async_trait]
pub trait TokenAuditLog: Send + Sync {
    async fn record(&self, event: TokenAuditEvent) -> PlatformResult<()>;
}

pub(crate) struct TokenAuditOutbox;

impl TokenAuditOutbox {
    pub(crate) fn ensure_event_id(event: &mut TokenAuditEvent) {
        if event.event_id.is_none() {
            event.event_id = Some(Self::generate_event_id());
        }
    }

    fn generate_event_id() -> String {
        let random = RandomString::generate(16);
        let sequence = EVENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        format!(
            "{:020}-{sequence:020}-{random}",
            Utc::now().timestamp_micros()
        )
    }

    pub(crate) fn standalone_document(event: &TokenAuditEvent) -> PlatformResult<Value> {
        let mut document = Map::new();
        document.insert(AUDIT_EVENT_FIELD.to_string(), Value::Bool(true));
        document.insert(
            "event".to_string(),
            serde_json::to_value(event).map_err(|error| {
                Self::malformed(format!("could not serialize audit event: {error}"))
            })?,
        );
        Ok(Value::Object(document))
    }

    pub(crate) fn standalone_event(value: &Value) -> PlatformResult<Option<TokenAuditEvent>> {
        if !Self::is_standalone_document(value) {
            return Ok(None);
        }
        let event = value.get("event").ok_or_else(|| {
            Self::malformed("standalone audit document is missing its event".to_string())
        })?;
        let event = serde_json::from_value(event.clone())
            .map_err(|error| Self::malformed(format!("invalid standalone audit event: {error}")))?;
        Ok(Some(event))
    }

    pub(crate) fn is_standalone_document(value: &Value) -> bool {
        value
            .get(AUDIT_EVENT_FIELD)
            .and_then(Value::as_bool)
            .is_some_and(|standalone| standalone)
    }

    pub(crate) fn is_storage_key(key: &str) -> bool {
        key.starts_with(AUDIT_EVENT_PREFIX)
    }

    pub(crate) fn storage_key(event_id: &str) -> String {
        format!("{AUDIT_EVENT_PREFIX}{event_id}")
    }

    pub(crate) fn require_event_id(event: &TokenAuditEvent) -> PlatformResult<&str> {
        event.event_id().ok_or_else(|| {
            PlatformError::auth_store(
                "relay",
                event.token_id.as_deref().unwrap_or("audit"),
                false,
                "pending audit event is missing an event id".to_string(),
            )
        })
    }

    fn malformed(detail: String) -> PlatformError {
        PlatformError::auth_store("read", "audit-outbox", false, detail)
    }
}
