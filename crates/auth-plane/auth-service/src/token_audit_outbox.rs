use crate::TokenAuditEvent;
use chrono::Utc;
use platform_errors::{BpError, BpResult};
use rand::{Rng, distributions::Alphanumeric};
use serde_json::{Map, Value};
use std::sync::atomic::{AtomicU64, Ordering};

static EVENT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(crate) const AUDIT_EVENT_FIELD: &str = "__platform_audit_event__";
pub(crate) const AUDIT_EVENT_PREFIX: &str = "audit/events/";

pub(crate) struct TokenAuditOutbox;

impl TokenAuditOutbox {
    pub(crate) fn ensure_event_id(event: &mut TokenAuditEvent) {
        if event.event_id.is_none() {
            event.event_id = Some(Self::generate_event_id());
        }
    }

    fn generate_event_id() -> String {
        let random: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();
        let sequence = EVENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        format!(
            "{:020}-{sequence:020}-{random}",
            Utc::now().timestamp_micros()
        )
    }

    pub(crate) fn standalone_document(event: &TokenAuditEvent) -> BpResult<Value> {
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

    pub(crate) fn standalone_event(value: &Value) -> BpResult<Option<TokenAuditEvent>> {
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

    pub(crate) fn require_event_id(event: &TokenAuditEvent) -> BpResult<&str> {
        event.event_id().ok_or_else(|| {
            BpError::auth_store(
                "relay",
                event.token_id.as_deref().unwrap_or("audit"),
                false,
                "pending audit event is missing an event id".to_string(),
            )
        })
    }

    fn malformed(detail: String) -> BpError {
        BpError::auth_store("read", "audit-outbox", false, detail)
    }
}
