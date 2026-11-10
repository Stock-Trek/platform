use crate::{ApiToken, TokenAuditEvent};
use chrono::Utc;
use platform_errors::{BpError, BpResult};
use rand::{Rng, distributions::Alphanumeric};
use serde_json::{Map, Value};

pub(crate) const AUDIT_OUTBOX_FIELD: &str = "audit_outbox";
pub(crate) const TOMBSTONE_FIELD: &str = "__platform_deleted__";

pub(crate) struct TokenAuditOutbox;

impl TokenAuditOutbox {
    pub(crate) fn ensure_event_id(event: &mut TokenAuditEvent) {
        if event.event_id.is_none() {
            event.event_id = Some(Self::generate_event_id());
        }
    }

    pub(crate) fn ensure_events_have_ids(events: &mut [TokenAuditEvent]) {
        for event in events {
            Self::ensure_event_id(event);
        }
    }

    fn generate_event_id() -> String {
        let random: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();
        format!("{}-{random}", Utc::now().timestamp_micros())
    }

    pub(crate) fn from_document(value: &Value) -> Vec<TokenAuditEvent> {
        value
            .get(AUDIT_OUTBOX_FIELD)
            .and_then(Value::as_array)
            .map(|events| {
                events
                    .iter()
                    .filter_map(|event| serde_json::from_value(event.clone()).ok())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn token_document(token: &ApiToken, outbox: &[TokenAuditEvent]) -> Value {
        let mut document = match token.to_value() {
            Value::Object(document) => document,
            _ => Map::new(),
        };
        Self::insert(&mut document, outbox);
        Value::Object(document)
    }

    pub(crate) fn tombstone_document(outbox: &[TokenAuditEvent]) -> Value {
        let mut document = Map::new();
        document.insert(TOMBSTONE_FIELD.to_string(), Value::Bool(true));
        Self::insert(&mut document, outbox);
        Value::Object(document)
    }

    fn insert(document: &mut Map<String, Value>, outbox: &[TokenAuditEvent]) {
        if !outbox.is_empty() {
            document.insert(
                AUDIT_OUTBOX_FIELD.to_string(),
                serde_json::to_value(outbox).unwrap_or(Value::Array(Vec::new())),
            );
        }
    }

    pub(crate) fn remove_events(outbox: &mut Vec<TokenAuditEvent>, event_id: &str) {
        outbox.retain(|event| event.event_id() != Some(event_id));
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
}
