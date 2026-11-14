use crate::{TokenAuditEvent, secret_input::RandomString};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use platform_errors::{PlatformError, PlatformResult};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::atomic::{AtomicU64, Ordering};

/// Usage work that can be applied after the authentication hot path returns.
///
/// Audit events are carried with the usage update so a single queued record
/// can update `last_used_at` and preserve every event produced by the request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    token_id: Option<String>,
    used_at: Option<DateTime<Utc>>,
    events: Vec<TokenAuditEvent>,
}

impl TokenUsage {
    pub fn for_token(
        token_id: impl Into<String>,
        used_at: DateTime<Utc>,
        events: Vec<TokenAuditEvent>,
    ) -> Self {
        Self {
            token_id: Some(token_id.into()),
            used_at: Some(used_at),
            events,
        }
    }

    pub fn audit(event: TokenAuditEvent) -> Self {
        Self {
            token_id: event.token_id.clone(),
            used_at: None,
            events: vec![event],
        }
    }

    pub fn token_id(&self) -> Option<&str> {
        self.token_id.as_deref()
    }

    pub fn used_at(&self) -> Option<DateTime<Utc>> {
        self.used_at
    }

    pub fn events(&self) -> &[TokenAuditEvent] {
        &self.events
    }

    pub fn ensure_event_ids(&mut self) {
        for event in &mut self.events {
            TokenAuditOutbox::ensure_event_id(event);
        }
    }

    pub fn merge(&mut self, other: Self) {
        if self.token_id.is_none() {
            self.token_id = other.token_id;
        }
        self.used_at = match (self.used_at, other.used_at) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left, right) => left.or(right),
        };
        self.events.extend(other.events);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenUsageReservation {
    id: String,
    usage: TokenUsage,
}

impl TokenUsageReservation {
    pub fn new(id: impl Into<String>, usage: TokenUsage) -> Self {
        Self {
            id: id.into(),
            usage,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn usage(&self) -> &TokenUsage {
        &self.usage
    }

    pub fn into_usage(self) -> TokenUsage {
        self.usage
    }
}

/// Durable queue for usage updates that must not block authentication.
///
/// Implementations must provide at-least-once delivery. A reservation remains
/// recoverable until it is acknowledged, and releasing it makes the same
/// logical record available to another worker.
#[async_trait]
pub trait TokenUsageQueue: Send + Sync {
    async fn enqueue(&self, usage: TokenUsage) -> PlatformResult<()>;

    async fn reserve(&self, limit: usize) -> PlatformResult<Vec<TokenUsageReservation>>;

    async fn acknowledge(&self, reservation: &TokenUsageReservation) -> PlatformResult<()>;

    async fn release(&self, reservation: &TokenUsageReservation) -> PlatformResult<()>;
}

impl crate::auth_service::AuthService {
    pub async fn enqueue_usage(&self, mut usage: TokenUsage) -> PlatformResult<()> {
        usage.ensure_event_ids();
        self.usage.enqueue(usage).await
    }
}
static EVENT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub const AUDIT_EVENT_FIELD: &str = "__platform_audit_event__";
pub const AUDIT_EVENT_PREFIX: &str = "audit/events/";

pub struct TokenAuditOutbox;

impl TokenAuditOutbox {
    pub fn ensure_event_id(event: &mut TokenAuditEvent) {
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

    pub fn standalone_document(event: &TokenAuditEvent) -> PlatformResult<Value> {
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

    pub fn standalone_event(value: &Value) -> PlatformResult<Option<TokenAuditEvent>> {
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

    pub fn is_standalone_document(value: &Value) -> bool {
        value
            .get(AUDIT_EVENT_FIELD)
            .and_then(Value::as_bool)
            .is_some_and(|standalone| standalone)
    }

    pub fn is_storage_key(key: &str) -> bool {
        key.starts_with(AUDIT_EVENT_PREFIX)
    }

    pub fn storage_key(event_id: &str) -> String {
        format!("{AUDIT_EVENT_PREFIX}{event_id}")
    }

    pub fn require_event_id(event: &TokenAuditEvent) -> PlatformResult<&str> {
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
