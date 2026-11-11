use crate::TokenAuditEvent;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use platform_errors::BpResult;
use serde::{Deserialize, Serialize};

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

    pub(crate) fn ensure_event_ids(&mut self) {
        for event in &mut self.events {
            crate::token_audit_outbox::TokenAuditOutbox::ensure_event_id(event);
        }
    }

    pub(crate) fn merge(&mut self, other: Self) {
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
    pub(crate) fn new(id: impl Into<String>, usage: TokenUsage) -> Self {
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
    async fn enqueue(&self, usage: TokenUsage) -> BpResult<()>;

    async fn reserve(&self, limit: usize) -> BpResult<Vec<TokenUsageReservation>>;

    async fn acknowledge(&self, reservation: &TokenUsageReservation) -> BpResult<()>;

    async fn release(&self, reservation: &TokenUsageReservation) -> BpResult<()>;
}

impl crate::auth_service::AuthService {
    pub(crate) async fn enqueue_usage(&self, mut usage: TokenUsage) -> BpResult<()> {
        usage.ensure_event_ids();
        self.usage.enqueue(usage).await
    }
}
