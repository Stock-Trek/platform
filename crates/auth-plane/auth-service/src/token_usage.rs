use crate::TokenAuditEvent;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
            crate::token_audit_outbox::TokenAuditOutbox::ensure_event_id(event);
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
