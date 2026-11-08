use crate::ApiToken;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use std::collections::VecDeque;
use strum::{Display, EnumString};

pub(crate) const MAX_AUDIT_DETAIL_LENGTH: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenAuditEvent {
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
            timestamp,
            action,
            token_id: None,
            owner: None,
            success: true,
            detail: None,
        }
    }

    pub fn with_token(mut self, token_id: impl Into<String>, owner: Option<&str>) -> Self {
        let token_id = token_id.into();
        self.token_id = ApiToken::validate_id(&token_id).ok().map(|()| token_id);
        self.owner = owner
            .and_then(|owner| ApiToken::validated_owner(owner).ok())
            .map(str::to_string);
        self
    }

    pub fn failure(mut self, detail: impl Into<String>) -> Self {
        self.success = false;
        self.detail = Some(Self::truncate(detail.into(), MAX_AUDIT_DETAIL_LENGTH));
        self
    }

    fn truncate(value: String, max_chars: usize) -> String {
        if value.chars().count() <= max_chars {
            value
        } else {
            value.chars().take(max_chars).collect()
        }
    }

    pub fn token_id(&self) -> Option<&str> {
        self.token_id.as_deref()
    }

    pub fn owner(&self) -> Option<&str> {
        self.owner.as_deref()
    }

    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }
}

pub trait TokenAuditLog: Send + Sync {
    fn record(&self, event: TokenAuditEvent);
}

#[derive(Debug)]
pub struct MemoryTokenAuditLog {
    events: Mutex<VecDeque<TokenAuditEvent>>,
    capacity: usize,
}

impl MemoryTokenAuditLog {
    pub fn new(capacity: usize) -> Self {
        Self {
            events: Mutex::new(VecDeque::new()),
            capacity,
        }
    }

    pub fn events(&self) -> Vec<TokenAuditEvent> {
        self.events.lock().iter().cloned().collect()
    }

    pub fn drain(&self) -> Vec<TokenAuditEvent> {
        std::mem::take(&mut *self.events.lock()).into()
    }
}

impl TokenAuditLog for MemoryTokenAuditLog {
    fn record(&self, event: TokenAuditEvent) {
        let mut events = self.events.lock();
        if self.capacity == 0 {
            return;
        }
        if events.len() >= self.capacity {
            events.pop_front();
        }
        events.push_back(event);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopTokenAuditLog;

impl TokenAuditLog for NoopTokenAuditLog {
    fn record(&self, _event: TokenAuditEvent) {}
}
