use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use std::sync::Arc;

const DEFAULT_CAPACITY: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

impl TokenAuditAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Issued => "issued",
            Self::Deleted => "deleted",
            Self::Authenticated => "authenticated",
            Self::AuthenticationFailed => "authentication_failed",
            Self::Signed => "signed",
            Self::SignatureVerified => "signature_verified",
            Self::SignatureRejected => "signature_rejected",
            Self::Rotated => "rotated",
            Self::Revoked => "revoked",
        }
    }
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
        self.token_id = Some(token_id.into());
        self.owner = owner.map(str::to_string);
        self
    }

    pub fn success(mut self) -> Self {
        self.success = true;
        self
    }

    pub fn failure(mut self, detail: impl Into<String>) -> Self {
        self.success = false;
        self.detail = Some(detail.into());
        self
    }

    pub fn action(&self) -> TokenAuditAction {
        self.action
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
    events: Mutex<Vec<TokenAuditEvent>>,
    capacity: usize,
}

impl Default for MemoryTokenAuditLog {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryTokenAuditLog {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            capacity: capacity.max(1),
        }
    }

    pub fn events(&self) -> Vec<TokenAuditEvent> {
        self.events.lock().clone()
    }

    pub fn drain(&self) -> Vec<TokenAuditEvent> {
        std::mem::take(&mut *self.events.lock())
    }

    pub fn clear(&self) {
        self.events.lock().clear();
    }

    pub fn len(&self) -> usize {
        self.events.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl TokenAuditLog for MemoryTokenAuditLog {
    fn record(&self, event: TokenAuditEvent) {
        let mut events = self.events.lock();
        if events.len() == self.capacity {
            events.remove(0);
        }
        events.push(event);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopTokenAuditLog;

impl TokenAuditLog for NoopTokenAuditLog {
    fn record(&self, _event: TokenAuditEvent) {}
}

impl TokenAuditLog for Arc<dyn TokenAuditLog> {
    fn record(&self, event: TokenAuditEvent) {
        self.as_ref().record(event);
    }
}
