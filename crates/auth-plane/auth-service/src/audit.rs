use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use platform_errors::BpResult;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use strum::{Display, EnumString};

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

    pub fn failure(mut self, detail: impl Into<String>) -> Self {
        self.success = false;
        self.detail = Some(detail.into());
        self
    }
}

/// Receives token audit events.
///
/// Delivery is awaited so implementations can provide backpressure and report
/// failures to the operation that produced the event.
#[async_trait]
pub trait TokenAuditLog: Send + Sync {
    async fn record(&self, event: TokenAuditEvent) -> BpResult<()>;
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

#[async_trait]
impl TokenAuditLog for MemoryTokenAuditLog {
    async fn record(&self, event: TokenAuditEvent) -> BpResult<()> {
        let mut events = self.events.lock();
        if self.capacity == 0 {
            return Ok(());
        }
        if events.len() >= self.capacity {
            events.pop_front();
        }
        events.push_back(event);
        Ok(())
    }
}
