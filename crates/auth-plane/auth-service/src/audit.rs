use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use platform_errors::PlatformResult;
use platform_sqs::Sqs;
use platform_time::Deadline;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, SystemTime},
};
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

/// Delivers token audit events to the durable audit sink.
///
/// Token mutations do not call this directly. They first commit the event to
/// the token's outbox, and [`crate::TokenAuditRelay`] drains it afterwards.
#[async_trait]
pub trait TokenAuditLog: Send + Sync {
    async fn record(&self, event: TokenAuditEvent) -> PlatformResult<()>;
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
}

#[async_trait]
impl TokenAuditLog for MemoryTokenAuditLog {
    async fn record(&self, event: TokenAuditEvent) -> PlatformResult<()> {
        if self.capacity == 0 {
            return Ok(());
        }
        let mut stored = self.events.lock();
        if stored.len() >= self.capacity {
            stored.pop_front();
        }
        stored.push_back(event);
        Ok(())
    }
}
#[async_trait]
pub trait AuditQueue: Send + Sync {
    async fn deliver(&self, events: &[TokenAuditEvent], deadline: Deadline) -> PlatformResult<()>;
}

#[async_trait]
impl AuditQueue for Sqs {
    async fn deliver(&self, events: &[TokenAuditEvent], deadline: Deadline) -> PlatformResult<()> {
        self.send_json(events, deadline).await
    }
}

/// Delivers audit events to an SQS queue.
///
/// Unlike the in-memory log, this sink is durable. It is used by
/// [`crate::TokenAuditRelay`] to drain token outboxes; a failed send is returned
/// to the relay so the event remains pending for a later retry.
#[derive(Clone)]
pub struct SqsTokenAuditLog {
    queue: Arc<dyn AuditQueue>,
    timeout: Duration,
}

impl SqsTokenAuditLog {
    pub fn new(sqs: Sqs, timeout: Duration) -> Self {
        Self {
            queue: Arc::new(sqs),
            timeout,
        }
    }

    #[cfg(test)]
    pub fn with_queue(queue: Arc<dyn AuditQueue>, timeout: Duration) -> Self {
        Self { queue, timeout }
    }

    fn deadline(&self) -> Deadline {
        Deadline::new(SystemTime::now() + self.timeout, Duration::ZERO)
    }
}

#[async_trait]
impl TokenAuditLog for SqsTokenAuditLog {
    async fn record(&self, event: TokenAuditEvent) -> PlatformResult<()> {
        self.queue
            .deliver(std::slice::from_ref(&event), self.deadline())
            .await
    }
}
