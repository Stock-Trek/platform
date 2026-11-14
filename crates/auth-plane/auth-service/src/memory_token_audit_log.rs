use crate::{token_audit_event::TokenAuditEvent, token_audit_log::TokenAuditLog};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::PlatformResult;
use std::collections::VecDeque;

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
