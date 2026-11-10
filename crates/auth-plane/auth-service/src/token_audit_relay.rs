use crate::{TokenAuditLog, TokenStore, token_audit_outbox::TokenAuditOutbox};
use platform_errors::BpResult;
use std::sync::Arc;

/// Drains pending token audit events and rotation transactions.
///
/// Events are removed only after the durable audit sink accepts them. The
/// relay therefore provides at-least-once delivery; event IDs are stable across
/// retries and can be used by the consumer as idempotency keys.
pub struct TokenAuditRelay {
    store: Arc<dyn TokenStore>,
    sink: Arc<dyn TokenAuditLog>,
}

impl TokenAuditRelay {
    pub fn new(store: Arc<dyn TokenStore>, sink: Arc<dyn TokenAuditLog>) -> Self {
        Self { store, sink }
    }

    pub async fn drain(&self) -> BpResult<usize> {
        let mut delivered = 0;
        for transaction in self.store.pending_rotations().await? {
            self.store.materialize_rotation(&transaction).await?;
            for event in transaction.events() {
                self.sink.record(event.clone()).await?;
                delivered += 1;
            }
            self.store
                .acknowledge_rotation(transaction.transaction_id())
                .await?;
        }
        for event in self.store.pending_audits().await? {
            let event_id = TokenAuditOutbox::require_event_id(&event)?.to_string();
            self.sink.record(event).await?;
            self.store.acknowledge_audit(&event_id).await?;
            delivered += 1;
        }
        Ok(delivered)
    }

    pub async fn relay(&self) -> BpResult<usize> {
        self.drain().await
    }
}
