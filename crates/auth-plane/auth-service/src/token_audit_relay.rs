use crate::{TokenAuditLog, TokenStore, token_audit_outbox::TokenAuditOutbox};
use chrono::{DateTime, Utc};
use platform_errors::{ErrorType, PlatformError, PlatformResult};
use platform_metrics::AuthAuditRelayMetrics;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

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

    pub fn store(&self) -> Arc<dyn TokenStore> {
        self.store.clone()
    }

    pub async fn drain(&self) -> PlatformResult<usize> {
        let started = Instant::now();
        let result = self.drain_once().await;
        AuthAuditRelayMetrics::record_drain_duration(started.elapsed());
        result
    }

    async fn drain_once(&self) -> PlatformResult<usize> {
        let rotations = self
            .store
            .pending_rotations()
            .await
            .map_err(|error| Self::record_failure("pending_rotations", error))?;
        let audits = self
            .store
            .pending_audits()
            .await
            .map_err(|error| Self::record_failure("pending_audits", error))?;
        AuthAuditRelayMetrics::record_backlog(
            audits.len(),
            Self::oldest_pending_age(&audits),
            rotations.len(),
        );

        let mut delivered = 0;
        for transaction in rotations {
            self.store
                .materialize_rotation(&transaction)
                .await
                .map_err(|error| Self::record_failure("materialize_rotation", error))?;
            for id in [transaction.original().id(), transaction.replacement().id()] {
                match self.store.get(id).await {
                    Ok(Some(token)) => {
                        if let Err(error) = self.store.index_owner_token(&token).await {
                            tracing::warn!(
                                %error,
                                token_id = token.id(),
                                "failed to update owner index"
                            );
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(%error, token_id = id, "failed to read rotated token");
                    }
                }
            }
            for event in transaction.events() {
                self.sink
                    .record(event.clone())
                    .await
                    .map_err(|error| Self::record_failure("sink", error))?;
                AuthAuditRelayMetrics::record_delivered(1);
                delivered += 1;
            }
            self.store
                .acknowledge_rotation(transaction.transaction_id())
                .await
                .map_err(|error| Self::record_failure("acknowledge_rotation", error))?;
        }

        for event in audits {
            let event_id = TokenAuditOutbox::require_event_id(&event)
                .map_err(|error| Self::record_failure("event_id", error))?
                .to_string();
            self.sink
                .record(event)
                .await
                .map_err(|error| Self::record_failure("sink", error))?;
            AuthAuditRelayMetrics::record_delivered(1);
            delivered += 1;
            self.store
                .acknowledge_audit(&event_id)
                .await
                .map_err(|error| Self::record_failure("acknowledge_audit", error))?;
        }

        AuthAuditRelayMetrics::record_backlog(0, None, 0);
        Ok(delivered)
    }

    pub async fn relay(&self) -> PlatformResult<usize> {
        self.drain().await
    }

    fn record_failure(operation: &'static str, error: PlatformError) -> PlatformError {
        AuthAuditRelayMetrics::record_failure(operation, error.error_type());
        error
    }

    fn oldest_pending_age(events: &[crate::TokenAuditEvent]) -> Option<Duration> {
        let oldest: DateTime<Utc> = events.iter().map(|event| event.timestamp).min()?;
        let elapsed = Utc::now().signed_duration_since(oldest);
        Some(Duration::from_secs_f64(
            elapsed.num_milliseconds().max(0) as f64 / 1_000.0,
        ))
    }
}
