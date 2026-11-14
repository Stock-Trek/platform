use crate::{
    AuthService, TokenMutation, TokenUsage, TokenUsageQueue, TokenUsageReservation,
    auth_service::MAX_UPDATE_ATTEMPTS, token_store::TokenStore,
};
use platform_errors::{ErrorType, PlatformError, PlatformResult};
use platform_metrics::AuthUsageQueueMetrics;
use std::{collections::HashMap, sync::Arc, time::Instant};

/// Applies queued usage records to the token store in bounded batches.
///
/// Records for the same token are merged so a burst of authentications causes
/// one `last_used_at` write instead of one write per request. Audit event IDs
/// are generated before enqueueing, making retries idempotent.
pub struct TokenUsageWorker {
    store: Arc<dyn TokenStore>,
    queue: Arc<dyn TokenUsageQueue>,
    batch_size: usize,
}

impl TokenUsageWorker {
    pub fn new(
        store: Arc<dyn TokenStore>,
        queue: Arc<dyn TokenUsageQueue>,
        batch_size: usize,
    ) -> Self {
        Self {
            store,
            queue,
            batch_size,
        }
    }

    pub async fn drain(&self) -> PlatformResult<usize> {
        let started = Instant::now();
        let result = self.drain_all().await;
        AuthUsageQueueMetrics::record_drain_duration(started.elapsed());
        match &result {
            Ok(applied) => AuthUsageQueueMetrics::record_delivered(*applied),
            Err(error) => AuthUsageQueueMetrics::record_failure("drain", error.error_type()),
        }
        result
    }

    async fn drain_all(&self) -> PlatformResult<usize> {
        let mut applied = 0;
        loop {
            let (batch_applied, reserved) = self.drain_batch().await?;
            applied += batch_applied;
            if reserved < self.batch_size {
                return Ok(applied);
            }
        }
    }

    async fn drain_batch(&self) -> PlatformResult<(usize, usize)> {
        let reservations = self.queue.reserve(self.batch_size).await?;
        let reserved = reservations.len();
        if reservations.is_empty() {
            return Ok((0, 0));
        }
        let groups = Self::group_reservations(&reservations);
        let mut applied = 0;
        let mut first_error = None;
        for (usage, indexes) in groups {
            match Self::apply_usage(&self.store, &usage).await {
                Ok(()) => {
                    for index in indexes {
                        let reservation = &reservations[index];
                        match self.queue.acknowledge(reservation).await {
                            Ok(()) => applied += 1,
                            Err(error) => {
                                if let Err(release_error) = self.queue.release(reservation).await {
                                    tracing::error!(
                                        %release_error,
                                        "failed to release token usage after acknowledgement failure"
                                    );
                                }
                                if first_error.is_none() {
                                    first_error = Some(error);
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    for index in indexes {
                        if let Err(release_error) = self.queue.release(&reservations[index]).await {
                            tracing::error!(
                                %release_error,
                                "failed to release token usage reservation"
                            );
                        }
                    }
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok((applied, reserved)),
        }
    }

    fn group_reservations(reservations: &[TokenUsageReservation]) -> Vec<(TokenUsage, Vec<usize>)> {
        let mut groups: Vec<(TokenUsage, Vec<usize>)> = Vec::new();
        let mut indexes: HashMap<Option<String>, usize> = HashMap::new();
        for (position, reservation) in reservations.iter().enumerate() {
            let key = reservation.usage().token_id().map(str::to_string);
            match indexes.get(&key).copied() {
                Some(index) => {
                    groups[index].0.merge(reservation.usage().clone());
                    groups[index].1.push(position);
                }
                None => {
                    indexes.insert(key, groups.len());
                    groups.push((reservation.usage().clone(), vec![position]));
                }
            }
        }
        groups
    }

    async fn apply_usage(store: &Arc<dyn TokenStore>, usage: &TokenUsage) -> PlatformResult<()> {
        let mut events = usage.events().to_vec();
        for event in &mut events {
            crate::token_usage::TokenAuditOutbox::ensure_event_id(event);
        }
        let Some(token_id) = usage.token_id() else {
            return Self::append_events(store, events).await;
        };
        let Some(used_at) = usage.used_at() else {
            return Self::append_events(store, events).await;
        };
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            let Some(current) = store.get(token_id).await? else {
                return Self::append_events(store, events).await;
            };
            let mut updated = current.clone();
            updated.metadata.record_use(used_at);
            if updated.to_value() == current.to_value() {
                return Self::append_events(store, events).await;
            }
            let mutation = TokenMutation::Update {
                expected: current,
                token: updated,
            };
            match store.commit_events(mutation, events.clone()).await {
                Ok(true) => return Ok(()),
                Ok(false) => return Self::append_events(store, events).await,
                Err(error) if AuthService::is_conflict(&error) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(PlatformError::token_conflict(token_id))
    }

    async fn append_events(
        store: &Arc<dyn TokenStore>,
        events: Vec<crate::TokenAuditEvent>,
    ) -> PlatformResult<()> {
        for event in events {
            store.append_audit(event).await?;
        }
        Ok(())
    }
}
