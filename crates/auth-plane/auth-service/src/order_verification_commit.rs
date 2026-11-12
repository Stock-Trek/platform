use crate::{
    ApiToken, AuthService, ConsumeResult, OrderRequestContext, OrderRequestVerification, ReplayKey,
    ReplayReservation, ReplayStoreError, TokenAuditAction, TokenAuditEvent, TokenUsage,
};
use chrono::{DateTime, Utc};
use platform_errors::{BuildError, BuildResult};
use platform_metrics::auth::AuthReplayMetrics;

impl AuthService {
    pub(crate) async fn reserve_order_request_replay(
        &self,
        context: &OrderRequestContext,
        now: DateTime<Utc>,
    ) -> BuildResult<Option<ReplayReservation>> {
        let replay_key = ReplayKey::new(context.token_id(), context.nonce());
        match self
            .replay
            .consume(replay_key, context.expires_at(), now)
            .await
        {
            Ok(ConsumeResult::Consumed(reservation)) => Ok(Some(reservation)),
            Ok(ConsumeResult::Replayed | ConsumeResult::Expired) => Ok(None),
            Err(error) => Err(Self::replay_store_error("consume", error)),
        }
    }

    pub(crate) async fn commit_signature_use(
        &self,
        token: ApiToken,
        now: DateTime<Utc>,
        reservation: &ReplayReservation,
    ) -> BuildResult<OrderRequestVerification> {
        let event = TokenAuditEvent::new(TokenAuditAction::SignatureVerified, now)
            .with_token(token.id(), Some(token.owner()));
        let usage = TokenUsage::for_token(token.id(), now, vec![event]);
        if let Err(error) = self.enqueue_usage(usage).await {
            tracing::error!(%error, "failed to enqueue token usage");
            self.release_replay_after_failure(reservation).await;
            return Err(error);
        }
        Ok(OrderRequestVerification::Valid)
    }

    fn replay_store_error(operation: &'static str, error: ReplayStoreError) -> BuildError {
        AuthReplayMetrics::record_store_failure(operation);
        let retryable = error.is_retryable();
        BuildError::auth_store(operation, "replay-store", retryable, error)
    }

    async fn release_replay(&self, reservation: &ReplayReservation) -> BuildResult<()> {
        self.replay
            .release(reservation)
            .await
            .map_err(|error| Self::replay_store_error("release", error))
    }

    /// Releases a replay reservation after an operation has already failed. The
    /// original failure is more useful to the caller, so release errors are
    /// logged instead of replacing it.
    async fn release_replay_after_failure(&self, reservation: &ReplayReservation) {
        if let Err(error) = self.release_replay(reservation).await {
            tracing::error!(%error, "failed to release replay reservation");
        }
    }
}
