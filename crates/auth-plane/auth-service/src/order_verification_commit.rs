use crate::{
    ApiToken, AuthService, ConsumeResult, OrderRequestContext, ReplayKey, ReplayReservation,
    ReplayStoreError, TokenAuditAction, TokenAuditEvent, TokenScope,
    auth_service::MAX_UPDATE_ATTEMPTS,
};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};
use platform_metrics::auth::AuthReplayMetrics;

impl AuthService {
    pub(crate) async fn reserve_order_request_replay(
        &self,
        context: &OrderRequestContext,
        now: DateTime<Utc>,
    ) -> BpResult<Option<ReplayReservation>> {
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
        token_id: &str,
        now: DateTime<Utc>,
        reservation: &ReplayReservation,
        signature: &str,
        order_request: &[u8],
    ) -> BpResult<bool> {
        let mut latest = token;
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            let mut updated = latest.clone();
            updated.metadata.record_use(now);
            match self.store.update(&latest, &updated).await {
                Ok(()) => {
                    let event = TokenAuditEvent::new(TokenAuditAction::SignatureVerified, now)
                        .with_token(updated.id(), Some(updated.owner()));
                    if let Err(error) = self.record_audit(event).await {
                        self.release_replay(reservation).await?;
                        return Err(error);
                    }
                    return Ok(true);
                }
                Err(error) if Self::is_conflict(&error) => {
                    latest = match self.token(token_id).await {
                        Ok(token) => token,
                        Err(error) => {
                            self.release_replay(reservation).await?;
                            return Err(error);
                        }
                    };
                    if let Err(error) = Self::ensure_active(&latest, now) {
                        self.release_replay(reservation).await?;
                        return self
                            .reject_signature(token_id, Some(&latest), now, &error.to_string())
                            .await;
                    }
                    if let Err(error) = Self::ensure_scope(&latest, TokenScope::OrdersWrite) {
                        self.release_replay(reservation).await?;
                        return self
                            .reject_signature(token_id, Some(&latest), now, &error.to_string())
                            .await;
                    }
                    match self.revalidate_signature(
                        token_id,
                        signature,
                        order_request,
                        &latest,
                        now,
                    ) {
                        Ok(Some(detail)) => {
                            self.release_replay(reservation).await?;
                            return self
                                .reject_signature(token_id, Some(&latest), now, detail)
                                .await;
                        }
                        Ok(None) => {}
                        Err(error) => {
                            self.release_replay(reservation).await?;
                            return Err(error);
                        }
                    }
                }
                Err(error) => {
                    self.release_replay(reservation).await?;
                    return Err(error);
                }
            }
        }
        self.release_replay(reservation).await?;
        Err(BpError::token_conflict(token_id))
    }

    fn replay_store_error(operation: &'static str, error: ReplayStoreError) -> BpError {
        AuthReplayMetrics::record_store_failure(operation);
        let retryable = error.is_retryable();
        BpError::auth_store(operation, "replay-store", retryable, error)
    }

    async fn release_replay(&self, reservation: &ReplayReservation) -> BpResult<()> {
        self.replay
            .release(reservation)
            .await
            .map_err(|error| Self::replay_store_error("release", error))
    }
}
