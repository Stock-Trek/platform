use crate::{
    ApiToken, AuthService, ConsumeResult, InvalidOrderRequest, OrderRequestContext,
    OrderRequestVerification, ReplayKey, ReplayReservation, ReplayStoreError, SignedOrderRequest,
    TokenAuditAction, TokenAuditEvent, TokenUsage, order_verification::CLOCK_SKEW_SECONDS,
};
use chrono::{DateTime, Duration, Utc};
use platform_errors::{AuthError, PlatformError, PlatformResult};
use platform_metrics::AuthReplayMetrics;

impl AuthService {
    pub async fn signature_token(
        &self,
        token_id: &str,
        now: DateTime<Utc>,
    ) -> PlatformResult<Option<ApiToken>> {
        if let Err(error) = ApiToken::validate_id(token_id) {
            self.record_signature_rejected(token_id, None, now, &error.to_string())
                .await;
            return Ok(None);
        }
        match self.token(token_id).await {
            Ok(token) => Ok(Some(token)),
            Err(error) if matches!(error, PlatformError::Auth(AuthError::UnknownToken { .. })) => {
                self.record_signature_rejected(token_id, None, now, &error.to_string())
                    .await;
                Ok(None)
            }
            Err(error) => {
                self.record_signature_rejected(token_id, None, now, &error.to_string())
                    .await;
                Err(error)
            }
        }
    }

    pub fn validate_order_signature(
        &self,
        token_id: &str,
        signature: &str,
        now: DateTime<Utc>,
    ) -> Result<(SignedOrderRequest, OrderRequestContext), InvalidOrderRequest> {
        let signed = SignedOrderRequest::decode(signature)
            .map_err(|_| InvalidOrderRequest::MalformedSignature)?;
        if signed.token_id() != token_id {
            return Err(InvalidOrderRequest::SignatureMismatch);
        }
        let context = signed
            .context()
            .map_err(|_| InvalidOrderRequest::MalformedSignature)?;
        let latest_issued_at = now
            .checked_add_signed(Duration::seconds(CLOCK_SKEW_SECONDS))
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        if signed.is_expired(now) {
            return Err(InvalidOrderRequest::ExpiredSignature);
        }
        if signed.issued_at() > latest_issued_at {
            return Err(InvalidOrderRequest::FutureSignature);
        }
        let validity = signed
            .expires_at()
            .signed_duration_since(signed.issued_at());
        if validity > self.signature_validity {
            return Err(InvalidOrderRequest::InvalidValidity);
        }
        Ok((signed, context))
    }

    pub async fn reject_signature(
        &self,
        token_id: &str,
        token: Option<&ApiToken>,
        now: DateTime<Utc>,
        reason: InvalidOrderRequest,
    ) -> PlatformResult<OrderRequestVerification> {
        self.record_signature_rejected(token_id, token, now, reason.detail())
            .await;
        Ok(OrderRequestVerification::Invalid(reason))
    }

    pub async fn reject_retryable(
        &self,
        token_id: &str,
        token: Option<&ApiToken>,
        now: DateTime<Utc>,
        error: &PlatformError,
    ) -> PlatformResult<OrderRequestVerification> {
        self.record_signature_rejected(token_id, token, now, &error.to_string())
            .await;
        tracing::warn!(%error, "order request verification will be retried");
        Ok(OrderRequestVerification::Retryable)
    }

    async fn record_signature_rejected(
        &self,
        token_id: &str,
        token: Option<&ApiToken>,
        now: DateTime<Utc>,
        detail: &str,
    ) {
        let event = TokenAuditEvent::new(TokenAuditAction::SignatureRejected, now)
            .with_token(token_id, token.map(ApiToken::owner))
            .failure(detail);
        self.record_audit_failure(event).await;
    }
}
impl AuthService {
    pub async fn reserve_order_request_replay(
        &self,
        context: &OrderRequestContext,
        now: DateTime<Utc>,
    ) -> PlatformResult<Option<ReplayReservation>> {
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

    pub async fn commit_signature_use(
        &self,
        token: ApiToken,
        now: DateTime<Utc>,
        reservation: &ReplayReservation,
    ) -> PlatformResult<OrderRequestVerification> {
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

    fn replay_store_error(operation: &'static str, error: ReplayStoreError) -> PlatformError {
        AuthReplayMetrics::record_store_failure(operation);
        let retryable = error.is_retryable();
        PlatformError::auth_store(operation, "replay-store", retryable, error)
    }

    async fn release_replay(&self, reservation: &ReplayReservation) -> PlatformResult<()> {
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
