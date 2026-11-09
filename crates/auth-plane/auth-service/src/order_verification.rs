use crate::{
    ApiToken, AuthService, ConsumeResult, OrderRequestSigner, ReplayKey, ReplayReservation,
    ReplayStoreError, SignedOrderRequest, TokenAuditAction, TokenAuditEvent, TokenScope,
    auth_service::MAX_UPDATE_ATTEMPTS,
};
use chrono::{DateTime, Duration, Utc};
use platform_errors::{AuthError, BpError, BpResult};
use platform_metrics::auth::AuthReplayMetrics;

const CLOCK_SKEW_SECONDS: i64 = 60;

impl AuthService {
    pub async fn verify_order_request(
        &self,
        token_id: &str,
        order_request: &[u8],
        signature: &str,
        now: DateTime<Utc>,
    ) -> BpResult<bool> {
        if let Err(error) = ApiToken::validate_id(token_id) {
            self.record_signature_rejected(token_id, None, now, &error.to_string())
                .await;
            return Ok(false);
        }
        let token = match self.token(token_id).await {
            Ok(token) => token,
            Err(error) if matches!(error, BpError::Auth(AuthError::UnknownToken { .. })) => {
                self.record_signature_rejected(token_id, None, now, &error.to_string())
                    .await;
                return Ok(false);
            }
            Err(error) => {
                self.record_signature_rejected(token_id, None, now, &error.to_string())
                    .await;
                return Err(error);
            }
        };
        if let Err(error) = Self::ensure_active(&token, now) {
            self.record_signature_rejected(token_id, Some(&token), now, &error.to_string())
                .await;
            return Ok(false);
        }
        if let Err(error) = Self::ensure_scope(&token, TokenScope::OrdersWrite) {
            self.record_signature_rejected(token_id, Some(&token), now, &error.to_string())
                .await;
            return Ok(false);
        }
        let Ok(signed) = SignedOrderRequest::decode(signature) else {
            self.record_signature_rejected(token_id, Some(&token), now, "malformed signature")
                .await;
            return Ok(false);
        };
        if signed.token_id() != token_id {
            self.record_signature_rejected(token_id, Some(&token), now, "token mismatch")
                .await;
            return Ok(false);
        }
        let Ok(context) = signed.context() else {
            self.record_signature_rejected(token_id, Some(&token), now, "invalid context")
                .await;
            return Ok(false);
        };
        let latest_issued_at = now
            .checked_add_signed(Duration::seconds(CLOCK_SKEW_SECONDS))
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        if signed.is_expired(now) {
            self.record_signature_rejected(token_id, Some(&token), now, "expired signature")
                .await;
            return Ok(false);
        }
        if signed.issued_at() > latest_issued_at {
            self.record_signature_rejected(
                token_id,
                Some(&token),
                now,
                "signature issued in the future",
            )
            .await;
            return Ok(false);
        }
        let validity = signed
            .expires_at()
            .signed_duration_since(signed.issued_at());
        if validity > self.signature_validity {
            self.record_signature_rejected(
                token_id,
                Some(&token),
                now,
                "signature validity exceeds policy",
            )
            .await;
            return Ok(false);
        }
        let signer = OrderRequestSigner::new(token.secret())?;
        if !signer.verify_order_request(&context, order_request, signed.signature())? {
            self.record_signature_rejected(token_id, Some(&token), now, "signature mismatch")
                .await;
            return Ok(false);
        }
        let replay_key = ReplayKey::new(context.token_id(), context.nonce());
        let reservation = match self
            .replay
            .consume(replay_key, context.expires_at(), now)
            .await
        {
            Ok(ConsumeResult::Consumed(reservation)) => reservation,
            Ok(ConsumeResult::Replayed | ConsumeResult::Expired) => {
                self.record_signature_rejected(token_id, Some(&token), now, "replayed signature")
                    .await;
                return Ok(false);
            }
            Err(error) => {
                let error = Self::replay_store_error("consume", error);
                self.record_signature_rejected(token_id, Some(&token), now, &error.to_string())
                    .await;
                return Err(error);
            }
        };
        let mut latest = token;
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            let mut updated = latest.clone();
            updated.metadata.record_use(now);
            match self.store.update(&latest, &updated).await {
                Ok(()) => {
                    let event = TokenAuditEvent::new(TokenAuditAction::SignatureVerified, now)
                        .with_token(updated.id(), Some(updated.owner()));
                    self.record_audit(event).await?;
                    return Ok(true);
                }
                Err(error) if Self::is_conflict(&error) => {
                    latest = match self.token(token_id).await {
                        Ok(token) => token,
                        Err(error) => {
                            self.release_replay(&reservation).await?;
                            return Err(error);
                        }
                    };
                    if let Err(error) = Self::ensure_active(&latest, now) {
                        self.release_replay(&reservation).await?;
                        self.record_signature_rejected(
                            token_id,
                            Some(&latest),
                            now,
                            &error.to_string(),
                        )
                        .await;
                        return Ok(false);
                    }
                    if let Err(error) = Self::ensure_scope(&latest, TokenScope::OrdersWrite) {
                        self.release_replay(&reservation).await?;
                        self.record_signature_rejected(
                            token_id,
                            Some(&latest),
                            now,
                            &error.to_string(),
                        )
                        .await;
                        return Ok(false);
                    }
                }
                Err(error) => {
                    self.release_replay(&reservation).await?;
                    return Err(error);
                }
            }
        }
        self.release_replay(&reservation).await?;
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
