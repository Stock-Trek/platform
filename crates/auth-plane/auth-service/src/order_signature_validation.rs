use crate::{
    ApiToken, AuthService, InvalidOrderRequest, OrderRequestContext, OrderRequestVerification,
    SignedOrderRequest, TokenAuditAction, TokenAuditEvent, order_verification::CLOCK_SKEW_SECONDS,
};
use chrono::{DateTime, Duration, Utc};
use platform_errors::{AuthError, BpError, BpResult};

impl AuthService {
    pub(crate) async fn signature_token(
        &self,
        token_id: &str,
        now: DateTime<Utc>,
    ) -> BpResult<Option<ApiToken>> {
        if let Err(error) = ApiToken::validate_id(token_id) {
            self.record_signature_rejected(token_id, None, now, &error.to_string())
                .await;
            return Ok(None);
        }
        match self.token(token_id).await {
            Ok(token) => Ok(Some(token)),
            Err(error) if matches!(error, BpError::Auth(AuthError::UnknownToken { .. })) => {
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

    pub(crate) fn validate_order_signature(
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

    pub(crate) async fn reject_signature(
        &self,
        token_id: &str,
        token: Option<&ApiToken>,
        now: DateTime<Utc>,
        reason: InvalidOrderRequest,
    ) -> BpResult<OrderRequestVerification> {
        self.record_signature_rejected(token_id, token, now, reason.detail())
            .await;
        Ok(OrderRequestVerification::Invalid(reason))
    }

    pub(crate) async fn reject_retryable(
        &self,
        token_id: &str,
        token: Option<&ApiToken>,
        now: DateTime<Utc>,
        error: &BpError,
    ) -> BpResult<OrderRequestVerification> {
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
