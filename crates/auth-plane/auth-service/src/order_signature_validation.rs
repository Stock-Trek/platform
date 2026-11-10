use crate::{
    ApiToken, AuthService, OrderRequestContext, OrderRequestSigner, SignedOrderRequest,
    TokenAuditAction, TokenAuditEvent, order_verification::CLOCK_SKEW_SECONDS,
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
    ) -> Result<(SignedOrderRequest, OrderRequestContext), &'static str> {
        let signed = SignedOrderRequest::decode(signature).map_err(|_| "malformed signature")?;
        if signed.token_id() != token_id {
            return Err("token mismatch");
        }
        let context = signed.context().map_err(|_| "invalid context")?;
        let latest_issued_at = now
            .checked_add_signed(Duration::seconds(CLOCK_SKEW_SECONDS))
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        if signed.is_expired(now) {
            return Err("expired signature");
        }
        if signed.issued_at() > latest_issued_at {
            return Err("signature issued in the future");
        }
        let validity = signed
            .expires_at()
            .signed_duration_since(signed.issued_at());
        if validity > self.signature_validity {
            return Err("signature validity exceeds policy");
        }
        Ok((signed, context))
    }

    pub(crate) fn revalidate_signature(
        &self,
        token_id: &str,
        signature: &str,
        order_request: &[u8],
        token: &ApiToken,
        now: DateTime<Utc>,
    ) -> BpResult<Option<&'static str>> {
        let (signed, context) = match self.validate_order_signature(token_id, signature, now) {
            Ok(validated) => validated,
            Err(detail) => return Ok(Some(detail)),
        };
        let signer = OrderRequestSigner::new(token.secret())?;
        if signer.verify_order_request(&context, order_request, signed.signature())? {
            Ok(None)
        } else {
            Ok(Some("signature mismatch"))
        }
    }

    pub(crate) async fn reject_signature(
        &self,
        token_id: &str,
        token: Option<&ApiToken>,
        now: DateTime<Utc>,
        detail: &str,
    ) -> BpResult<bool> {
        self.record_signature_rejected(token_id, token, now, detail)
            .await;
        Ok(false)
    }

    pub(crate) async fn reject_signature_with_error<T>(
        &self,
        token_id: &str,
        token: Option<&ApiToken>,
        now: DateTime<Utc>,
        error: BpError,
    ) -> BpResult<T> {
        self.record_signature_rejected(token_id, token, now, &error.to_string())
            .await;
        Err(error)
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
