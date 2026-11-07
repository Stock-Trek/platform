use crate::{
    ApiToken, AuthService, OrderRequestContext, OrderRequestSigner, SignatureInput,
    SignedOrderRequest, TokenAuditAction, TokenAuditEvent, TokenScope,
};
use chrono::{DateTime, Duration, Utc};
use platform_errors::BpResult;

const CLOCK_SKEW_SECONDS: i64 = 60;

impl AuthService {
    pub async fn sign_order_request(
        &self,
        token_id: &str,
        secret: impl SignatureInput,
        order_request: &[u8],
        issued_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> BpResult<SignedOrderRequest> {
        let token = self
            .authenticate(token_id, secret, Some(TokenScope::OrdersWrite), now)
            .await?;
        self.sign_with_token_at(&token, order_request, issued_at, now)
    }

    pub async fn sign_order_request_for_owner(
        &self,
        owner: &str,
        order_request: &[u8],
        issued_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> BpResult<SignedOrderRequest> {
        let token = self
            .token_for_owner_with_scope(owner, TokenScope::OrdersWrite, now)
            .await?;
        let authenticated = self
            .authenticate(
                token.id(),
                token.secret(),
                Some(TokenScope::OrdersWrite),
                now,
            )
            .await?;
        self.sign_with_token_at(&authenticated, order_request, issued_at, now)
    }

    pub async fn verify_order_request(
        &self,
        token_id: &str,
        order_request: &[u8],
        signature: impl SignatureInput,
        now: DateTime<Utc>,
    ) -> BpResult<bool> {
        let signature = signature.signature_input();
        let mut token = self.token(token_id).await?;
        if Self::ensure_active(&token, now).is_err()
            || Self::ensure_scope(&token, TokenScope::OrdersWrite).is_err()
        {
            self.record_signature_rejected(token_id, Some(&token), now, "inactive or unauthorized");
            return Ok(false);
        }
        let Ok(signed) = SignedOrderRequest::decode(signature) else {
            self.record_signature_rejected(token_id, Some(&token), now, "malformed signature");
            return Ok(false);
        };
        if signed.token_id() != token_id {
            self.record_signature_rejected(token_id, Some(&token), now, "token mismatch");
            return Ok(false);
        }
        let Ok(context) = signed.context() else {
            self.record_signature_rejected(token_id, Some(&token), now, "invalid context");
            return Ok(false);
        };
        let latest_issued_at = now
            .checked_add_signed(Duration::seconds(CLOCK_SKEW_SECONDS))
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        let latest_expires_at = signed
            .issued_at()
            .checked_add_signed(self.signature_validity);
        if signed.is_expired(now) || signed.issued_at() > latest_issued_at {
            self.record_signature_rejected(token_id, Some(&token), now, "expired signature");
            return Ok(false);
        }
        if latest_expires_at.is_some_and(|latest| signed.expires_at() > latest) {
            self.record_signature_rejected(
                token_id,
                Some(&token),
                now,
                "signature validity exceeds policy",
            );
            return Ok(false);
        }
        let signer = OrderRequestSigner::new(token.secret())?;
        if !signer.verify_order_request(&context, order_request, signed.raw_signature())? {
            self.record_signature_rejected(token_id, Some(&token), now, "signature mismatch");
            return Ok(false);
        }
        if !self.replay.consume(
            context.token_id(),
            context.nonce(),
            context.expires_at(),
            now,
        ) {
            self.record_signature_rejected(token_id, Some(&token), now, "replayed signature");
            return Ok(false);
        }
        token.metadata.record_use(now);
        self.store.update(&token).await?;
        let event = TokenAuditEvent::new(TokenAuditAction::SignatureVerified, now)
            .with_token(token.id(), Some(token.owner()));
        self.audit.record(event);
        Ok(true)
    }

    fn sign_with_token_at(
        &self,
        authenticated: &ApiToken,
        order_request: &[u8],
        issued_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> BpResult<SignedOrderRequest> {
        let context = match OrderRequestContext::generate(
            authenticated.id(),
            issued_at,
            self.signature_validity,
        ) {
            Ok(context) => context,
            Err(error) => {
                let event = TokenAuditEvent::new(TokenAuditAction::Signed, now)
                    .with_token(authenticated.id(), Some(authenticated.owner()))
                    .failure(error.to_string());
                self.audit.record(event);
                return Err(error);
            }
        };
        let signer = OrderRequestSigner::new(authenticated.secret())?;
        let signature = match signer.sign_order_request(&context, order_request) {
            Ok(signature) => signature,
            Err(error) => {
                let event = TokenAuditEvent::new(TokenAuditAction::Signed, now)
                    .with_token(authenticated.id(), Some(authenticated.owner()))
                    .failure(error.to_string());
                self.audit.record(event);
                return Err(error);
            }
        };
        let event = TokenAuditEvent::new(TokenAuditAction::Signed, now)
            .with_token(authenticated.id(), Some(authenticated.owner()));
        self.audit.record(event);
        Ok(SignedOrderRequest::from_context(&context, signature))
    }

    fn record_signature_rejected(
        &self,
        token_id: &str,
        token: Option<&ApiToken>,
        now: DateTime<Utc>,
        detail: &str,
    ) {
        let event = TokenAuditEvent::new(TokenAuditAction::SignatureRejected, now)
            .with_token(token_id, token.map(ApiToken::owner))
            .failure(detail);
        self.audit.record(event);
    }
}
