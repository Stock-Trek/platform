use crate::{
    ApiToken, AuthService, OrderRequestContext, OrderRequestSigner, SecretInput, SignatureInput,
    SignedOrderRequest, TokenAuditAction, TokenAuditEvent, TokenScope,
    auth_service::MAX_UPDATE_ATTEMPTS,
};
use chrono::{DateTime, Duration, Utc};
use platform_errors::{BpError, BpResult};

const CLOCK_SKEW_SECONDS: i64 = 60;

impl AuthService {
    pub async fn sign_order_request(
        &self,
        token_id: &str,
        secret: impl SecretInput,
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
        let token = match self.token(token_id).await {
            Ok(token) => token,
            Err(error) => {
                self.record_signature_rejected(token_id, None, now, &error.to_string());
                return Err(error);
            }
        };
        if let Err(error) = Self::ensure_active(&token, now) {
            self.record_signature_rejected(token_id, Some(&token), now, &error.to_string());
            return Ok(false);
        }
        if let Err(error) = Self::ensure_scope(&token, TokenScope::OrdersWrite) {
            self.record_signature_rejected(token_id, Some(&token), now, &error.to_string());
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
        if signed.is_expired(now) {
            self.record_signature_rejected(token_id, Some(&token), now, "expired signature");
            return Ok(false);
        }
        if signed.issued_at() > latest_issued_at {
            self.record_signature_rejected(
                token_id,
                Some(&token),
                now,
                "signature issued in the future",
            );
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
        let mut latest = token;
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            let mut updated = latest.clone();
            updated.metadata.record_use(now);
            match self.store.update(&latest, &updated).await {
                Ok(()) => {
                    let event = TokenAuditEvent::new(TokenAuditAction::SignatureVerified, now)
                        .with_token(updated.id(), Some(updated.owner()));
                    self.audit.record(event);
                    return Ok(true);
                }
                Err(error) if Self::is_conflict(&error) => {
                    latest = match self.token(token_id).await {
                        Ok(token) => token,
                        Err(error) => {
                            self.replay.release(token_id, context.nonce());
                            return Err(error);
                        }
                    };
                    if let Err(error) = Self::ensure_active(&latest, now) {
                        self.replay.release(token_id, context.nonce());
                        self.record_signature_rejected(
                            token_id,
                            Some(&latest),
                            now,
                            &error.to_string(),
                        );
                        return Ok(false);
                    }
                    if let Err(error) = Self::ensure_scope(&latest, TokenScope::OrdersWrite) {
                        self.replay.release(token_id, context.nonce());
                        self.record_signature_rejected(
                            token_id,
                            Some(&latest),
                            now,
                            &error.to_string(),
                        );
                        return Ok(false);
                    }
                }
                Err(error) => {
                    self.replay.release(token_id, context.nonce());
                    return Err(error);
                }
            }
        }
        self.replay.release(token_id, context.nonce());
        Err(BpError::token_conflict(token_id))
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
                self.record_signing_failure(authenticated, now, &error);
                return Err(error);
            }
        };
        if context.expires_at() <= now {
            let error = BpError::auth_invalid_field("expires_at", "has already expired");
            self.record_signing_failure(authenticated, now, &error);
            return Err(error);
        }
        let signer = match OrderRequestSigner::new(authenticated.secret()) {
            Ok(signer) => signer,
            Err(error) => {
                self.record_signing_failure(authenticated, now, &error);
                return Err(error);
            }
        };
        let signature = match signer.sign_order_request(&context, order_request) {
            Ok(signature) => signature,
            Err(error) => {
                self.record_signing_failure(authenticated, now, &error);
                return Err(error);
            }
        };
        let event = TokenAuditEvent::new(TokenAuditAction::Signed, now)
            .with_token(authenticated.id(), Some(authenticated.owner()));
        self.audit.record(event);
        Ok(SignedOrderRequest::from_context(&context, signature))
    }

    fn record_signing_failure(&self, token: &ApiToken, now: DateTime<Utc>, error: &BpError) {
        let event = TokenAuditEvent::new(TokenAuditAction::Signed, now)
            .with_token(token.id(), Some(token.owner()))
            .failure(error.to_string());
        self.audit.record(event);
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
