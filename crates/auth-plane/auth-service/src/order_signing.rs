use crate::{
    ApiToken, AuthService, OrderRequestContext, SecretInput, SignedOrderRequest, TokenAuditAction,
    TokenAuditEvent, TokenScope, TokenUsage, order_verification::CLOCK_SKEW_SECONDS,
};
use chrono::{DateTime, Duration, Utc};
use hmac::{Hmac, KeyInit, Mac};
use platform_errors::{PlatformError, PlatformResult};
use secrecy::{ExposeSecret, SecretString};
use sha2::Sha256;
use subtle::ConstantTimeEq;

pub struct OrderRequestSigner {
    mac: Hmac<Sha256>,
}

impl OrderRequestSigner {
    pub fn new(secret: &SecretString) -> PlatformResult<Self> {
        if secret.expose_secret().trim().is_empty() {
            return Err(PlatformError::auth_invalid_field(
                "secret",
                "must not be empty",
            ));
        }
        let mac =
            Hmac::<Sha256>::new_from_slice(secret.expose_secret().as_bytes()).map_err(|e| {
                PlatformError::auth_invalid_field("secret", format!("rejected as an HMAC key: {e}"))
            })?;
        Ok(Self { mac })
    }

    pub fn sign_order_request(
        &self,
        context: &OrderRequestContext,
        order_request: &[u8],
    ) -> PlatformResult<String> {
        let mut mac = self.mac.clone();
        mac.update(&context.canonical(order_request));
        Ok(data_encoding::HEXLOWER.encode(&mac.finalize().into_bytes()))
    }

    pub fn verify_order_request(
        &self,
        context: &OrderRequestContext,
        order_request: &[u8],
        signature: &str,
    ) -> PlatformResult<bool> {
        let expected = self.sign_order_request(context, order_request)?;
        Ok(expected.as_bytes().ct_eq(signature.as_bytes()).into())
    }
}
impl AuthService {
    pub async fn sign_order_request(
        &self,
        token_id: &str,
        secret: impl SecretInput,
        order_request: &[u8],
        issued_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> PlatformResult<SignedOrderRequest> {
        let candidate = secret.secret_input().to_string();
        let token = match self.token(token_id).await {
            Ok(token) => token,
            Err(error) => {
                self.record_authentication_failure(token_id, None, now, &error)
                    .await;
                return Err(error);
            }
        };
        self.sign_with_loaded_token(token, Some(candidate), order_request, issued_at, now)
            .await
    }

    pub async fn sign_order_request_for_token(
        &self,
        token_id: &str,
        order_request: &[u8],
        issued_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> PlatformResult<SignedOrderRequest> {
        let token = self.token(token_id).await?;
        self.sign_with_loaded_token(token, None, order_request, issued_at, now)
            .await
    }

    async fn sign_with_loaded_token(
        &self,
        token: ApiToken,
        candidate: Option<String>,
        order_request: &[u8],
        issued_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> PlatformResult<SignedOrderRequest> {
        let token_id = token.id().to_string();
        let candidate = candidate.unwrap_or_else(|| token.secret().expose_secret().to_string());
        if !token.matches_secret(&candidate) {
            let error = PlatformError::auth_invalid_secret(token.id());
            self.record_authentication_failure(&token_id, Some(&token), now, &error)
                .await;
            return Err(error);
        }
        if let Err(error) = Self::ensure_active(&token, now) {
            self.record_authentication_failure(&token_id, Some(&token), now, &error)
                .await;
            return Err(error);
        }
        if let Err(error) = Self::ensure_scope(&token, TokenScope::OrdersWrite) {
            self.record_authentication_failure(&token_id, Some(&token), now, &error)
                .await;
            return Err(error);
        }
        let context =
            match OrderRequestContext::generate(token.id(), issued_at, self.signature_validity) {
                Ok(context) => context,
                Err(error) => {
                    self.record_signing_failure(&token, now, &error).await;
                    return Err(error);
                }
            };
        let latest_issued_at = now
            .checked_add_signed(Duration::seconds(CLOCK_SKEW_SECONDS))
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        if context.issued_at() > latest_issued_at {
            let error = PlatformError::auth_invalid_field("issued_at", "must not be in the future");
            self.record_signing_failure(&token, now, &error).await;
            return Err(error);
        }
        if context.expires_at() <= now {
            let error = PlatformError::auth_invalid_field("expires_at", "has already expired");
            self.record_signing_failure(&token, now, &error).await;
            return Err(error);
        }
        let signer = match OrderRequestSigner::new(token.secret()) {
            Ok(signer) => signer,
            Err(error) => {
                self.record_signing_failure(&token, now, &error).await;
                return Err(error);
            }
        };
        let signature = match signer.sign_order_request(&context, order_request) {
            Ok(signature) => signature,
            Err(error) => {
                self.record_signing_failure(&token, now, &error).await;
                return Err(error);
            }
        };
        let signed = match SignedOrderRequest::from_context(&context, signature) {
            Ok(signed) => signed,
            Err(error) => {
                self.record_signing_failure(&token, now, &error).await;
                return Err(error);
            }
        };
        let authenticated_event = TokenAuditEvent::new(TokenAuditAction::Authenticated, now)
            .with_token(token.id(), Some(token.owner()));
        let signed_event = TokenAuditEvent::new(TokenAuditAction::Signed, now)
            .with_token(token.id(), Some(token.owner()));
        let usage = TokenUsage::for_token(token.id(), now, vec![authenticated_event, signed_event]);
        match self.enqueue_usage(usage).await {
            Ok(()) => Ok(signed),
            Err(error) => {
                tracing::error!(%error, "failed to enqueue token usage");
                Err(error)
            }
        }
    }

    async fn record_signing_failure(
        &self,
        token: &ApiToken,
        now: DateTime<Utc>,
        error: &PlatformError,
    ) {
        let event = TokenAuditEvent::new(TokenAuditAction::Signed, now)
            .with_token(token.id(), Some(token.owner()))
            .failure(error.to_string());
        self.record_audit_failure(event).await;
    }
}
