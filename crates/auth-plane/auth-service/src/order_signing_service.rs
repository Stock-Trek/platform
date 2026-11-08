use crate::{
    ApiToken, AuthService, OrderRequestContext, OrderRequestSigner, SecretInput,
    SignedOrderRequest, TokenAuditAction, TokenAuditEvent, TokenScope,
};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};

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
}
