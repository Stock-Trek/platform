use crate::{
    ApiToken, OrderRequestContext, OrderRequestSigner, SignatureInput, SignedOrderRequest,
    replay_cache::ReplayCache, signature_input::SignatureArguments, token_store::TokenStore,
};
use chrono::{Duration, Utc};
use platform_errors::{BpError, BpResult};
use std::{str, sync::Arc};

const DEFAULT_SIGNATURE_VALIDITY_SECONDS: i64 = 300;
const CLOCK_SKEW_SECONDS: i64 = 60;

#[derive(Clone)]
pub struct AuthService {
    store: Arc<dyn TokenStore>,
    replay: Arc<ReplayCache>,
    signature_validity: Duration,
}

impl AuthService {
    pub fn new(store: Arc<dyn TokenStore>) -> Self {
        Self {
            store,
            replay: Arc::new(ReplayCache::new()),
            signature_validity: Duration::seconds(DEFAULT_SIGNATURE_VALIDITY_SECONDS),
        }
    }

    pub fn with_signature_validity(mut self, validity: std::time::Duration) -> Self {
        if let Ok(validity) = Duration::from_std(validity) {
            self.signature_validity = validity;
        }
        self
    }

    pub async fn add_token(
        &self,
        id: impl Into<String>,
        owner: impl Into<String>,
    ) -> BpResult<ApiToken> {
        let token = ApiToken::generate(id, owner)?;
        self.store.put(&token).await?;
        Ok(token)
    }

    pub async fn delete_token(&self, id: &str) -> BpResult<()> {
        ApiToken::validate_id(id)?;
        self.store.delete(id).await
    }

    pub async fn token(&self, id: &str) -> BpResult<ApiToken> {
        ApiToken::validate_id(id)?;
        self.store
            .get(id)
            .await?
            .ok_or_else(|| BpError::unknown_token(id))
    }

    pub async fn token_ids(&self) -> BpResult<Vec<String>> {
        self.store.ids().await
    }

    pub async fn tokens_for_owner(&self, owner: &str) -> BpResult<Vec<ApiToken>> {
        if owner.trim().is_empty() {
            return Err(BpError::auth_invalid_field("owner", "must not be empty"));
        }
        let mut tokens = Vec::new();
        for id in self.store.ids().await? {
            if let Some(token) = self.store.get(&id).await?
                && token.owner() == owner
            {
                tokens.push(token);
            }
        }
        Ok(tokens)
    }

    pub async fn token_for_owner(&self, owner: &str) -> BpResult<ApiToken> {
        self.tokens_for_owner(owner)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| BpError::auth_invalid_field("owner", "has no API token"))
    }

    pub async fn authenticate(
        &self,
        token_id: &str,
        secret: impl SignatureInput,
    ) -> BpResult<ApiToken> {
        let candidate = match secret.signature_argument() {
            crate::SignatureArgument::Text(secret) => secret,
            crate::SignatureArgument::Bytes(secret) => str::from_utf8(secret)
                .map_err(|e| BpError::auth_invalid_field("secret", e.to_string()))?,
        };
        let token = self.token(token_id).await?;
        if !token.matches_secret(candidate) {
            return Err(BpError::auth_invalid_field(
                "secret",
                "does not match the token",
            ));
        }
        Ok(token)
    }

    pub async fn sign_order_request(
        &self,
        token_id: &str,
        secret: impl SignatureInput,
        order_request: impl SignatureInput,
    ) -> BpResult<SignedOrderRequest> {
        let (secret, order_request) = SignatureArguments::split_sign(&secret, &order_request)?;
        self.sign_order_request_at(token_id, secret, order_request, Utc::now())
            .await
    }

    pub async fn sign_order_request_for_owner(
        &self,
        owner: &str,
        order_request: impl SignatureInput,
    ) -> BpResult<SignedOrderRequest> {
        let token = self.token_for_owner(owner).await?;
        self.sign_order_request(token.id(), token.secret(), order_request)
            .await
    }

    pub async fn sign_order_request_at(
        &self,
        token_id: &str,
        secret: &str,
        order_request: &[u8],
        issued_at: chrono::DateTime<Utc>,
    ) -> BpResult<SignedOrderRequest> {
        let token = self.authenticate(token_id, secret).await?;
        let context =
            OrderRequestContext::generate(token.id(), issued_at, self.signature_validity)?;
        let signer = OrderRequestSigner::new(token.secret())?;
        let signature = signer.sign_order_request(&context, order_request)?;
        Ok(SignedOrderRequest::from_context(&context, signature))
    }

    pub async fn verify_order_request(
        &self,
        token_id: &str,
        order_request: impl SignatureInput,
        signature: impl SignatureInput,
    ) -> BpResult<bool> {
        let (order_request, signature) =
            SignatureArguments::split_verify(&order_request, &signature)?;
        let token = self.token(token_id).await?;
        let Ok(signed) = SignedOrderRequest::decode(signature) else {
            return Ok(false);
        };
        if signed.token_id() != token_id {
            return Ok(false);
        }
        let Ok(context) = signed.context() else {
            return Ok(false);
        };
        let now = Utc::now();
        if context.expires_at() <= now
            || context.issued_at() > now + Duration::seconds(CLOCK_SKEW_SECONDS)
        {
            return Ok(false);
        }
        let signer = OrderRequestSigner::new(token.secret())?;
        if !signer.verify_order_request(&context, order_request, signed.raw_signature())? {
            return Ok(false);
        }
        Ok(self
            .replay
            .consume(context.nonce(), context.expires_at(), now))
    }
}
