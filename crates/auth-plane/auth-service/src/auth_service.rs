use crate::{ApiToken, OrderRequestSigner, SignedOrderRequest, token_store::TokenStore};
use platform_errors::{BpError, BpResult};
use std::sync::Arc;

pub struct AuthService {
    store: Arc<dyn TokenStore>,
}

impl AuthService {
    pub fn new(store: Arc<dyn TokenStore>) -> Self {
        Self { store }
    }

    pub async fn add_token(&self, id: impl Into<String>, owner: impl Into<String>) -> BpResult<ApiToken> {
        let token = ApiToken::generate(id, owner)?;
        self.store.put(&token).await?;
        Ok(token)
    }

    pub async fn delete_token(&self, id: &str) -> BpResult<()> {
        self.store.delete(id).await
    }

    pub async fn token(&self, id: &str) -> BpResult<ApiToken> {
        self.store
            .get(id)
            .await?
            .ok_or_else(|| BpError::unknown_token(id))
    }

    pub async fn token_ids(&self) -> BpResult<Vec<String>> {
        self.store.ids().await
    }

    pub async fn sign_order_request(
        &self,
        token_id: &str,
        order_request: &[u8],
    ) -> BpResult<SignedOrderRequest> {
        let token = self.token(token_id).await?;
        let signer = OrderRequestSigner::new(token.secret())?;
        let signature = signer.sign(order_request)?;
        Ok(SignedOrderRequest::new(token_id, signature))
    }

    pub async fn verify_order_request(
        &self,
        token_id: &str,
        order_request: &[u8],
        signature: &str,
    ) -> BpResult<bool> {
        let token = self.token(token_id).await?;
        let signer = OrderRequestSigner::new(token.secret())?;
        signer.verify(order_request, signature)
    }
}
