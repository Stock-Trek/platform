use crate::{OrderRequestService, SignedOrderSubmission};
use chrono::{Duration, Utc};
use platform_auth_service::{
    ApiToken, AuthService, AuthServiceConfig, MemoryReplayStore, MemoryTokenStore,
    MemoryTokenUsageQueue, TokenMetadata, TokenScope, TokenStore,
};
use platform_errors::BuildResult;
use platform_messages::UnsignedOrderRequest;
use std::sync::Arc;
use stock_trek_types::cex::{
    account_id::AccountId, activation::Activation, asset_id::AssetId, cex_id::CexId,
    order_request::OrderRequest, tag::Tag,
};

pub const OWNER: &str = "portfolio-owner";

pub struct Fixture;

impl Fixture {
    pub fn auth_service(store: Arc<MemoryTokenStore>) -> AuthService {
        AuthService::new(
            store,
            AuthServiceConfig {
                token_lifetime: Duration::days(30),
                rotation_overlap: Duration::days(7),
                signature_validity: Duration::minutes(5),
            },
            Arc::new(MemoryReplayStore::default()),
            Arc::new(MemoryTokenUsageQueue::new(1024)),
        )
        .unwrap()
    }

    pub async fn service() -> (OrderRequestService, AuthService, ApiToken) {
        let store = Arc::new(MemoryTokenStore::default());
        let now = Utc::now();
        let metadata = TokenMetadata::new(now + Duration::days(30), TokenScope::DEFAULT).unwrap();
        let token = ApiToken::generate("portfolio-token", OWNER, now, metadata).unwrap();
        store.put(&token).await.unwrap();
        let auth_service = Self::auth_service(store);
        (
            OrderRequestService::new(auth_service.clone()),
            auth_service,
            token,
        )
    }

    pub fn canonical_bytes(request: &UnsignedOrderRequest) -> Vec<u8> {
        request.canonical_bytes().unwrap()
    }

    pub async fn sign(
        service: &OrderRequestService,
        auth_service: &AuthService,
        request: &UnsignedOrderRequest,
    ) -> BuildResult<SignedOrderSubmission> {
        let now = Utc::now();
        let jwt = auth_service
            .issue_jwt_for_owner(request.owner(), now)
            .await?;
        let claims = service.authenticate(jwt.token(), now).await?;
        service.sign_for_caller(request, &claims).await
    }

    pub fn unsigned(owner: &str) -> UnsignedOrderRequest {
        UnsignedOrderRequest::new(
            CexId::Binance,
            AccountId("account-1".to_string()),
            owner,
            OrderRequest::MarketBuy {
                base: AssetId::Bitcoin,
                quote: AssetId::TetherUSD,
                activation: Activation::Immediate,
                quote_quantity: 250.0,
                tag: Tag::new("strategy-entry"),
            },
        )
    }
}
