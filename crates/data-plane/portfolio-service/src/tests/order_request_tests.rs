use crate::OrderRequestService;
use chrono::{Duration, Utc};
use platform_auth_service::{
    ApiToken, AuthService, AuthServiceConfig, MemoryReplayStore, MemoryTokenStore, TokenMetadata,
    TokenScope, TokenStore,
};
use platform_messages::order_request::UnsignedOrderRequest;
use std::sync::Arc;
use stock_trek_types::cex::{
    account_id::AccountId, activation::Activation, asset_id::AssetId, cex_id::CexId,
    order_request::OrderRequest, tag::Tag,
};

const OWNER: &str = "portfolio-owner";

struct Fixture;

impl Fixture {
    fn auth_service(store: Arc<MemoryTokenStore>) -> AuthService {
        AuthService::new(
            store,
            AuthServiceConfig {
                token_lifetime: Duration::days(30),
                rotation_overlap: Duration::days(7),
                signature_validity: Duration::minutes(5),
            },
            Arc::new(MemoryReplayStore::default()),
        )
        .unwrap()
    }

    async fn service() -> (OrderRequestService, AuthService, ApiToken) {
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

    fn order_request_bytes(request: &UnsignedOrderRequest) -> Vec<u8> {
        request.order_request_bytes().unwrap()
    }

    fn unsigned(owner: &str) -> UnsignedOrderRequest {
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

#[tokio::test]
async fn signs_unsigned_order_requests_for_the_owner() {
    let (service, auth_service, token) = Fixture::service().await;
    let request = Fixture::unsigned(OWNER);

    let submission = service.sign(&request).await.unwrap();

    assert_eq!(submission.cex(), CexId::Binance);
    assert_eq!(submission.account(), &AccountId("account-1".to_string()));
    assert_eq!(submission.owner(), OWNER);
    assert_eq!(submission.token_id(), token.id());
    assert_eq!(
        serde_json::to_value(submission.order_request()).unwrap(),
        serde_json::to_value(request.order_request()).unwrap()
    );
    assert!(
        auth_service
            .verify_order_request(
                token.id(),
                &Fixture::order_request_bytes(&request),
                submission.signature(),
                Utc::now(),
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn verifies_a_jwt_for_the_request_owner() {
    let (service, auth_service, _) = Fixture::service().await;
    let request = Fixture::unsigned(OWNER);
    let now = Utc::now();
    let jwt = auth_service.issue_jwt_for_owner(OWNER, now).await.unwrap();

    assert!(service.verify_jwt(jwt.token(), &request, now).await.is_ok());
    assert!(
        service
            .verify_jwt(jwt.token(), &Fixture::unsigned("other-owner"), now)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn serializes_the_signed_submission_for_delivery() {
    let (service, _, _) = Fixture::service().await;

    let submission = service.sign(&Fixture::unsigned(OWNER)).await.unwrap();
    let body = serde_json::to_value(&submission).unwrap();

    assert_eq!(body["cex"], "binance");
    assert_eq!(body["account"], "account-1");
    assert_eq!(body["owner"], OWNER);
    assert_eq!(body["tokenId"], submission.token_id());
    assert_eq!(body["signature"], submission.signature());
}

#[tokio::test]
async fn rejects_an_empty_owner() {
    let (service, _, _) = Fixture::service().await;

    assert!(service.sign(&Fixture::unsigned("  ")).await.is_err());
}

#[tokio::test]
async fn rejects_an_owner_without_a_token() {
    let auth_service = Fixture::auth_service(Arc::new(MemoryTokenStore::default()));
    let service = OrderRequestService::new(auth_service);

    assert!(service.sign(&Fixture::unsigned(OWNER)).await.is_err());
}
