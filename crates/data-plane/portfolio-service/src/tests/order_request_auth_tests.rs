use super::order_request_fixture_tests::{Fixture, OWNER};
use crate::{AccountKey, PortfolioService};
use chrono::Utc;
use platform_messages::UnsignedOrderRequest;
use std::sync::Arc;
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

#[test]
fn canonical_payload_contains_the_routing_fields() {
    let payload = serde_json::to_value(Fixture::unsigned(OWNER)).unwrap();

    assert_eq!(payload["cex"], CexId::Binance.to_string().as_str());
    assert_eq!(payload["account"], "account-1");
    assert_eq!(payload["owner"], OWNER);
    assert_eq!(
        payload["orderRequest"]["MarketBuy"]["tag"],
        "strategy-entry"
    );
}

#[tokio::test]
async fn signature_binds_the_routing_fields() {
    let (service, auth_service, token) = Fixture::service().await;
    let request = Fixture::unsigned(OWNER);
    let submission = Fixture::sign(&service, &auth_service, &request)
        .await
        .unwrap();
    let now = Utc::now();

    let reowned = request.clone().with_owner("other-owner");
    let reaccounted = UnsignedOrderRequest::new(
        request.cex(),
        AccountId("account-2".to_string()),
        request.owner(),
        request.order_request().clone(),
    );

    for tampered in [&reowned, &reaccounted] {
        assert!(
            auth_service
                .verify_order_request(
                    token.id(),
                    &Fixture::canonical_bytes(tampered),
                    submission.signature(),
                    now,
                )
                .await
                .unwrap()
                .is_invalid()
        );
    }
    assert!(
        auth_service
            .verify_order_request(
                token.id(),
                &submission.canonical_bytes().unwrap(),
                submission.signature(),
                now,
            )
            .await
            .unwrap()
            .is_valid()
    );
}

#[tokio::test]
async fn normalizes_the_request_owner() {
    let (service, auth_service, token) = Fixture::service().await;
    let request = Fixture::unsigned(&format!("  {OWNER}  "));

    let submission = Fixture::sign(&service, &auth_service, &request)
        .await
        .unwrap();

    assert_eq!(submission.owner(), OWNER);
    assert!(
        auth_service
            .verify_order_request(
                token.id(),
                &submission.canonical_bytes().unwrap(),
                submission.signature(),
                Utc::now(),
            )
            .await
            .unwrap()
            .is_valid()
    );
}

#[tokio::test]
async fn verifies_a_jwt_against_a_normalized_request_owner() {
    let (service, auth_service, _) = Fixture::service().await;
    let now = Utc::now();
    let jwt = auth_service.issue_jwt_for_owner(OWNER, now).await.unwrap();

    assert!(
        service
            .verify_jwt(jwt.token(), &Fixture::unsigned("  portfolio-owner  "), now)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn signs_for_the_authenticated_owner_not_the_body_owner() {
    let (service, auth_service, _) = Fixture::service().await;
    let now = Utc::now();
    let jwt = auth_service.issue_jwt_for_owner(OWNER, now).await.unwrap();
    let claims = service.authenticate(jwt.token(), now).await.unwrap();

    let submission = service
        .sign_for_caller(&Fixture::unsigned("body-owner"), &claims)
        .await
        .unwrap();

    assert_eq!(submission.owner(), OWNER);
    assert!(
        auth_service
            .verify_order_request(
                claims.token_id(),
                &submission.canonical_bytes().unwrap(),
                submission.signature(),
                Utc::now(),
            )
            .await
            .unwrap()
            .is_valid()
    );
}

fn in_memory_portfolio_feed() -> PortfolioService {
    PortfolioService::new(Arc::new(
        platform_exchange_gateway::connector::NoopPortfolioFeed::new(),
    ))
}

#[tokio::test]
async fn rejects_an_account_that_the_owner_does_not_track() {
    let (service, auth_service, _) = Fixture::service().await;
    let portfolio = Arc::new(in_memory_portfolio_feed());
    let account = AccountKey::from((CexId::Binance, AccountId("account-1".to_string())));
    portfolio
        .track_user_account("other-owner", account)
        .await
        .unwrap();
    let service = service.with_portfolio_service(portfolio);

    let error = Fixture::sign(&service, &auth_service, &Fixture::unsigned(OWNER)).await;

    assert!(error.is_err());
}

#[tokio::test]
async fn accepts_an_account_tracked_by_the_owner() {
    let (service, auth_service, _) = Fixture::service().await;
    let portfolio = Arc::new(in_memory_portfolio_feed());
    let account = AccountKey::from((CexId::Binance, AccountId("account-1".to_string())));
    portfolio.track_user_account(OWNER, account).await.unwrap();
    let service = service.with_portfolio_service(portfolio);

    assert!(
        Fixture::sign(&service, &auth_service, &Fixture::unsigned(OWNER))
            .await
            .is_ok()
    );
}
