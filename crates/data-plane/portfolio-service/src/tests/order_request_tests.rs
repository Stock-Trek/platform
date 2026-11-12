use crate::OrderRequestService;
use crate::tests::order_request_fixture_tests::{Fixture, OWNER};
use chrono::Utc;
use platform_auth_service::MemoryTokenStore;
use std::sync::Arc;
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

#[tokio::test]
async fn signs_unsigned_order_requests_for_the_owner() {
    let (service, auth_service, token) = Fixture::service().await;
    let request = Fixture::unsigned(OWNER);

    let submission = Fixture::sign(&service, &auth_service, &request)
        .await
        .unwrap();

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
    let (service, auth_service, _) = Fixture::service().await;

    let submission = Fixture::sign(&service, &auth_service, &Fixture::unsigned(OWNER))
        .await
        .unwrap();
    let body = serde_json::to_value(&submission).unwrap();

    assert_eq!(body["cex"], CexId::Binance.to_string());
    assert_eq!(body["account"], "account-1");
    assert_eq!(body["owner"], OWNER);
    assert_eq!(body["tokenId"], submission.token_id());
    assert_eq!(body["signature"], submission.signature());
}

#[tokio::test]
async fn rejects_an_empty_owner() {
    let (service, auth_service, _) = Fixture::service().await;

    assert!(
        Fixture::sign(&service, &auth_service, &Fixture::unsigned("  "))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn rejects_an_owner_without_a_token() {
    let auth_service = Fixture::auth_service(Arc::new(MemoryTokenStore::default()));
    let service = OrderRequestService::new(auth_service.clone());

    assert!(
        Fixture::sign(&service, &auth_service, &Fixture::unsigned(OWNER))
            .await
            .is_err()
    );
}
