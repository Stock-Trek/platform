use crate::tests::support_tests::{ORDER_REQUEST, TestFixture};
use chrono::{Duration, Utc};

#[tokio::test]
async fn signs_order_requests_with_the_authenticated_token() {
    let service = TestFixture::service();
    let now = Utc::now();
    let authenticated = TestFixture::add_token_at(
        &service,
        "token-a",
        "owner-a",
        now - Duration::minutes(2),
        TestFixture::options(),
    )
    .await;
    TestFixture::add_token_at(
        &service,
        "token-z",
        "owner-a",
        now - Duration::minutes(1),
        TestFixture::options(),
    )
    .await;

    let signed = service
        .sign_order_request_for_token(authenticated.id(), ORDER_REQUEST, now, now)
        .await
        .unwrap();

    assert_eq!(signed.token_id(), authenticated.id());
    assert!(
        service
            .verify_order_request(authenticated.id(), ORDER_REQUEST, signed.envelope(), now)
            .await
            .unwrap()
            .is_valid()
    );
}

#[tokio::test]
async fn rejects_an_authenticated_token_without_order_write_scope() {
    let service = TestFixture::service();
    let now = Utc::now();
    let read_only = TestFixture::add_token_at(
        &service,
        "token-a",
        "owner-a",
        now,
        crate::token_options::TokenOptions::new(
            crate::token_options::TokenExpiry::After(Duration::days(30)),
            [crate::TokenScope::OrdersRead],
        ),
    )
    .await;

    assert!(
        service
            .sign_order_request_for_token(read_only.id(), ORDER_REQUEST, now, now)
            .await
            .is_err()
    );
}
