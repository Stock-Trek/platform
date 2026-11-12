use crate::tests::support_tests::{OWNER, TestFixture};
use chrono::{Duration, Utc};

#[tokio::test]
async fn issues_and_verifies_a_jwt_for_the_owner() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", OWNER).await;
    let now = Utc::now();

    let issued = service.issue_jwt_for_owner(OWNER, now).await.unwrap();
    let claims = service.verify_jwt(issued.token(), now).await.unwrap();

    assert_eq!(claims.token_id(), token.id());
    assert_eq!(claims.owner(), OWNER);
    assert_eq!(claims.expires_at(), issued.expires_at());
    assert!(claims.expires_at() > now);
}

#[tokio::test]
async fn rejects_a_tampered_jwt() {
    let service = TestFixture::service();
    TestFixture::add_token(&service, "token-a", OWNER).await;
    let now = Utc::now();
    let issued = service.issue_jwt_for_owner(OWNER, now).await.unwrap();
    let replacement = if issued.token().ends_with('A') {
        "B"
    } else {
        "A"
    };
    let mut tampered = issued.token().to_string();
    tampered.replace_range(tampered.len() - 1.., replacement);

    assert!(service.verify_jwt(&tampered, now).await.is_err());
}

#[tokio::test]
async fn rejects_an_expired_jwt() {
    let service = TestFixture::service();
    TestFixture::add_token(&service, "token-a", OWNER).await;
    let now = Utc::now();
    let issued = service.issue_jwt_for_owner(OWNER, now).await.unwrap();

    assert!(
        service
            .verify_jwt(issued.token(), now + Duration::minutes(6))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn rejects_a_jwt_after_its_api_token_expires() {
    let service = TestFixture::service();
    TestFixture::add_token_with_expiry(
        &service,
        "token-a",
        OWNER,
        Utc::now() + Duration::minutes(1),
    )
    .await;
    let now = Utc::now();
    let issued = service.issue_jwt_for_owner(OWNER, now).await.unwrap();

    assert!(
        service
            .verify_jwt(issued.token(), now + Duration::minutes(2))
            .await
            .is_err()
    );
}
