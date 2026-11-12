use crate::tests::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{RevocationReason, RevokedBy};
use chrono::{Duration, Utc};
use secrecy::ExposeSecret;

#[tokio::test]
async fn tracks_the_last_time_a_token_was_used() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let used_at = Utc::now();

    service
        .authenticate(token.id(), token.secret(), None, used_at)
        .await
        .unwrap();

    let stored = service.token(token.id()).await.unwrap();
    assert_eq!(stored.metadata().last_used_at(), Some(used_at));
}

#[tokio::test]
async fn failed_authentication_does_not_update_the_last_used_time() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;

    let _ = service
        .authenticate(token.id(), "wrong-secret", None, Utc::now())
        .await;

    let stored = service.token(token.id()).await.unwrap();
    assert!(stored.metadata().last_used_at().is_none());
}

#[tokio::test]
async fn signing_updates_last_used_and_requires_an_active_token() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();

    service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .unwrap();

    let used = service.token(token.id()).await.unwrap();
    assert!(used.metadata().last_used_at().is_some());
    let revoked = service
        .revoke_token(
            token.id(),
            RevocationReason::Compromised,
            RevokedBy::SecurityTeam,
            now,
        )
        .await
        .unwrap();
    assert!(
        service
            .sign_order_request(
                revoked.id(),
                revoked.secret().expose_secret(),
                ORDER_REQUEST,
                now,
                now,
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn purging_removes_only_expired_tokens() {
    let service = TestFixture::service();
    let now = Utc::now();
    TestFixture::add_expired_token(
        &service,
        "expired",
        "owner-a",
        now - Duration::seconds(2),
        now - Duration::seconds(1),
    )
    .await;
    TestFixture::add_token_with_expiry(&service, "active", "owner-a", now + Duration::minutes(1))
        .await;

    let purged = service.purge_expired_tokens(now).await.unwrap();

    assert_eq!(purged, 1);
    let ids: Vec<String> = service
        .tokens_for_owner("owner-a")
        .await
        .unwrap()
        .into_iter()
        .map(|token| token.id().to_string())
        .collect();
    assert_eq!(ids, vec!["active"]);
}
