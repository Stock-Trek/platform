use super::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{AuthService, TokenOptions};
use chrono::{Duration, Utc};
use platform_errors::{AuthError, BpError};
use secrecy::ExposeSecret;

struct LifecycleFixture;

impl LifecycleFixture {
    fn service() -> AuthService {
        TestFixture::service()
    }
}

#[tokio::test]
async fn expires_tokens_are_rejected() {
    let service = LifecycleFixture::service();
    let token = service
        .add_token_with_expiry("token-a", "owner-a", Utc::now() - Duration::seconds(1))
        .await
        .unwrap();

    let error = service
        .authenticate(token.id(), token.secret())
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BpError::Auth(AuthError::InvalidField {
            field: "token_id",
            ..
        })
    ));
    assert!(token.is_expired(Utc::now()));
}

#[tokio::test]
async fn applies_the_configured_default_lifetime() {
    let service = LifecycleFixture::service().with_token_lifetime(Duration::minutes(5));
    let created_at = Utc::now();

    let token = service
        .add_token_at("token-a", "owner-a", created_at)
        .await
        .unwrap();

    assert_eq!(token.expires_at(), Some(created_at + Duration::minutes(5)));
    assert!(token.is_active(created_at + Duration::minutes(4)));
    assert!(!token.is_active(created_at + Duration::minutes(5)));
}

#[tokio::test]
async fn token_option_lifetimes_are_relative_to_creation_time() {
    let service = LifecycleFixture::service();
    let created_at = Utc::now();
    let options = TokenOptions::new().with_lifetime(Duration::minutes(5));

    let token = service
        .add_token_with_options_at("token-a", "owner-a", created_at, options)
        .await
        .unwrap();

    assert_eq!(token.expires_at(), Some(created_at + Duration::minutes(5)));
}

#[tokio::test]
async fn tracks_the_last_time_a_token_was_used() {
    let service = LifecycleFixture::service();
    let token = service.add_token("token-a", "owner-a").await.unwrap();
    let used_at = Utc::now();

    service
        .authenticate_at(token.id(), token.secret(), used_at)
        .await
        .unwrap();

    let stored = service.token(token.id()).await.unwrap();
    assert_eq!(stored.last_used_at(), Some(used_at));
}

#[tokio::test]
async fn failed_authentication_does_not_update_the_last_used_time() {
    let service = LifecycleFixture::service();
    let token = service.add_token("token-a", "owner-a").await.unwrap();

    let _ = service.authenticate(token.id(), "wrong-secret").await;

    let stored = service.token(token.id()).await.unwrap();
    assert!(stored.last_used_at().is_none());
}

#[tokio::test]
async fn revocation_records_metadata_and_blocks_use() {
    let service = LifecycleFixture::service();
    let token = service.add_token("token-a", "owner-a").await.unwrap();

    let revoked = service
        .revoke_token_with_actor(token.id(), "compromised", "security-team")
        .await
        .unwrap();

    assert!(revoked.revoked_at().is_some());
    assert_eq!(revoked.revocation_reason(), Some("compromised"));
    assert_eq!(revoked.revoked_by(), Some("security-team"));
    assert!(
        service
            .authenticate(token.id(), token.secret())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn owner_listing_includes_inactive_tokens() {
    let service = LifecycleFixture::service();
    let active = service.add_token("token-a", "owner-a").await.unwrap();
    let revoked = service.add_token("token-b", "owner-a").await.unwrap();
    service
        .revoke_token(revoked.id(), "no longer needed")
        .await
        .unwrap();

    let all = service.tokens_for_owner("owner-a").await.unwrap();
    let active_only = service.active_tokens_for_owner("owner-a").await.unwrap();

    assert_eq!(all.len(), 2);
    assert_eq!(active_only.len(), 1);
    assert_eq!(active_only[0].id(), active.id());
}

#[tokio::test]
async fn signing_updates_last_used_and_requires_an_active_token() {
    let service = LifecycleFixture::service();
    let token = service.add_token("token-a", "owner-a").await.unwrap();

    service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST)
        .await
        .unwrap();

    let used = service.token(token.id()).await.unwrap();
    assert!(used.last_used_at().is_some());
    let revoked = service
        .revoke_token(token.id(), "compromised")
        .await
        .unwrap();
    assert!(
        service
            .sign_order_request(
                revoked.id(),
                revoked.secret().expose_secret(),
                ORDER_REQUEST
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn purging_removes_only_expired_tokens() {
    let service = LifecycleFixture::service();
    let now = Utc::now();
    service
        .add_token_with_expiry("expired", "owner-a", now - Duration::seconds(1))
        .await
        .unwrap();
    service
        .add_token_with_expiry("active", "owner-a", now + Duration::minutes(1))
        .await
        .unwrap();

    let purged = service.purge_expired_tokens(now).await.unwrap();

    assert_eq!(purged, 1);
    assert_eq!(service.token_ids().await.unwrap(), vec!["active"]);
}
