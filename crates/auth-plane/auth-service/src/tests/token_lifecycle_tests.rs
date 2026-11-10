use super::support_tests::TestFixture;
use crate::{RevocationReason, RevokedBy, TokenExpiry, TokenOptions, TokenScope};
use chrono::{Duration, Utc};
use platform_errors::{AuthError, BpError};

#[tokio::test]
async fn expires_tokens_are_rejected() {
    let service = TestFixture::service();
    let expires_at = Utc::now() + Duration::seconds(1);
    let token =
        TestFixture::add_token_with_expiry(&service, "token-a", "owner-a", expires_at).await;

    let error = service
        .authenticate(token.id(), token.secret(), None, expires_at)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BpError::Auth(AuthError::Expired { ref token_id }) if token_id == token.id()
    ));
    assert!(token.metadata().is_expired(expires_at));
}

#[tokio::test]
async fn token_option_lifetimes_are_relative_to_creation_time() {
    let service = TestFixture::service();
    let created_at = Utc::now();
    let options = TokenOptions::new(
        TokenExpiry::After(Duration::minutes(5)),
        TokenScope::DEFAULT,
    );

    let token =
        TestFixture::add_token_at(&service, "token-a", "owner-a", created_at, options).await;

    assert_eq!(
        token.metadata().expires_at(),
        created_at + Duration::minutes(5)
    );
}

#[tokio::test]
async fn rejects_non_positive_relative_lifetimes() {
    let service = TestFixture::service();
    let options = TokenOptions::new(TokenExpiry::After(Duration::zero()), TokenScope::DEFAULT);

    let now = Utc::now();
    let error = service
        .add_token("token-a", "owner-a", now, options, now)
        .await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "expires_at",
            ..
        }))
    ));
}

#[tokio::test]
async fn absolute_expiry_is_validated_against_the_injected_now() {
    let service = TestFixture::service();
    let now = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
    let expires_at = now + Duration::hours(1);
    let options = TokenOptions::new(TokenExpiry::At(expires_at), TokenScope::DEFAULT);

    let token = service
        .add_token("token-a", "owner-a", now, options, now)
        .await
        .unwrap();

    assert_eq!(token.metadata().expires_at(), expires_at);
}

#[tokio::test]
async fn rejects_an_already_past_absolute_expiry() {
    let service = TestFixture::service();
    let now = Utc::now();
    let options = TokenOptions::new(
        TokenExpiry::At(now - Duration::seconds(1)),
        TokenScope::DEFAULT,
    );

    let error = service
        .add_token("token-a", "owner-a", now, options, now)
        .await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "expires_at",
            ..
        }))
    ));
}

#[tokio::test]
async fn revocation_records_metadata_and_blocks_use() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();

    let revoked = service
        .revoke_token(
            token.id(),
            RevocationReason::Compromised,
            RevokedBy::SecurityTeam,
            now,
        )
        .await
        .unwrap();

    assert_eq!(revoked.metadata().revoked_at(), Some(now));
    assert_eq!(
        revoked.metadata().revocation_reason(),
        Some(RevocationReason::Compromised)
    );
    assert_eq!(
        revoked.metadata().revoked_by(),
        Some(RevokedBy::SecurityTeam)
    );
    let error = service
        .authenticate(token.id(), token.secret(), None, now)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BpError::Auth(AuthError::Revoked { ref token_id }) if token_id == token.id()
    ));
}

#[tokio::test]
async fn owner_listing_includes_inactive_tokens() {
    let service = TestFixture::service();
    let active = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let revoked = TestFixture::add_token(&service, "token-b", "owner-a").await;
    let now = Utc::now();
    service
        .revoke_token(
            revoked.id(),
            RevocationReason::NoLongerNeeded,
            RevokedBy::SecurityTeam,
            now,
        )
        .await
        .unwrap();

    let all = service.tokens_for_owner("owner-a").await.unwrap();
    let active_only = service
        .active_tokens_for_owner("owner-a", now)
        .await
        .unwrap();

    assert_eq!(all.len(), 2);
    assert_eq!(active_only.len(), 1);
    assert_eq!(active_only[0].id(), active.id());
}
