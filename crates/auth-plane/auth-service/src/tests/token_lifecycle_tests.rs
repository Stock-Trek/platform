use super::support_tests::TestFixture;
use crate::{
    ApiToken, AuthService, RevocationReason, RevokedBy, TokenExpiry, TokenOptions, TokenScope,
};
use chrono::{Duration, Utc};
use platform_errors::{AuthError, BpError};

struct LifecycleFixture;

impl LifecycleFixture {
    fn service() -> AuthService {
        TestFixture::service()
    }

    async fn add_token_with_expiry(
        service: &AuthService,
        id: &str,
        owner: &str,
        expires_at: chrono::DateTime<Utc>,
    ) -> ApiToken {
        let options = TokenOptions::new(TokenExpiry::At(expires_at), TokenScope::DEFAULT);
        TestFixture::add_token_at(service, id, owner, Utc::now(), options).await
    }
}

#[tokio::test]
async fn expires_tokens_are_rejected() {
    let service = LifecycleFixture::service();
    let now = Utc::now();
    let token = LifecycleFixture::add_token_with_expiry(&service, "token-a", "owner-a", now).await;

    let error = service
        .authenticate(token.id(), token.secret(), None, now)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BpError::Auth(AuthError::InvalidField {
            field: "token_id",
            ..
        })
    ));
    assert!(token.metadata().is_expired(now));
}

#[tokio::test]
async fn token_option_lifetimes_are_relative_to_creation_time() {
    let service = LifecycleFixture::service();
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
    let service = LifecycleFixture::service();
    let options = TokenOptions::new(TokenExpiry::After(Duration::zero()), TokenScope::DEFAULT);

    let error = service
        .add_token("token-a", "owner-a", Utc::now(), options)
        .await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "expiry",
            ..
        }))
    ));
}

#[tokio::test]
async fn revocation_records_metadata_and_blocks_use() {
    let service = LifecycleFixture::service();
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
    assert!(
        service
            .authenticate(token.id(), token.secret(), None, now)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn owner_listing_includes_inactive_tokens() {
    let service = LifecycleFixture::service();
    let now = Utc::now();
    let active = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let revoked = TestFixture::add_token(&service, "token-b", "owner-a").await;
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
