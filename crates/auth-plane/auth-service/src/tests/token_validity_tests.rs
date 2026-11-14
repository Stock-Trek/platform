use crate::tests::support_tests::TestFixture;
use crate::{
    ApiToken, AuthService, TokenMetadata, TokenScope, token_options::TokenExpiry,
    token_options::TokenOptions,
};
use chrono::{DateTime, Duration, Utc};
use platform_errors::{AuthError, PlatformError};

struct ValidityFixture;

impl ValidityFixture {
    fn options(expiry: TokenExpiry) -> TokenOptions {
        TokenOptions::new(expiry, TokenScope::DEFAULT)
    }

    async fn store(service: &AuthService, token: &ApiToken) {
        service.store.put(token).await.unwrap();
    }

    fn future_token(now: DateTime<Utc>, id: &str, owner: &str) -> ApiToken {
        let metadata = TokenMetadata::new(now + Duration::hours(2), TokenScope::DEFAULT).unwrap();
        ApiToken::generate(id, owner, now + Duration::hours(1), metadata).unwrap()
    }

    fn assert_invalid_field(error: PlatformError, expected: &'static str) {
        assert!(matches!(
            error,
            PlatformError::Auth(AuthError::InvalidField { field, .. }) if field == expected
        ));
    }
}

#[tokio::test]
async fn rejects_a_future_creation_time() {
    let service = TestFixture::service();
    let now = Utc::now();
    let created_at = now + Duration::seconds(1);

    for (id, expiry) in [
        ("relative", TokenExpiry::After(Duration::days(30))),
        ("absolute", TokenExpiry::At(created_at + Duration::days(30))),
    ] {
        let error = service
            .add_token(
                id,
                "owner-a",
                created_at,
                ValidityFixture::options(expiry),
                now,
            )
            .await
            .unwrap_err();

        ValidityFixture::assert_invalid_field(error, "created_at");
    }
}

#[tokio::test]
async fn rejects_a_relative_expiry_that_has_already_passed() {
    let service = TestFixture::service();
    let now = Utc::now();
    let created_at = now - Duration::hours(2);

    for (id, lifetime) in [
        ("expired", Duration::hours(1)),
        ("expires-now", Duration::hours(2)),
    ] {
        let error = service
            .add_token(
                id,
                "owner-a",
                created_at,
                ValidityFixture::options(TokenExpiry::After(lifetime)),
                now,
            )
            .await
            .unwrap_err();

        ValidityFixture::assert_invalid_field(error, "expires_at");
    }
}

#[tokio::test]
async fn future_created_tokens_cannot_authenticate() {
    let service = TestFixture::service();
    let now = Utc::now();
    let token = ValidityFixture::future_token(now, "future", "owner-a");
    ValidityFixture::store(&service, &token).await;

    let error = service
        .authenticate(token.id(), token.secret(), None, now)
        .await
        .unwrap_err();

    ValidityFixture::assert_invalid_field(error, "created_at");
}

#[tokio::test]
async fn future_created_tokens_are_not_active_for_their_owner() {
    let service = TestFixture::service();
    let now = Utc::now();
    let active = service
        .add_token("active", "owner-a", now, TestFixture::options(), now)
        .await
        .unwrap();
    let future = ValidityFixture::future_token(now, "future", "owner-a");
    ValidityFixture::store(&service, &future).await;

    let active_tokens = service
        .active_tokens_for_owner("owner-a", now)
        .await
        .unwrap();
    let selected = service.token_for_owner("owner-a", now).await.unwrap();

    assert_eq!(active_tokens.len(), 1);
    assert_eq!(active_tokens[0].id(), active.id());
    assert_eq!(selected.id(), active.id());
}

#[tokio::test]
async fn an_owner_with_only_a_future_token_has_no_active_token() {
    let service = TestFixture::service();
    let now = Utc::now();
    let future = ValidityFixture::future_token(now, "future", "owner-a");
    ValidityFixture::store(&service, &future).await;

    assert!(
        service
            .active_tokens_for_owner("owner-a", now)
            .await
            .unwrap()
            .is_empty()
    );
    let error = service.token_for_owner("owner-a", now).await.unwrap_err();

    ValidityFixture::assert_invalid_field(error, "owner");
}

#[test]
fn api_token_activity_starts_at_its_creation_time() {
    let now = Utc::now();
    let token = ValidityFixture::future_token(now, "future", "owner-a");

    assert!(!token.is_active(now));
    assert!(token.is_active(now + Duration::hours(1)));
}
