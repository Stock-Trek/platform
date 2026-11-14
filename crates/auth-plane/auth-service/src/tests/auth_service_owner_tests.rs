use crate::tests::support_tests::TestFixture;
use crate::{ApiToken, TokenScope, token_options::TokenExpiry, token_options::TokenOptions};
use chrono::{Duration, Utc};
use platform_errors::{AuthError, PlatformError};
use secrecy::SecretString;

#[tokio::test]
async fn finds_tokens_by_owner() {
    let service = TestFixture::service();
    let first = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let second = TestFixture::add_token(&service, "token-b", "owner-b").await;

    let tokens = service.tokens_for_owner("owner-a").await.unwrap();

    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].id(), first.id());
    assert_ne!(tokens[0].id(), second.id());
}

#[tokio::test]
async fn an_owner_without_a_token_cannot_be_used() {
    let service = TestFixture::service();

    let error = service.token_for_owner("owner-a", Utc::now()).await;

    assert!(matches!(
        error,
        Err(PlatformError::Auth(AuthError::InvalidField { .. }))
    ));
}

#[tokio::test]
async fn rejects_an_empty_owner() {
    let service = TestFixture::service();

    let error = service.tokens_for_owner(" ").await;

    assert!(matches!(
        error,
        Err(PlatformError::Auth(AuthError::InvalidField { .. }))
    ));
}

#[test]
fn normalizes_owners_with_surrounding_whitespace() {
    let token = ApiToken::from_parts(
        "token-a",
        " demo-owner ",
        SecretString::from("secret"),
        Utc::now(),
        TestFixture::metadata(),
    )
    .unwrap();

    assert_eq!(token.owner(), "demo-owner");
}

#[tokio::test]
async fn finds_tokens_with_a_normalized_owner() {
    let service = TestFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;

    let tokens = service.tokens_for_owner("  owner-a  ").await.unwrap();

    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].id(), token.id());
}

#[tokio::test]
async fn selects_a_token_with_the_required_scope_for_an_owner() {
    let service = TestFixture::service();
    let now = Utc::now();
    let read_options = TokenOptions::new(
        TokenExpiry::After(Duration::days(30)),
        [TokenScope::OrdersRead],
    );
    let write_options = TokenOptions::new(
        TokenExpiry::After(Duration::days(30)),
        [TokenScope::OrdersWrite],
    );
    TestFixture::add_token_at(&service, "token-a", "owner-a", now, read_options).await;
    let writer =
        TestFixture::add_token_at(&service, "token-z", "owner-a", now, write_options).await;

    let selected = service
        .token_for_owner_with_scope("owner-a", TokenScope::OrdersWrite, now)
        .await
        .unwrap();

    assert_eq!(selected.id(), writer.id());
}

#[tokio::test]
async fn selecting_a_token_for_an_owner_requires_the_required_scope() {
    let service = TestFixture::service();
    let now = Utc::now();
    let read_options = TokenOptions::new(
        TokenExpiry::After(Duration::days(30)),
        [TokenScope::OrdersRead],
    );
    TestFixture::add_token_at(&service, "token-a", "owner-a", now, read_options).await;

    let error = service
        .token_for_owner_with_scope("owner-a", TokenScope::OrdersWrite, now)
        .await;

    assert!(matches!(
        error,
        Err(PlatformError::Auth(AuthError::InvalidField {
            field: "scope",
            ..
        }))
    ));
}

#[tokio::test]
async fn chooses_the_newest_token_when_an_owner_has_several() {
    let service = TestFixture::service();
    let now = Utc::now();
    let older = TestFixture::add_token_at(
        &service,
        "token-a",
        "owner-a",
        now - Duration::minutes(2),
        TestFixture::options(),
    )
    .await;
    let newer = TestFixture::add_token_at(
        &service,
        "token-z",
        "owner-a",
        now - Duration::minutes(1),
        TestFixture::options(),
    )
    .await;

    let tokens = service.tokens_for_owner("owner-a").await.unwrap();

    assert_eq!(
        tokens.iter().map(|token| token.id()).collect::<Vec<_>>(),
        vec![older.id(), newer.id()]
    );
    assert_eq!(
        service.token_for_owner("owner-a", now).await.unwrap().id(),
        newer.id()
    );
}

#[tokio::test]
async fn chooses_the_most_recently_used_token() {
    let service = TestFixture::service();
    let now = Utc::now();
    let older = TestFixture::add_token_at(
        &service,
        "token-a",
        "owner-a",
        now - Duration::minutes(2),
        TestFixture::options(),
    )
    .await;
    let newer = TestFixture::add_token_at(
        &service,
        "token-z",
        "owner-a",
        now - Duration::minutes(1),
        TestFixture::options(),
    )
    .await;
    service
        .authenticate(older.id(), older.secret(), None, now)
        .await
        .unwrap();

    let selected = service.token_for_owner("owner-a", now).await.unwrap();

    assert_eq!(selected.id(), older.id());
    assert_ne!(selected.id(), newer.id());
}
