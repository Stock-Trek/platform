use crate::tests::support_tests::TestFixture;
use chrono::Utc;
use platform_errors::{AuthError, PlatformError};
use secrecy::ExposeSecret;
use std::sync::Arc;

#[tokio::test]
async fn adding_a_duplicate_token_fails_without_replacing_it() {
    let service = TestFixture::service();
    let original = TestFixture::add_token(&service, "token-a", "demo-owner").await;

    let now = Utc::now();
    let error = service
        .add_token("token-a", "other-owner", now, TestFixture::options(), now)
        .await;

    assert!(matches!(
        error,
        Err(PlatformError::Auth(AuthError::TokenExists { .. }))
    ));
    let stored = service.token("token-a").await.unwrap();
    assert_eq!(stored.owner(), original.owner());
    assert_eq!(
        stored.secret().expose_secret(),
        original.secret().expose_secret()
    );
}

#[tokio::test]
async fn concurrent_duplicate_adds_only_create_one_token() {
    let service = Arc::new(TestFixture::service());
    let first_service = Arc::clone(&service);
    let second_service = Arc::clone(&service);

    let now = Utc::now();
    let first_add =
        first_service.add_token("token-a", "first-owner", now, TestFixture::options(), now);
    let second_add =
        second_service.add_token("token-a", "second-owner", now, TestFixture::options(), now);
    let (first, second) = tokio::join!(first_add, second_add);

    assert!(first.is_ok() ^ second.is_ok());
    let ids: Vec<String> = service
        .tokens_for_owner("first-owner")
        .await
        .unwrap()
        .into_iter()
        .chain(service.tokens_for_owner("second-owner").await.unwrap())
        .map(|token| token.id().to_string())
        .collect();
    assert_eq!(ids, vec!["token-a"]);
}

#[tokio::test]
async fn deleting_a_missing_token_is_idempotent() {
    let service = TestFixture::service();

    service.delete_token("ghost", Utc::now()).await.unwrap();
    service.delete_token("ghost", Utc::now()).await.unwrap();
}

#[tokio::test]
async fn token_ids_are_listed_sorted() {
    let service = TestFixture::service();
    TestFixture::add_token(&service, "token-b", "demo-owner").await;
    TestFixture::add_token(&service, "token-a", "demo-owner").await;

    let ids: Vec<String> = service
        .tokens_for_owner("demo-owner")
        .await
        .unwrap()
        .into_iter()
        .map(|token| token.id().to_string())
        .collect();

    assert_eq!(ids, vec!["token-a", "token-b"]);
}

#[tokio::test]
async fn looks_up_a_stored_token() {
    let service = TestFixture::service();
    let added = TestFixture::add_token(&service, "token-a", "demo-owner").await;

    let stored = service.token(added.id()).await.unwrap();

    assert_eq!(stored.id(), added.id());
    assert_eq!(stored.owner(), added.owner());
}

#[tokio::test]
async fn looking_up_an_unknown_token_fails() {
    let service = TestFixture::service();

    let error = service.token("ghost").await;

    assert!(matches!(
        error,
        Err(PlatformError::Auth(AuthError::UnknownToken { .. }))
    ));
}

#[tokio::test]
async fn authenticates_a_token_with_its_secret() {
    let service = TestFixture::service();
    let added = TestFixture::add_token(&service, "token-a", "demo-owner").await;

    let authenticated = service
        .authenticate(added.id(), added.secret(), None, Utc::now())
        .await
        .unwrap();

    assert_eq!(authenticated.id(), added.id());
}

#[tokio::test]
async fn authentication_rejects_the_wrong_secret() {
    let service = TestFixture::service();
    let added = TestFixture::add_token(&service, "token-a", "demo-owner").await;

    let error = service
        .authenticate(added.id(), "wrong-secret", None, Utc::now())
        .await;

    assert!(matches!(
        error,
        Err(PlatformError::Auth(AuthError::InvalidSecret { ref token_id }))
            if token_id == added.id()
    ));
}

#[tokio::test]
async fn authenticating_an_unknown_token_fails() {
    let service = TestFixture::service();

    let error = service
        .authenticate("ghost", "secret", None, Utc::now())
        .await;

    assert!(matches!(
        error,
        Err(PlatformError::Auth(AuthError::UnknownToken { .. }))
    ));
}

#[tokio::test]
async fn rejects_path_traversal_token_ids_before_store_access() {
    let service = TestFixture::service();

    for id in ["../metadata", "../../../sys/health", "not/a/path", ""] {
        let error = service.token(id).await;
        assert!(matches!(
            error,
            Err(PlatformError::Auth(AuthError::InvalidField { .. }))
        ));
        let error = service.delete_token(id, Utc::now()).await;
        assert!(matches!(
            error,
            Err(PlatformError::Auth(AuthError::InvalidField { .. }))
        ));
        let error = service.authenticate(id, "secret", None, Utc::now()).await;
        assert!(matches!(
            error,
            Err(PlatformError::Auth(AuthError::InvalidField { .. }))
        ));
    }
}
