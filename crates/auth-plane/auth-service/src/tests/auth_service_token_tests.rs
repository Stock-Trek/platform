use super::support_tests::TestFixture;
use platform_errors::{AuthError, BpError};
use secrecy::ExposeSecret;
use std::sync::Arc;

#[tokio::test]
async fn adding_a_duplicate_token_fails_without_replacing_it() {
    let service = TestFixture::service();
    let original = service.add_token("token-a", "demo-owner").await.unwrap();

    let error = service.add_token("token-a", "other-owner").await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::TokenExists { .. }))
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

    let (first, second) = tokio::join!(
        first_service.add_token("token-a", "first-owner"),
        second_service.add_token("token-a", "second-owner"),
    );

    assert!(first.is_ok() ^ second.is_ok());
    assert_eq!(service.token_ids().await.unwrap(), vec!["token-a"]);
}

#[tokio::test]
async fn deleting_a_missing_token_is_idempotent() {
    let service = TestFixture::service();

    service.delete_token("ghost").await.unwrap();
    service.delete_token("ghost").await.unwrap();
}

#[tokio::test]
async fn token_ids_are_listed_sorted() {
    let service = TestFixture::service();
    service.add_token("token-b", "demo-owner").await.unwrap();
    service.add_token("token-a", "demo-owner").await.unwrap();

    let ids = service.token_ids().await.unwrap();

    assert_eq!(ids, vec!["token-a".to_string(), "token-b".to_string()]);
}

#[tokio::test]
async fn looks_up_a_stored_token() {
    let service = TestFixture::service();
    let added = service.add_token("token-a", "demo-owner").await.unwrap();

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
        Err(BpError::Auth(AuthError::UnknownToken { .. }))
    ));
}

#[tokio::test]
async fn authenticates_a_token_with_its_secret() {
    let service = TestFixture::service();
    let added = service.add_token("token-a", "demo-owner").await.unwrap();

    let authenticated = service
        .authenticate(added.id(), added.secret())
        .await
        .unwrap();

    assert_eq!(authenticated.id(), added.id());
}

#[tokio::test]
async fn authentication_rejects_the_wrong_secret() {
    let service = TestFixture::service();
    let added = service.add_token("token-a", "demo-owner").await.unwrap();

    let error = service.authenticate(added.id(), "wrong-secret").await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField { .. }))
    ));
}

#[tokio::test]
async fn authenticating_an_unknown_token_fails() {
    let service = TestFixture::service();

    let error = service.authenticate("ghost", "secret").await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::UnknownToken { .. }))
    ));
}

#[tokio::test]
async fn rejects_path_traversal_token_ids_before_store_access() {
    let service = TestFixture::service();

    for id in ["../metadata", "../../../sys/health", "not/a/path", ""] {
        let error = service.token(id).await;
        assert!(matches!(
            error,
            Err(BpError::Auth(AuthError::InvalidField { .. }))
        ));
        let error = service.delete_token(id).await;
        assert!(matches!(
            error,
            Err(BpError::Auth(AuthError::InvalidField { .. }))
        ));
        let error = service.authenticate(id, "secret").await;
        assert!(matches!(
            error,
            Err(BpError::Auth(AuthError::InvalidField { .. }))
        ));
    }
}
