use super::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{AuthService, api_token::MAX_TOKEN_ID_LENGTH};
use chrono::{Duration, Utc};
use platform_errors::{AuthError, BpError};

struct RotationFixture;

impl RotationFixture {
    fn service(overlap_seconds: i64) -> AuthService {
        let mut config = TestFixture::config();
        config.rotation_overlap = Duration::seconds(overlap_seconds);
        crate::AuthService::new(
            std::sync::Arc::new(crate::MemoryTokenStore::default()),
            config,
        )
        .unwrap()
    }
}

#[tokio::test]
async fn rotation_keeps_the_old_token_valid_for_the_overlap() {
    let service = RotationFixture::service(60);
    let old = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();

    let replacement = service.rotate_token(old.id(), now).await.unwrap();

    assert_eq!(replacement.metadata().rotated_from(), Some(old.id()));
    let stored = service.token(old.id()).await.unwrap();
    assert_eq!(stored.metadata().replaced_by(), Some(replacement.id()));
    assert_eq!(
        stored.metadata().rotation_overlap_until(),
        Some(now + Duration::seconds(60))
    );
    assert!(
        service
            .authenticate(old.id(), old.secret(), None, now + Duration::seconds(30))
            .await
            .is_ok()
    );
    assert!(
        service
            .authenticate(old.id(), old.secret(), None, now + Duration::seconds(61))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn rotation_applies_the_configured_token_lifetime() {
    let mut config = TestFixture::config();
    config.token_lifetime = Duration::minutes(5);
    config.rotation_overlap = Duration::minutes(1);
    let service = crate::AuthService::new(
        std::sync::Arc::new(crate::MemoryTokenStore::default()),
        config,
    )
    .unwrap();
    let old = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();

    let replacement = service.rotate_token(old.id(), now).await.unwrap();

    assert_eq!(
        replacement.metadata().expires_at(),
        now + Duration::minutes(5)
    );
}

#[tokio::test]
async fn a_rotated_token_can_be_used_after_the_overlap_ends() {
    let service = RotationFixture::service(1);
    let old = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();

    let replacement = service.rotate_token(old.id(), now).await.unwrap();

    assert!(
        service
            .authenticate(
                replacement.id(),
                replacement.secret(),
                None,
                now + Duration::seconds(2)
            )
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn owner_signing_prefers_the_rotated_token_during_the_overlap() {
    let service = RotationFixture::service(60);
    let old = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let replacement = service
        .rotate_token_to(old.id(), "token-b", now)
        .await
        .unwrap();

    let signed = service
        .sign_order_request_for_owner("owner-a", ORDER_REQUEST, now, now)
        .await
        .unwrap();

    assert_eq!(signed.token_id(), replacement.id());
}

#[tokio::test]
async fn rotation_rejects_reusing_the_original_id() {
    let service = RotationFixture::service(60);
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;

    let error = service
        .rotate_token_to(token.id(), token.id(), Utc::now())
        .await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "replacement_id",
            ..
        }))
    ));
    assert!(
        service
            .token(token.id())
            .await
            .unwrap()
            .metadata()
            .replaced_by()
            .is_none()
    );
}

#[tokio::test]
async fn generated_replacement_ids_respect_the_token_id_limit() {
    let service = RotationFixture::service(60);
    let id = "a".repeat(MAX_TOKEN_ID_LENGTH);
    let token = TestFixture::add_token(&service, &id, "owner-a").await;

    let replacement = service.rotate_token(token.id(), Utc::now()).await.unwrap();

    assert!(replacement.id().len() <= MAX_TOKEN_ID_LENGTH);
    assert_eq!(replacement.metadata().rotated_from(), Some(token.id()));
}

#[tokio::test]
async fn rotation_rejects_an_invalid_replacement_id() {
    let service = RotationFixture::service(60);
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;

    let error = service
        .rotate_token_to(token.id(), "not/a/path", Utc::now())
        .await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "replacement_id",
            ..
        }))
    ));
}

#[tokio::test]
async fn a_token_can_only_be_rotated_once() {
    let service = RotationFixture::service(60);
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    service
        .rotate_token_to(token.id(), "token-b", now)
        .await
        .unwrap();

    let error = service.rotate_token_to(token.id(), "token-c", now).await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "token_id",
            ..
        }))
    ));
}
