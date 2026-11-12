use super::support_tests::{AuthServiceBuilder, TestFixture};
use crate::{AuthService, api_token::MAX_TOKEN_ID_LENGTH};
use chrono::{Duration, Utc};
use platform_errors::{AuthError, BuildError};

struct RotationFixture;

impl RotationFixture {
    fn service(overlap_seconds: i64) -> AuthService {
        AuthServiceBuilder::new()
            .rotation_overlap(Duration::seconds(overlap_seconds))
            .build()
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
    let error = service
        .authenticate(old.id(), old.secret(), None, now + Duration::seconds(61))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BuildError::Auth(AuthError::RotationOverlapEnded { ref token_id })
            if token_id == old.id()
    ));
}

#[tokio::test]
async fn rotation_applies_the_configured_token_lifetime() {
    let service = AuthServiceBuilder::new()
        .token_lifetime(Duration::minutes(5))
        .rotation_overlap(Duration::minutes(1))
        .build();
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
async fn owner_token_selection_prefers_the_rotated_token_during_the_overlap() {
    let service = RotationFixture::service(60);
    let old = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let replacement = service
        .rotate_token_to(old.id(), "token-b", now)
        .await
        .unwrap();

    let selected = service.token_for_owner("owner-a", now).await.unwrap();

    assert_eq!(selected.id(), replacement.id());
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
        Err(BuildError::Auth(AuthError::InvalidField {
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
        Err(BuildError::Auth(AuthError::InvalidField {
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
        Err(BuildError::Auth(AuthError::InvalidField {
            field: "token_id",
            ..
        }))
    ));
}
