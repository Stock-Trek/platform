use super::support_tests::TestFixture;
use chrono::Utc;
use platform_errors::{AuthError, BpError};

#[tokio::test]
async fn rejects_rotation_back_to_the_predecessor_id() {
    let service = TestFixture::service();
    let original = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let replacement = service
        .rotate_token_to(original.id(), "token-b", now)
        .await
        .unwrap();
    service.delete_token(original.id(), now).await.unwrap();

    let error = service
        .rotate_token_to(replacement.id(), original.id(), now)
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
            .token(replacement.id())
            .await
            .unwrap()
            .metadata()
            .replaced_by()
            .is_none()
    );
    assert!(matches!(
        service.token(original.id()).await,
        Err(BpError::Auth(AuthError::UnknownToken { .. }))
    ));
}

#[tokio::test]
async fn rejects_rotation_that_would_close_a_cross_token_cycle() {
    let service = TestFixture::service();
    let original = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let replacement = service
        .rotate_token_to(original.id(), "token-b", now)
        .await
        .unwrap();
    service.delete_token(original.id(), now).await.unwrap();
    let next = service
        .rotate_token_to(replacement.id(), "token-c", now)
        .await
        .unwrap();

    let error = service.rotate_token_to(next.id(), original.id(), now).await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "replacement_id",
            ..
        }))
    ));
    assert!(
        service
            .token(next.id())
            .await
            .unwrap()
            .metadata()
            .replaced_by()
            .is_none()
    );
}

#[tokio::test]
async fn rotates_a_replacement_forward_to_a_new_id() {
    let service = TestFixture::service();
    let original = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let replacement = service
        .rotate_token_to(original.id(), "token-b", now)
        .await
        .unwrap();

    let next = service
        .rotate_token_to(replacement.id(), "token-c", now)
        .await
        .unwrap();

    assert_eq!(next.metadata().rotated_from(), Some(replacement.id()));
    assert_eq!(
        service
            .token(replacement.id())
            .await
            .unwrap()
            .metadata()
            .replaced_by(),
        Some(next.id())
    );
}
