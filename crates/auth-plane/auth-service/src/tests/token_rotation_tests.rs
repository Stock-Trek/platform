use super::support_tests::{ORDER_REQUEST, TestFixture};
use crate::AuthService;
use chrono::{Duration, Utc};
use platform_errors::{AuthError, BpError};

struct RotationFixture;

impl RotationFixture {
    fn service(overlap_seconds: i64) -> AuthService {
        TestFixture::service().with_rotation_overlap(Duration::seconds(overlap_seconds))
    }
}

#[tokio::test]
async fn rotation_keeps_the_old_token_valid_for_the_overlap() {
    let service = RotationFixture::service(60);
    let old = service.add_token("token-a", "owner-a").await.unwrap();
    let now = Utc::now();

    let replacement = service.rotate_token_at(old.id(), now).await.unwrap();

    assert_eq!(replacement.rotated_from(), Some(old.id()));
    let stored = service.token(old.id()).await.unwrap();
    assert_eq!(stored.replaced_by(), Some(replacement.id()));
    assert_eq!(
        stored.rotation_overlap_until(),
        Some(now + Duration::seconds(60))
    );
    assert!(
        service
            .authenticate_at(old.id(), old.secret(), now + Duration::seconds(30))
            .await
            .is_ok()
    );
    assert!(
        service
            .authenticate_at(old.id(), old.secret(), now + Duration::seconds(61))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_rotated_token_can_be_used_after_the_overlap_ends() {
    let service = RotationFixture::service(1);
    let old = service.add_token("token-a", "owner-a").await.unwrap();
    let now = Utc::now();

    let replacement = service.rotate_token_at(old.id(), now).await.unwrap();

    assert!(
        service
            .authenticate_at(
                replacement.id(),
                replacement.secret(),
                now + Duration::seconds(2)
            )
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn owner_signing_prefers_the_rotated_token_during_the_overlap() {
    let service = RotationFixture::service(60);
    let old = service.add_token("token-a", "owner-a").await.unwrap();
    let replacement = service.rotate_token_to(old.id(), "token-b").await.unwrap();

    let signed = service
        .sign_order_request_for_owner("owner-a", ORDER_REQUEST)
        .await
        .unwrap();

    assert_eq!(signed.token_id(), replacement.id());
}

#[tokio::test]
async fn a_token_can_only_be_rotated_once() {
    let service = RotationFixture::service(60);
    let token = service.add_token("token-a", "owner-a").await.unwrap();
    service
        .rotate_token_to(token.id(), "token-b")
        .await
        .unwrap();

    let error = service.rotate_token_to(token.id(), "token-c").await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "token_id",
            ..
        }))
    ));
}
