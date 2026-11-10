use super::support_tests::{AuthServiceBuilder, TestFixture};
use crate::{ApiToken, AuthService, RevocationReason, RevokedBy};
use chrono::{DateTime, Duration, Utc};
use platform_errors::{AuthError, BpError};

struct RotationRecoveryFixture;

impl RotationRecoveryFixture {
    fn service(overlap_seconds: i64) -> AuthService {
        AuthServiceBuilder::new()
            .rotation_overlap(Duration::seconds(overlap_seconds))
            .build()
    }

    fn service_with_lifetime(lifetime_seconds: i64, overlap_seconds: i64) -> AuthService {
        AuthServiceBuilder::new()
            .token_lifetime(Duration::seconds(lifetime_seconds))
            .rotation_overlap(Duration::seconds(overlap_seconds))
            .build()
    }

    async fn orphan_replacement(
        service: &AuthService,
        original: &ApiToken,
        replacement_id: &str,
        now: DateTime<Utc>,
    ) -> ApiToken {
        let mut metadata = TestFixture::metadata();
        metadata.rotated_from = Some(original.id().to_string());
        let replacement =
            ApiToken::generate(replacement_id, original.owner(), now, metadata).unwrap();
        service.store.put(&replacement).await.unwrap();
        replacement
    }

    fn assert_replacement_id_error(error: BpError) {
        assert!(matches!(
            error,
            BpError::Auth(AuthError::InvalidField {
                field: "replacement_id",
                ..
            })
        ));
    }
}

#[tokio::test]
async fn retrying_a_completed_rotation_rejects_a_revoked_replacement() {
    let service = RotationRecoveryFixture::service(60);
    let original = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    service
        .rotate_token_to(original.id(), "token-b", now)
        .await
        .unwrap();
    service
        .revoke_token(
            "token-b",
            RevocationReason::Compromised,
            RevokedBy::SecurityTeam,
            now,
        )
        .await
        .unwrap();

    let error = service
        .rotate_token_to(original.id(), "token-b", now)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BpError::Auth(AuthError::Revoked { ref token_id }) if token_id == "token-b"
    ));
}

#[tokio::test]
async fn retrying_a_completed_rotation_rejects_an_expired_replacement() {
    let service = RotationRecoveryFixture::service_with_lifetime(1, 1);
    let original = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    service
        .rotate_token_to(original.id(), "token-b", now)
        .await
        .unwrap();

    let error = service
        .rotate_token_to(original.id(), "token-b", now + Duration::seconds(2))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BpError::Auth(AuthError::Expired { ref token_id }) if token_id == "token-b"
    ));
}

#[tokio::test]
async fn retrying_a_completed_rotation_rejects_a_node_that_was_rotated_again() {
    let service = RotationRecoveryFixture::service(60);
    let original = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    service
        .rotate_token_to(original.id(), "token-b", now)
        .await
        .unwrap();
    service
        .rotate_token_to("token-b", "token-c", now)
        .await
        .unwrap();

    let error = service
        .rotate_token_to(original.id(), "token-b", now)
        .await
        .unwrap_err();

    RotationRecoveryFixture::assert_replacement_id_error(error);
}

#[tokio::test]
async fn existing_dead_replacement_is_rejected_before_the_original_is_retired() {
    let service = RotationRecoveryFixture::service(60);
    let original = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    RotationRecoveryFixture::orphan_replacement(&service, &original, "token-b", now).await;
    service
        .revoke_token(
            "token-b",
            RevocationReason::Compromised,
            RevokedBy::SecurityTeam,
            now,
        )
        .await
        .unwrap();

    let error = service
        .rotate_token_to(original.id(), "token-b", now)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BpError::Auth(AuthError::Revoked { ref token_id }) if token_id == "token-b"
    ));
    assert!(
        service
            .token(original.id())
            .await
            .unwrap()
            .metadata()
            .replaced_by()
            .is_none()
    );
}

#[tokio::test]
async fn existing_replacement_that_was_rotated_again_is_rejected() {
    let service = RotationRecoveryFixture::service(60);
    let original = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    RotationRecoveryFixture::orphan_replacement(&service, &original, "token-b", now).await;
    service
        .rotate_token_to("token-b", "token-c", now)
        .await
        .unwrap();

    let error = service
        .rotate_token_to(original.id(), "token-b", now)
        .await
        .unwrap_err();

    RotationRecoveryFixture::assert_replacement_id_error(error);
    assert!(
        service
            .token(original.id())
            .await
            .unwrap()
            .metadata()
            .replaced_by()
            .is_none()
    );
}
