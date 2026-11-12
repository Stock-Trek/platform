use crate::tests::support_tests::{AuthServiceBuilder, TestFixture};
use crate::{
    ApiToken, AuthService, MemoryTokenStore, TokenAuditAction, TokenAuditEvent,
    TokenRotationTransaction, TokenStore,
};
use chrono::{DateTime, Duration, Utc};
use platform_errors::{AuthError, BuildError};
use secrecy::{ExposeSecret, SecretString};

struct RotationIdempotencyFixture;

impl RotationIdempotencyFixture {
    fn service() -> AuthService {
        AuthServiceBuilder::new()
            .rotation_overlap(Duration::seconds(60))
            .build()
    }

    fn replacement(original: &ApiToken, id: &str, secret: &str, now: DateTime<Utc>) -> ApiToken {
        let mut metadata = TestFixture::metadata();
        metadata.rotated_from = Some(original.id().to_string());
        ApiToken::from_parts(
            id,
            original.owner(),
            SecretString::from(secret.to_string()),
            now,
            metadata,
        )
        .unwrap()
    }

    fn transaction(
        original: &ApiToken,
        replacement: &ApiToken,
        now: DateTime<Utc>,
    ) -> TokenRotationTransaction {
        let mut retired = original.clone();
        retired
            .metadata
            .begin_rotation(replacement.id(), now + Duration::seconds(60));
        let events = vec![
            TokenAuditEvent::new(TokenAuditAction::Rotated, now)
                .with_token(original.id(), Some(original.owner())),
            TokenAuditEvent::new(TokenAuditAction::Issued, now)
                .with_token(replacement.id(), Some(replacement.owner())),
        ];
        TokenRotationTransaction::new(original.clone(), retired, replacement.clone(), events)
            .unwrap()
    }
}

#[tokio::test]
async fn retrying_a_partial_rotation_returns_the_persisted_secret() {
    let service = RotationIdempotencyFixture::service();
    let original = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let persisted =
        RotationIdempotencyFixture::replacement(&original, "token-b", "persisted-secret", now);
    service.store.put(&persisted).await.unwrap();

    let replacement = service
        .rotate_token_to(original.id(), "token-b", now)
        .await
        .unwrap();

    assert!(replacement.matches_secret("persisted-secret"));
    assert_eq!(
        replacement.secret().expose_secret(),
        persisted.secret().expose_secret()
    );
    assert_eq!(
        service
            .token(original.id())
            .await
            .unwrap()
            .metadata()
            .replaced_by(),
        Some("token-b")
    );
}

#[tokio::test]
async fn materializing_a_different_secret_for_an_existing_replacement_is_rejected() {
    let store = MemoryTokenStore::default();
    let now = Utc::now();
    let original = TestFixture::token("token-a");
    store.put(&original).await.unwrap();
    let first =
        RotationIdempotencyFixture::replacement(&original, "token-b", "persisted-secret", now);
    let first_transaction = RotationIdempotencyFixture::transaction(&original, &first, now);
    store
        .materialize_rotation(&first_transaction)
        .await
        .unwrap();

    let competing =
        RotationIdempotencyFixture::replacement(&original, "token-b", "unstored-secret", now);
    let competing_transaction = RotationIdempotencyFixture::transaction(&original, &competing, now);
    let error = store
        .materialize_rotation(&competing_transaction)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BuildError::Auth(AuthError::Conflict { ref token_id }) if token_id == "token-b"
    ));
    let stored = store.get("token-b").await.unwrap().unwrap();
    assert!(stored.matches_secret("persisted-secret"));
    assert!(!stored.matches_secret("unstored-secret"));
}
