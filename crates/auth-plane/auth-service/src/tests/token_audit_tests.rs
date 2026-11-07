use super::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{MemoryTokenAuditLog, TokenAuditAction};
use chrono::Utc;
use std::sync::Arc;

struct AuditFixture;

impl AuditFixture {
    fn service() -> (crate::AuthService, Arc<MemoryTokenAuditLog>) {
        let audit = Arc::new(MemoryTokenAuditLog::new(4096));
        let service = TestFixture::service_with_audit(audit.clone());
        (service, audit)
    }

    fn actions(audit: &MemoryTokenAuditLog) -> Vec<TokenAuditAction> {
        audit
            .events()
            .into_iter()
            .map(|event| event.action)
            .collect()
    }
}

#[tokio::test]
async fn records_token_issuance_and_authentication() {
    let (service, audit) = AuditFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;

    service
        .authenticate(token.id(), token.secret(), None, Utc::now())
        .await
        .unwrap();

    assert_eq!(
        AuditFixture::actions(&audit),
        vec![TokenAuditAction::Issued, TokenAuditAction::Authenticated]
    );
    let events = audit.events();
    assert_eq!(events[0].token_id(), Some("token-a"));
    assert_eq!(events[0].owner(), Some("owner-a"));
}

#[tokio::test]
async fn records_failed_authentication_without_the_secret() {
    let (service, audit) = AuditFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;

    let _ = service
        .authenticate(token.id(), "wrong-secret", None, Utc::now())
        .await;

    let events = audit.events();
    let failure = events
        .iter()
        .find(|event| event.action == TokenAuditAction::AuthenticationFailed)
        .unwrap();
    assert!(!failure.success);
    assert!(failure.detail().is_some());
    assert!(!format!("{events:?}").contains("wrong-secret"));
}

#[tokio::test]
async fn records_rotation_and_revocation() {
    let (service, audit) = AuditFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let rotated = service
        .rotate_token_to(token.id(), "token-b", now)
        .await
        .unwrap();

    service
        .revoke_token(rotated.id(), "no longer needed", "security-team", now)
        .await
        .unwrap();

    let actions = AuditFixture::actions(&audit);
    assert!(actions.contains(&TokenAuditAction::Rotated));
    assert!(actions.contains(&TokenAuditAction::Revoked));
}

#[tokio::test]
async fn records_signing_and_signature_verification() {
    let (service, audit) = AuditFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .unwrap();

    service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.signature(), now)
        .await
        .unwrap();

    let actions = AuditFixture::actions(&audit);
    assert!(actions.contains(&TokenAuditAction::Signed));
    assert!(actions.contains(&TokenAuditAction::SignatureVerified));
}

#[tokio::test]
async fn records_rejected_signatures() {
    let (service, audit) = AuditFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .unwrap();
    let tampered = TestFixture::tampered_signature(signed.signature());

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, tampered, now)
        .await
        .unwrap();

    assert!(!verified);
    assert!(AuditFixture::actions(&audit).contains(&TokenAuditAction::SignatureRejected));
}

#[tokio::test]
async fn audit_events_can_be_drained() {
    let (service, audit) = AuditFixture::service();
    TestFixture::add_token(&service, "token-a", "owner-a").await;

    assert_eq!(audit.drain().len(), 1);
    assert!(audit.events().is_empty());
}
