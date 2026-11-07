use super::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{MemoryTokenAuditLog, TokenAuditAction};
use std::sync::Arc;

struct AuditFixture;

impl AuditFixture {
    fn service() -> (crate::AuthService, Arc<MemoryTokenAuditLog>) {
        let audit = Arc::new(MemoryTokenAuditLog::new());
        let service = TestFixture::service().with_audit_log(audit.clone());
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
    let token = service.add_token("token-a", "owner-a").await.unwrap();

    service
        .authenticate(token.id(), token.secret())
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
    let token = service.add_token("token-a", "owner-a").await.unwrap();

    let _ = service.authenticate(token.id(), "wrong-secret").await;

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
    let token = service.add_token("token-a", "owner-a").await.unwrap();
    let rotated = service
        .rotate_token_to(token.id(), "token-b")
        .await
        .unwrap();

    service
        .revoke_token(rotated.id(), "no longer needed")
        .await
        .unwrap();

    let actions = AuditFixture::actions(&audit);
    assert!(actions.contains(&TokenAuditAction::Rotated));
    assert!(actions.contains(&TokenAuditAction::Revoked));
}

#[tokio::test]
async fn records_signing_and_signature_verification() {
    let (service, audit) = AuditFixture::service();
    let token = service.add_token("token-a", "owner-a").await.unwrap();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST)
        .await
        .unwrap();

    service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.signature())
        .await
        .unwrap();

    let actions = AuditFixture::actions(&audit);
    assert!(actions.contains(&TokenAuditAction::Signed));
    assert!(actions.contains(&TokenAuditAction::SignatureVerified));
}

#[tokio::test]
async fn records_rejected_signatures() {
    let (service, audit) = AuditFixture::service();
    let token = service.add_token("token-a", "owner-a").await.unwrap();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST)
        .await
        .unwrap();
    let tampered = TestFixture::tampered_signature(signed.signature());

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, tampered)
        .await
        .unwrap();

    assert!(!verified);
    assert!(AuditFixture::actions(&audit).contains(&TokenAuditAction::SignatureRejected));
}

#[tokio::test]
async fn audit_events_can_be_drained() {
    let service = TestFixture::service();
    service.add_token("token-a", "owner-a").await.unwrap();

    assert_eq!(service.drain_audit_events().len(), 1);
    assert!(service.audit_events().is_empty());
}
