use super::support_tests::{AuthServiceBuilder, ORDER_REQUEST, TestFixture};
use crate::{MemoryTokenAuditLog, RevocationReason, RevokedBy, TokenAuditAction, TokenAuditEvent};
use chrono::{Duration, Utc};
use std::sync::Arc;

struct AuditFixture;

impl AuditFixture {
    fn service() -> (crate::AuthService, Arc<MemoryTokenAuditLog>) {
        let audit = Arc::new(MemoryTokenAuditLog::new(4096));
        let service = AuthServiceBuilder::new().build();
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

    TestFixture::drain(&service, audit.clone()).await;

    assert_eq!(
        AuditFixture::actions(&audit),
        vec![TokenAuditAction::Issued, TokenAuditAction::Authenticated]
    );
    let events = audit.events();
    assert_eq!(events[0].token_id.as_deref(), Some("token-a"));
    assert_eq!(events[0].owner.as_deref(), Some("owner-a"));
}

#[test]
fn serializes_audit_events_as_json() {
    let event = TokenAuditEvent::new(TokenAuditAction::Signed, Utc::now())
        .with_token("token-a", Some("owner-a"))
        .failure("signing failed");

    let value = serde_json::to_value(&event).unwrap();

    assert_eq!(value["action"], "signed");
    assert_eq!(value["token_id"], "token-a");
    assert_eq!(value["owner"], "owner-a");
    assert_eq!(value["success"], false);
    assert_eq!(value["detail"], "signing failed");
    let decoded: TokenAuditEvent = serde_json::from_value(value).unwrap();
    assert_eq!(decoded, event);
}

#[tokio::test]
async fn issuance_events_use_the_operation_time() {
    let (service, audit) = AuditFixture::service();
    let created_at = Utc::now() - Duration::hours(1);
    let now = Utc::now();

    service
        .add_token(
            "token-a",
            "owner-a",
            created_at,
            TestFixture::options(),
            now,
        )
        .await
        .unwrap();
    TestFixture::drain(&service, audit.clone()).await;

    let events = audit.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].action, TokenAuditAction::Issued);
    assert_eq!(events[0].timestamp, now);
}

#[tokio::test]
async fn records_failed_authentication_without_the_secret() {
    let (service, audit) = AuditFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;

    let _ = service
        .authenticate(token.id(), "wrong-secret", None, Utc::now())
        .await;
    TestFixture::drain(&service, audit.clone()).await;

    let events = audit.events();
    let failure = events
        .iter()
        .find(|event| event.action == TokenAuditAction::AuthenticationFailed)
        .unwrap();
    assert!(!failure.success);
    assert!(failure.detail.is_some());
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
    TestFixture::drain(&service, audit.clone()).await;

    service
        .revoke_token(
            rotated.id(),
            RevocationReason::NoLongerNeeded,
            RevokedBy::SecurityTeam,
            now,
        )
        .await
        .unwrap();
    TestFixture::drain(&service, audit.clone()).await;

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
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .unwrap();
    TestFixture::drain(&service, audit.clone()).await;

    let actions = AuditFixture::actions(&audit);
    assert!(actions.contains(&TokenAuditAction::Signed));
    assert!(actions.contains(&TokenAuditAction::SignatureVerified));
}

#[tokio::test]
async fn records_signing_failures() {
    let (service, audit) = AuditFixture::service();
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let issued_at = chrono::DateTime::<Utc>::MAX_UTC - Duration::seconds(1);

    let result = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, issued_at, now)
        .await;

    assert!(result.is_err());
    TestFixture::drain(&service, audit.clone()).await;
    let failure = audit
        .events()
        .into_iter()
        .find(|event| event.action == TokenAuditAction::Signed)
        .expect("a signing event should be recorded");
    assert!(!failure.success);
}

#[tokio::test]
async fn records_rejected_signatures_for_unknown_tokens() {
    let (service, audit) = AuditFixture::service();
    let now = Utc::now();

    let verified = service
        .verify_order_request("ghost", ORDER_REQUEST, "v1.0.0.nonce.token.signature", now)
        .await
        .unwrap();

    assert!(verified.is_invalid());
    TestFixture::drain(&service, audit.clone()).await;
    let rejection = audit
        .events()
        .into_iter()
        .find(|event| event.action == TokenAuditAction::SignatureRejected)
        .expect("a rejection event should be recorded");
    assert_eq!(rejection.token_id.as_deref(), Some("ghost"));
    assert_eq!(rejection.owner, None);
}

#[tokio::test]
async fn records_rejected_signatures_for_malformed_token_ids() {
    let (service, audit) = AuditFixture::service();
    let now = Utc::now();

    let verified = service
        .verify_order_request(
            "not/a/path",
            ORDER_REQUEST,
            "v1.0.0.nonce.token.signature",
            now,
        )
        .await
        .unwrap();

    assert!(verified.is_invalid());
    TestFixture::drain(&service, audit.clone()).await;
    let rejection = audit
        .events()
        .into_iter()
        .find(|event| event.action == TokenAuditAction::SignatureRejected)
        .expect("a rejection event should be recorded");
    assert_eq!(rejection.token_id.as_deref(), Some("not/a/path"));
    assert_eq!(rejection.owner, None);
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
    let tampered = TestFixture::tampered_signature(signed.envelope());

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, &tampered, now)
        .await
        .unwrap();

    assert!(verified.is_invalid());
    TestFixture::drain(&service, audit.clone()).await;
    assert!(AuditFixture::actions(&audit).contains(&TokenAuditAction::SignatureRejected));
}
