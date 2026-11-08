use super::support_tests::TestFixture;
use crate::{TokenAuditAction, TokenAuditEvent, TokenAuditLog};
use async_trait::async_trait;
use chrono::Utc;
use platform_errors::{AuthError, BpError, BpResult};
use std::sync::Arc;

struct FailingAuditLog;

#[async_trait]
impl TokenAuditLog for FailingAuditLog {
    async fn record(&self, _event: TokenAuditEvent) -> BpResult<()> {
        Err(BpError::auth_invalid_field("audit", "queue unavailable"))
    }
}

#[tokio::test]
async fn propagates_audit_delivery_failures() {
    let service = TestFixture::service_with_audit(Arc::new(FailingAuditLog));
    let now = Utc::now();

    let error = service
        .add_token("token-a", "owner-a", now, TestFixture::options(), now)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BpError::Auth(AuthError::InvalidField { field: "audit", .. })
    ));
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
