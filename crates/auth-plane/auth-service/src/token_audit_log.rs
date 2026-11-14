use crate::token_audit_event::TokenAuditEvent;
use async_trait::async_trait;
use platform_errors::PlatformResult;

#[async_trait]
pub trait TokenAuditLog: Send + Sync {
    async fn record(&self, event: TokenAuditEvent) -> PlatformResult<()>;
}
