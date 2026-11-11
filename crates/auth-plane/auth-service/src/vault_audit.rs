use crate::{
    TokenAuditEvent,
    token_audit_outbox::{AUDIT_EVENT_PREFIX, TokenAuditOutbox},
    vault_client::VaultClient,
};
use platform_errors::{BpError, BpResult};

impl VaultClient {
    pub(crate) async fn pending_audits(&self) -> BpResult<Vec<TokenAuditEvent>> {
        let mut events = Vec::new();
        for key in self.keys_with_prefix(AUDIT_EVENT_PREFIX).await? {
            let Some((data, _)) = self.get_value_entry(&key).await? else {
                continue;
            };
            let event = TokenAuditOutbox::standalone_event(&data)?.ok_or_else(|| {
                BpError::auth_store(
                    "read",
                    "audit-outbox",
                    false,
                    "audit index record is not a standalone event".to_string(),
                )
            })?;
            events.push(event);
        }
        events.sort_by(|left, right| {
            left.timestamp
                .cmp(&right.timestamp)
                .then_with(|| left.event_id().cmp(&right.event_id()))
        });
        Ok(events)
    }

    pub(crate) async fn acknowledge_audit(&self, event_id: &str) -> BpResult<()> {
        self.delete_key(&TokenAuditOutbox::storage_key(event_id))
            .await
    }
}
