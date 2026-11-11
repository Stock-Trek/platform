use crate::TokenAuditEvent;
use sha2::{Digest, Sha256};

pub(crate) struct TokenRotationIdentity;

impl TokenRotationIdentity {
    pub(crate) fn transaction_id(original_id: &str, replacement_id: &str) -> String {
        Self::id("transaction", &[original_id, replacement_id])
    }

    pub(crate) fn event_id(transaction_id: &str, index: usize, event: &TokenAuditEvent) -> String {
        let index = index.to_string();
        let action = event.action.to_string();
        let token_id = event.token_id.as_deref().unwrap_or_default();
        Self::id(
            "event",
            &[transaction_id, &index, action.as_str(), token_id],
        )
    }

    fn id(kind: &str, parts: &[&str]) -> String {
        let mut hasher = Sha256::new();
        for part in std::iter::once(kind).chain(parts.iter().copied()) {
            hasher.update((part.len() as u64).to_be_bytes());
            hasher.update(part.as_bytes());
        }
        format!("0-{}", data_encoding::HEXLOWER.encode(&hasher.finalize()))
    }
}
