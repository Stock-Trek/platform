use crate::{ApiToken, AuthService, MemoryTokenStore};
use std::sync::Arc;

pub(crate) const ORDER_REQUEST: &[u8] =
    br#"{"limit":{"base_dto":"BTC","quote_dto":"USDC","side":"buy"}}"#;

pub(crate) struct TestFixture;

impl TestFixture {
    pub(crate) fn token(id: &str) -> ApiToken {
        ApiToken::generate(id, "demo-owner").unwrap()
    }

    pub(crate) fn service() -> AuthService {
        AuthService::new(Arc::new(MemoryTokenStore::new()))
    }

    pub(crate) fn tampered_signature(signature: &str) -> String {
        let mut tampered = signature.to_string();
        let replacement = if signature.ends_with('0') { "1" } else { "0" };
        let start = tampered.len().saturating_sub(1);
        tampered.replace_range(start.., replacement);
        tampered
    }
}
