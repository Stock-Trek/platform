use crate::{
    ApiToken, AuthService, AuthServiceConfig, MemoryTokenAuditLog, MemoryTokenStore, TokenExpiry,
    TokenMetadata, TokenOptions, TokenScope,
};
use chrono::{DateTime, Duration, Utc};
use std::sync::Arc;

pub(crate) const ORDER_REQUEST: &[u8] =
    br#"{"limit":{"base_dto":"BTC","quote_dto":"USDC","side":"buy"}}"#;

pub(crate) const OWNER: &str = "demo-owner";

pub(crate) struct TestFixture;

impl TestFixture {
    pub(crate) fn config() -> AuthServiceConfig {
        Self::config_with_audit(Arc::new(MemoryTokenAuditLog::new(4096)))
    }

    pub(crate) fn config_with_audit(audit: Arc<MemoryTokenAuditLog>) -> AuthServiceConfig {
        AuthServiceConfig {
            token_lifetime: Duration::days(30),
            rotation_overlap: Duration::days(7),
            signature_validity: Duration::minutes(5),
            audit,
        }
    }

    pub(crate) fn service() -> AuthService {
        AuthService::new(Arc::new(MemoryTokenStore::default()), Self::config()).unwrap()
    }

    pub(crate) fn service_with_audit(audit: Arc<MemoryTokenAuditLog>) -> AuthService {
        AuthService::new(
            Arc::new(MemoryTokenStore::default()),
            Self::config_with_audit(audit),
        )
        .unwrap()
    }

    pub(crate) fn metadata() -> TokenMetadata {
        TokenMetadata::new(Utc::now() + Duration::days(30), TokenScope::DEFAULT).unwrap()
    }

    pub(crate) fn options() -> TokenOptions {
        TokenOptions::new(TokenExpiry::After(Duration::days(30)), TokenScope::DEFAULT)
    }

    pub(crate) fn token(id: &str) -> ApiToken {
        ApiToken::generate(id, OWNER, Utc::now(), Self::metadata()).unwrap()
    }

    pub(crate) async fn add_token(service: &AuthService, id: &str, owner: &str) -> ApiToken {
        service
            .add_token(id, owner, Utc::now(), Self::options())
            .await
            .unwrap()
    }

    pub(crate) async fn add_token_at(
        service: &AuthService,
        id: &str,
        owner: &str,
        created_at: DateTime<Utc>,
        options: TokenOptions,
    ) -> ApiToken {
        service
            .add_token(id, owner, created_at, options)
            .await
            .unwrap()
    }

    pub(crate) async fn add_expired_token(
        service: &AuthService,
        id: &str,
        owner: &str,
        created_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> ApiToken {
        let metadata = TokenMetadata::new(expires_at, TokenScope::DEFAULT).unwrap();
        let token = ApiToken::generate(id, owner, created_at, metadata).unwrap();
        service.store.put(&token).await.unwrap();
        token
    }

    pub(crate) fn tampered_signature(signature: &str) -> String {
        let mut tampered = signature.to_string();
        let replacement = if signature.ends_with('0') { "1" } else { "0" };
        let start = tampered.len().saturating_sub(1);
        tampered.replace_range(start.., replacement);
        tampered
    }
}
