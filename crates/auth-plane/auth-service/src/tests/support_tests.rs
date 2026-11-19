use crate::tests::token_usage_queue_fixture_tests::ImmediateTokenUsageQueue;
use crate::{
    ApiToken, AuthService, AuthServiceConfig, MemoryReplayStore, MemoryTokenStore, TokenAuditLog,
    TokenAuditRelay, TokenMetadata, TokenScope, TokenStore, TokenUsageQueue,
    replay_store::ReplayStore, token_options::TokenExpiry, token_options::TokenOptions,
};
use chrono::{DateTime, Duration, Utc};
use platform_errors::PlatformResult;
use std::sync::Arc;

pub(super) const ORDER_REQUEST: &[u8] =
    br#"{"limit":{"base_dto":"BTC","quote_dto":"USDC","side":"buy"}}"#;

pub(super) const OWNER: &str = "demo-owner";

pub(super) const TEST_SIGNATURE: &str =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

pub(super) struct AuthServiceBuilder {
    store: Arc<dyn TokenStore>,
    config: AuthServiceConfig,
    replay: Arc<dyn ReplayStore>,
    usage: Option<Arc<dyn TokenUsageQueue>>,
}

impl AuthServiceBuilder {
    pub(super) fn new() -> Self {
        Self {
            store: Arc::new(MemoryTokenStore::default()),
            config: TestFixture::config(),
            replay: Arc::new(MemoryReplayStore::default()),
            usage: None,
        }
    }

    pub(super) fn store(mut self, store: Arc<dyn TokenStore>) -> Self {
        self.store = store;
        self
    }

    pub(super) fn replay(mut self, replay: Arc<dyn ReplayStore>) -> Self {
        self.replay = replay;
        self
    }

    pub(super) fn config(mut self, config: AuthServiceConfig) -> Self {
        self.config = config;
        self
    }

    pub(super) fn usage(mut self, usage: Arc<dyn TokenUsageQueue>) -> Self {
        self.usage = Some(usage);
        self
    }

    pub(super) fn token_lifetime(mut self, lifetime: Duration) -> Self {
        self.config.token_lifetime = lifetime;
        self
    }

    pub(super) fn rotation_overlap(mut self, overlap: Duration) -> Self {
        self.config.rotation_overlap = overlap;
        self
    }

    pub(super) fn signature_validity(mut self, validity: Duration) -> Self {
        self.config.signature_validity = validity;
        self
    }

    pub(super) fn build(self) -> AuthService {
        self.try_build().expect("valid service")
    }

    pub(super) fn try_build(self) -> PlatformResult<AuthService> {
        let usage = self
            .usage
            .unwrap_or_else(|| Arc::new(ImmediateTokenUsageQueue::new(self.store.clone())));
        AuthService::new(self.store, self.config, self.replay, usage)
    }
}

pub(super) struct TestFixture;

impl TestFixture {
    pub(super) fn config() -> AuthServiceConfig {
        AuthServiceConfig {
            token_lifetime: Duration::days(30),
            rotation_overlap: Duration::days(7),
            signature_validity: Duration::minutes(5),
        }
    }

    pub(super) fn service() -> AuthService {
        AuthServiceBuilder::new().build()
    }

    pub(super) fn service_with_usage(usage: Arc<dyn TokenUsageQueue>) -> AuthService {
        AuthServiceBuilder::new().usage(usage).build()
    }

    pub(super) fn immediate_usage(store: Arc<dyn TokenStore>) -> Arc<dyn TokenUsageQueue> {
        Arc::new(ImmediateTokenUsageQueue::new(store))
    }

    pub(super) async fn drain(service: &AuthService, audit: Arc<dyn TokenAuditLog>) -> usize {
        TokenAuditRelay::new(service.store.clone(), audit)
            .drain()
            .await
            .unwrap()
    }

    pub(super) fn metadata() -> TokenMetadata {
        TokenMetadata::new(Utc::now() + Duration::days(30), TokenScope::DEFAULT).unwrap()
    }

    pub(super) fn options() -> TokenOptions {
        TokenOptions::new(TokenExpiry::After(Duration::days(30)), TokenScope::DEFAULT)
    }

    pub(super) fn token(id: &str) -> ApiToken {
        ApiToken::generate(id, OWNER, Utc::now(), Self::metadata()).unwrap()
    }

    pub(super) async fn add_token(service: &AuthService, id: &str, owner: &str) -> ApiToken {
        let now = Utc::now();
        service
            .add_token(id, owner, now, Self::options(), now)
            .await
            .unwrap()
    }

    pub(super) async fn add_token_at(
        service: &AuthService,
        id: &str,
        owner: &str,
        created_at: DateTime<Utc>,
        options: TokenOptions,
    ) -> ApiToken {
        service
            .add_token(id, owner, created_at, options, created_at)
            .await
            .unwrap()
    }

    pub(super) async fn add_token_with_expiry(
        service: &AuthService,
        id: &str,
        owner: &str,
        expires_at: DateTime<Utc>,
    ) -> ApiToken {
        let options = TokenOptions::new(TokenExpiry::At(expires_at), TokenScope::DEFAULT);
        Self::add_token_at(service, id, owner, Utc::now(), options).await
    }

    pub(super) async fn add_expired_token(
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

    pub(super) fn tampered_signature(signature: &str) -> String {
        let mut tampered = signature.to_string();
        let replacement = if signature.ends_with('0') { "1" } else { "0" };
        let start = tampered.len().saturating_sub(1);
        tampered.replace_range(start.., replacement);
        tampered
    }
}
