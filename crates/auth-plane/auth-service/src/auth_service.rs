use crate::{
    JwtSigningKey, JwtVerifier, ReplayStore, TokenAuditEvent, TokenMutation,
    TokenRotationTransaction, TokenUsageQueue, jwt_signing_key::DEFAULT_JWT_KEY_ID,
    token_store::TokenStore,
};
use chrono::Duration;
use jsonwebtoken::jwk::JwkSet;
use platform_errors::{AuthError, BuildError, BuildResult};
use std::sync::Arc;

pub const MAX_UPDATE_ATTEMPTS: usize = 5;

pub struct AuthServiceConfig {
    pub token_lifetime: Duration,
    pub rotation_overlap: Duration,
    pub signature_validity: Duration,
}

#[derive(Clone)]
pub struct AuthService {
    pub store: Arc<dyn TokenStore>,
    pub replay: Arc<dyn ReplayStore>,
    pub signature_validity: Duration,
    pub token_lifetime: Duration,
    pub rotation_overlap: Duration,
    pub jwt_signer: Arc<JwtSigningKey>,
    pub usage: Arc<dyn TokenUsageQueue>,
}

impl AuthService {
    pub fn new(
        store: Arc<dyn TokenStore>,
        config: AuthServiceConfig,
        replay: Arc<dyn ReplayStore>,
        usage: Arc<dyn TokenUsageQueue>,
    ) -> BuildResult<Self> {
        let signer = JwtSigningKey::generate(DEFAULT_JWT_KEY_ID)?;
        Self::with_jwt_signing_key(store, config, replay, Arc::new(signer), usage)
    }

    pub fn with_jwt_signing_key(
        store: Arc<dyn TokenStore>,
        config: AuthServiceConfig,
        replay: Arc<dyn ReplayStore>,
        jwt_signer: Arc<JwtSigningKey>,
        usage: Arc<dyn TokenUsageQueue>,
    ) -> BuildResult<Self> {
        Self::validate_config(&config)?;
        Ok(Self {
            store,
            replay,
            signature_validity: config.signature_validity,
            token_lifetime: config.token_lifetime,
            rotation_overlap: config.rotation_overlap,
            jwt_signer,
            usage,
        })
    }

    pub fn jwks(&self) -> &JwkSet {
        self.jwt_signer.jwks()
    }

    pub fn jwt_verifier(&self) -> JwtVerifier {
        JwtVerifier::new(self.jwks().clone())
    }

    fn validate_config(config: &AuthServiceConfig) -> BuildResult<()> {
        if config.token_lifetime <= Duration::zero() {
            return Err(BuildError::auth_invalid_field(
                "token_lifetime",
                "must be greater than zero",
            ));
        }
        if config.rotation_overlap < Duration::zero() {
            return Err(BuildError::auth_invalid_field(
                "rotation_overlap",
                "must not be negative",
            ));
        }
        if config.rotation_overlap > config.token_lifetime {
            return Err(BuildError::auth_invalid_field(
                "rotation_overlap",
                "must not exceed token_lifetime",
            ));
        }
        if config.signature_validity < Duration::seconds(1) {
            return Err(BuildError::auth_invalid_field(
                "signature_validity",
                "must be at least one second",
            ));
        }
        Ok(())
    }

    pub async fn commit_mutation(
        &self,
        mutation: TokenMutation,
        event: TokenAuditEvent,
    ) -> BuildResult<bool> {
        let index_mutation = mutation.clone();
        let applied = self.store.commit(mutation, event).await?;
        if applied {
            self.sync_owner_index_mutation(&index_mutation).await;
        }
        Ok(applied)
    }

    pub async fn commit_rotation(&self, transaction: &TokenRotationTransaction) -> BuildResult<()> {
        self.store.commit_rotation(transaction).await
    }

    pub async fn record_audit_failure(&self, event: TokenAuditEvent) {
        if let Err(error) = self.enqueue_usage(crate::TokenUsage::audit(event)).await {
            tracing::error!(%error, "failed to enqueue token audit event");
        }
    }

    pub fn is_conflict(error: &BuildError) -> bool {
        matches!(error, BuildError::Auth(AuthError::Conflict { .. }))
    }
}
