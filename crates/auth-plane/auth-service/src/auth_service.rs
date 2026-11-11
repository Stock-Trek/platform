use crate::{
    JwtSigningKey, JwtVerifier, ReplayStore, TokenAuditEvent, TokenMutation,
    TokenRotationTransaction, jwt_signing_key::DEFAULT_JWT_KEY_ID, token_store::TokenStore,
};
use chrono::Duration;
use jsonwebtoken::jwk::JwkSet;
use platform_errors::{AuthError, BpError, BpResult};
use std::sync::Arc;

pub(crate) const MAX_UPDATE_ATTEMPTS: usize = 5;

pub struct AuthServiceConfig {
    pub token_lifetime: Duration,
    pub rotation_overlap: Duration,
    pub signature_validity: Duration,
}

#[derive(Clone)]
pub struct AuthService {
    pub(crate) store: Arc<dyn TokenStore>,
    pub(crate) replay: Arc<dyn ReplayStore>,
    pub(crate) signature_validity: Duration,
    pub(crate) token_lifetime: Duration,
    pub(crate) rotation_overlap: Duration,
    pub(crate) jwt_signer: Arc<JwtSigningKey>,
}

impl AuthService {
    pub fn new(
        store: Arc<dyn TokenStore>,
        config: AuthServiceConfig,
        replay: Arc<dyn ReplayStore>,
    ) -> BpResult<Self> {
        let signer = JwtSigningKey::generate(DEFAULT_JWT_KEY_ID)?;
        Self::with_jwt_signing_key(store, config, replay, Arc::new(signer))
    }

    pub fn with_jwt_signing_key(
        store: Arc<dyn TokenStore>,
        config: AuthServiceConfig,
        replay: Arc<dyn ReplayStore>,
        jwt_signer: Arc<JwtSigningKey>,
    ) -> BpResult<Self> {
        Self::validate_config(&config)?;
        Ok(Self {
            store,
            replay,
            signature_validity: config.signature_validity,
            token_lifetime: config.token_lifetime,
            rotation_overlap: config.rotation_overlap,
            jwt_signer,
        })
    }

    pub fn jwks(&self) -> &JwkSet {
        self.jwt_signer.jwks()
    }

    pub fn jwt_verifier(&self) -> JwtVerifier {
        JwtVerifier::new(self.jwks().clone())
    }

    fn validate_config(config: &AuthServiceConfig) -> BpResult<()> {
        if config.token_lifetime <= Duration::zero() {
            return Err(BpError::auth_invalid_field(
                "token_lifetime",
                "must be greater than zero",
            ));
        }
        if config.rotation_overlap < Duration::zero() {
            return Err(BpError::auth_invalid_field(
                "rotation_overlap",
                "must not be negative",
            ));
        }
        if config.rotation_overlap > config.token_lifetime {
            return Err(BpError::auth_invalid_field(
                "rotation_overlap",
                "must not exceed token_lifetime",
            ));
        }
        if config.signature_validity < Duration::seconds(1) {
            return Err(BpError::auth_invalid_field(
                "signature_validity",
                "must be at least one second",
            ));
        }
        Ok(())
    }

    pub(crate) async fn commit_mutation(
        &self,
        mutation: TokenMutation,
        event: TokenAuditEvent,
    ) -> BpResult<bool> {
        let index_mutation = mutation.clone();
        let applied = self.store.commit(mutation, event).await?;
        if applied {
            self.sync_owner_index_mutation(&index_mutation).await;
        }
        Ok(applied)
    }

    pub(crate) async fn commit_rotation(
        &self,
        transaction: &TokenRotationTransaction,
    ) -> BpResult<()> {
        self.store.commit_rotation(transaction).await
    }

    pub(crate) async fn record_audit_failure(&self, event: TokenAuditEvent) {
        if let Err(error) = self.store.append_audit(event).await {
            tracing::error!(%error, "failed to enqueue token audit event");
        }
    }

    pub(crate) fn is_conflict(error: &BpError) -> bool {
        matches!(error, BpError::Auth(AuthError::Conflict { .. }))
    }
}
