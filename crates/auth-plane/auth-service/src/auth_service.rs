use crate::{
    MemoryReplayStore, ReplayStore, TokenAuditEvent, TokenAuditLog,
    memory_replay_store::DEFAULT_REPLAY_CAPACITY, token_store::TokenStore,
};
use chrono::Duration;
use platform_errors::{AuthError, BpError, BpResult};
use std::{num::NonZeroUsize, sync::Arc};

pub(crate) const MAX_UPDATE_ATTEMPTS: usize = 5;

pub struct AuthServiceConfig {
    pub token_lifetime: Duration,
    pub rotation_overlap: Duration,
    pub signature_validity: Duration,
    pub audit: Arc<dyn TokenAuditLog>,
}

#[derive(Clone)]
pub struct AuthService {
    pub(crate) store: Arc<dyn TokenStore>,
    pub(crate) replay: Arc<dyn ReplayStore>,
    pub(crate) audit: Arc<dyn TokenAuditLog>,
    pub(crate) signature_validity: Duration,
    pub(crate) token_lifetime: Duration,
    pub(crate) rotation_overlap: Duration,
}

impl AuthService {
    pub fn new(store: Arc<dyn TokenStore>, config: AuthServiceConfig) -> BpResult<Self> {
        let replay = MemoryReplayStore::new(
            NonZeroUsize::new(DEFAULT_REPLAY_CAPACITY).expect("non-zero default capacity"),
        );
        Self::new_with_replay_store(store, config, Arc::new(replay))
    }

    pub fn new_with_replay_store(
        store: Arc<dyn TokenStore>,
        config: AuthServiceConfig,
        replay: Arc<dyn ReplayStore>,
    ) -> BpResult<Self> {
        Self::validate_config(&config)?;
        Ok(Self {
            store,
            replay,
            audit: config.audit,
            signature_validity: config.signature_validity,
            token_lifetime: config.token_lifetime,
            rotation_overlap: config.rotation_overlap,
        })
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
        if config.signature_validity < Duration::seconds(1) {
            return Err(BpError::auth_invalid_field(
                "signature_validity",
                "must be at least one second",
            ));
        }
        Ok(())
    }

    pub(crate) async fn record_audit(&self, event: TokenAuditEvent) -> BpResult<()> {
        self.audit.record(event).await
    }

    /// Records an event for an operation that is already failing. The original
    /// failure is more useful to the caller, so audit delivery errors are logged
    /// instead of replacing it.
    pub(crate) async fn record_audit_failure(&self, event: TokenAuditEvent) {
        if let Err(error) = self.audit.record(event).await {
            tracing::error!(%error, "failed to record token audit event");
        }
    }

    pub(crate) fn is_conflict(error: &BpError) -> bool {
        matches!(error, BpError::Auth(AuthError::Conflict { .. }))
    }

    pub(crate) fn is_duplicate(error: &BpError) -> bool {
        matches!(error, BpError::Auth(AuthError::TokenExists { .. }))
    }
}
