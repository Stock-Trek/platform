use crate::{TokenAuditLog, replay_cache::ReplayCache, token_store::TokenStore};
use chrono::Duration;
use platform_errors::{BpError, BpResult};
use std::sync::Arc;

pub struct AuthServiceConfig {
    pub token_lifetime: Duration,
    pub rotation_overlap: Duration,
    pub signature_validity: Duration,
    pub audit: Arc<dyn TokenAuditLog>,
}

#[derive(Clone)]
pub struct AuthService {
    pub(crate) store: Arc<dyn TokenStore>,
    pub(crate) replay: Arc<ReplayCache>,
    pub(crate) audit: Arc<dyn TokenAuditLog>,
    pub(crate) signature_validity: Duration,
    pub(crate) token_lifetime: Duration,
    pub(crate) rotation_overlap: Duration,
}

impl AuthService {
    pub fn new(store: Arc<dyn TokenStore>, config: AuthServiceConfig) -> BpResult<Self> {
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
        Ok(Self {
            store,
            replay: Arc::new(ReplayCache::new()),
            audit: config.audit,
            signature_validity: config.signature_validity,
            token_lifetime: config.token_lifetime,
            rotation_overlap: config.rotation_overlap,
        })
    }
}
