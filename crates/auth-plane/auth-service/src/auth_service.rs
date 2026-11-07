use crate::{
    ApiToken, MemoryTokenAuditLog, NoopTokenAuditLog, TokenAuditEvent, TokenAuditLog,
    TokenDuration, TokenExpiry, replay_cache::ReplayCache, token_store::TokenStore,
};
use chrono::{DateTime, Duration, Utc};
use platform_errors::BpResult;
use std::sync::Arc;

pub const DEFAULT_TOKEN_LIFETIME_SECONDS: i64 = 30 * 24 * 60 * 60;
pub const DEFAULT_ROTATION_OVERLAP_SECONDS: i64 = 7 * 24 * 60 * 60;
const DEFAULT_SIGNATURE_VALIDITY_SECONDS: i64 = 300;

#[derive(Clone)]
pub struct AuthService {
    pub(crate) store: Arc<dyn TokenStore>,
    pub(crate) replay: Arc<ReplayCache>,
    pub(crate) audit: Arc<dyn TokenAuditLog>,
    pub(crate) memory_audit: Option<Arc<MemoryTokenAuditLog>>,
    pub(crate) signature_validity: Duration,
    pub(crate) default_token_lifetime: Option<Duration>,
    pub(crate) rotation_overlap: Duration,
}

impl AuthService {
    pub fn new(store: Arc<dyn TokenStore>) -> Self {
        let audit = Arc::new(MemoryTokenAuditLog::new());
        Self {
            store,
            replay: Arc::new(ReplayCache::new()),
            audit: audit.clone(),
            memory_audit: Some(audit),
            signature_validity: Duration::seconds(DEFAULT_SIGNATURE_VALIDITY_SECONDS),
            default_token_lifetime: Some(Duration::seconds(DEFAULT_TOKEN_LIFETIME_SECONDS)),
            rotation_overlap: Duration::seconds(DEFAULT_ROTATION_OVERLAP_SECONDS),
        }
    }

    pub fn with_audit_log(mut self, audit: Arc<dyn TokenAuditLog>) -> Self {
        self.audit = audit;
        self.memory_audit = None;
        self
    }

    pub fn with_noop_audit_log(mut self) -> Self {
        self.audit = Arc::new(NoopTokenAuditLog);
        self.memory_audit = None;
        self
    }

    pub fn with_signature_validity(mut self, validity: std::time::Duration) -> Self {
        if let Ok(validity) = Duration::from_std(validity) {
            self.signature_validity = validity;
        }
        self
    }

    pub fn with_token_lifetime<D: TokenDuration>(mut self, lifetime: D) -> Self {
        if let Some(lifetime) = lifetime.as_chrono_duration() {
            self.default_token_lifetime = Some(lifetime.max(Duration::zero()));
        }
        self
    }

    pub fn with_default_token_lifetime<D: TokenDuration>(self, lifetime: D) -> Self {
        self.with_token_lifetime(lifetime)
    }

    pub fn without_token_expiry(mut self) -> Self {
        self.default_token_lifetime = None;
        self
    }

    pub fn with_rotation_overlap<D: TokenDuration>(mut self, overlap: D) -> Self {
        if let Some(overlap) = overlap.as_chrono_duration() {
            self.rotation_overlap = overlap.max(Duration::zero());
        }
        self
    }

    pub fn audit_events(&self) -> Vec<TokenAuditEvent> {
        self.memory_audit
            .as_ref()
            .map(|audit| audit.events())
            .unwrap_or_default()
    }

    pub fn drain_audit_events(&self) -> Vec<TokenAuditEvent> {
        self.memory_audit
            .as_ref()
            .map(|audit| audit.drain())
            .unwrap_or_default()
    }

    pub fn audit_log(&self) -> Arc<dyn TokenAuditLog> {
        Arc::clone(&self.audit)
    }

    pub(crate) fn record(&self, event: TokenAuditEvent) {
        self.audit.record(event);
    }

    pub(crate) fn resolve_expiry(
        &self,
        expiry: TokenExpiry,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        match expiry {
            TokenExpiry::Default => self.default_token_lifetime.map(|lifetime| now + lifetime),
            TokenExpiry::Never => None,
            TokenExpiry::At(expires_at) => Some(expires_at),
            TokenExpiry::After(lifetime) => Some(now + lifetime.max(Duration::zero())),
        }
    }

    pub(crate) async fn touch(&self, token: &ApiToken, now: DateTime<Utc>) -> BpResult<ApiToken> {
        let mut token = token.clone();
        token.record_use(now);
        self.store.update(&token).await?;
        Ok(token)
    }
}
