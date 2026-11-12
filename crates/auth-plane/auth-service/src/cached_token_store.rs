use crate::{
    ApiToken, OwnerIndex, TokenAuditEvent, TokenCacheConfig, TokenMutation,
    TokenRotationTransaction, TokenStore,
    circuit_breaker::{BreakerState, CircuitBreaker},
    flight_gates::FlightGates,
    token_cache::TokenCache,
};
use async_trait::async_trait;
use platform_errors::BpResult;
use std::sync::Arc;

pub struct CachedTokenStore {
    pub(crate) inner: Arc<dyn TokenStore>,
    pub(crate) cache: TokenCache,
    pub(crate) breaker: CircuitBreaker,
    pub(crate) gates: FlightGates,
}

impl CachedTokenStore {
    pub fn new(inner: Arc<dyn TokenStore>, config: TokenCacheConfig) -> Self {
        Self {
            inner,
            cache: TokenCache::new(&config),
            breaker: CircuitBreaker::new(config.failure_threshold(), config.open_duration()),
            gates: FlightGates::new(),
        }
    }

    pub fn breaker_state(&self) -> BreakerState {
        self.breaker.state()
    }
}

#[async_trait]
impl TokenStore for CachedTokenStore {
    async fn put(&self, token: &ApiToken) -> BpResult<()> {
        match self.inner.put(token).await {
            Ok(()) => {
                self.record_token(token);
                Ok(())
            }
            Err(error) => {
                self.invalidate(token.id());
                Err(error)
            }
        }
    }

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        self.cached_get(id).await
    }

    async fn delete(&self, id: &str) -> BpResult<()> {
        match self.inner.delete(id).await {
            Ok(()) => {
                self.record_missing(id);
                Ok(())
            }
            Err(error) => {
                self.invalidate(id);
                Err(error)
            }
        }
    }

    async fn delete_if(&self, expected: &ApiToken) -> BpResult<bool> {
        match self.inner.delete_if(expected).await {
            Ok(true) => {
                self.record_missing(expected.id());
                Ok(true)
            }
            Ok(false) => {
                self.invalidate(expected.id());
                Ok(false)
            }
            Err(error) => {
                self.invalidate(expected.id());
                Err(error)
            }
        }
    }

    async fn ids(&self) -> BpResult<Vec<String>> {
        self.inner.ids().await
    }

    async fn owner_index(&self, owner: &str) -> BpResult<Option<OwnerIndex>> {
        self.inner.owner_index(owner).await
    }

    async fn put_owner_index(&self, index: &OwnerIndex) -> BpResult<()> {
        self.inner.put_owner_index(index).await
    }

    async fn owner_indexes(&self) -> BpResult<Vec<OwnerIndex>> {
        self.inner.owner_indexes().await
    }

    async fn index_owner_token(&self, token: &ApiToken) -> BpResult<()> {
        self.inner.index_owner_token(token).await
    }

    async fn remove_owner_index_token(&self, owner: &str, id: &str) -> BpResult<()> {
        self.inner.remove_owner_index_token(owner, id).await
    }

    async fn update(&self, expected: &ApiToken, token: &ApiToken) -> BpResult<()> {
        match self.inner.update(expected, token).await {
            Ok(()) => {
                self.record_token(token);
                Ok(())
            }
            Err(error) => {
                self.invalidate(expected.id());
                if token.id() != expected.id() {
                    self.invalidate(token.id());
                }
                Err(error)
            }
        }
    }

    async fn commit(&self, mutation: TokenMutation, event: TokenAuditEvent) -> BpResult<bool> {
        let cached_mutation = mutation.clone();
        match self.inner.commit(mutation, event).await {
            Ok(true) => {
                self.record_mutation(&cached_mutation);
                Ok(true)
            }
            Ok(false) => {
                self.invalidate_mutation(&cached_mutation);
                Ok(false)
            }
            Err(error) => {
                self.invalidate_mutation(&cached_mutation);
                Err(error)
            }
        }
    }

    async fn commit_events(
        &self,
        mutation: TokenMutation,
        events: Vec<TokenAuditEvent>,
    ) -> BpResult<bool> {
        let cached_mutation = mutation.clone();
        match self.inner.commit_events(mutation, events).await {
            Ok(true) => {
                self.record_mutation(&cached_mutation);
                Ok(true)
            }
            Ok(false) => {
                self.invalidate_mutation(&cached_mutation);
                Ok(false)
            }
            Err(error) => {
                self.invalidate_mutation(&cached_mutation);
                Err(error)
            }
        }
    }

    async fn append_audit(&self, event: TokenAuditEvent) -> BpResult<()> {
        self.inner.append_audit(event).await
    }

    async fn commit_rotation(&self, transaction: &TokenRotationTransaction) -> BpResult<()> {
        self.inner.commit_rotation(transaction).await
    }

    async fn materialize_rotation(&self, transaction: &TokenRotationTransaction) -> BpResult<()> {
        let result = self.inner.materialize_rotation(transaction).await;
        self.invalidate(transaction.original().id());
        self.invalidate(transaction.replacement().id());
        result
    }

    async fn pending_audits(&self) -> BpResult<Vec<TokenAuditEvent>> {
        self.inner.pending_audits().await
    }

    async fn acknowledge_audit(&self, event_id: &str) -> BpResult<()> {
        self.inner.acknowledge_audit(event_id).await
    }

    async fn pending_rotations(&self) -> BpResult<Vec<TokenRotationTransaction>> {
        self.inner.pending_rotations().await
    }

    async fn acknowledge_rotation(&self, transaction_id: &str) -> BpResult<()> {
        self.inner.acknowledge_rotation(transaction_id).await
    }
}
