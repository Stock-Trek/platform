use crate::{ApiToken, CachedTokenStore, TokenMutation};
use tokio::time::Instant;

impl CachedTokenStore {
    pub(crate) fn record_token(&self, token: &ApiToken) {
        self.cache
            .store_token(token.id(), token.clone(), Instant::now());
    }

    pub(crate) fn record_missing(&self, id: &str) {
        self.cache.store_missing(id, Instant::now());
    }

    pub(crate) fn invalidate(&self, id: &str) {
        self.cache.invalidate(id);
    }

    pub(crate) fn record_mutation(&self, mutation: &TokenMutation) {
        match mutation {
            TokenMutation::Create(token) => self.record_token(token),
            TokenMutation::Update { expected, token } => {
                if token.id() != expected.id() {
                    self.invalidate(expected.id());
                }
                self.record_token(token);
            }
            TokenMutation::Delete { expected } => self.record_missing(expected.id()),
        }
    }

    pub(crate) fn invalidate_mutation(&self, mutation: &TokenMutation) {
        match mutation {
            TokenMutation::Create(token) => self.invalidate(token.id()),
            TokenMutation::Update { expected, token } => {
                self.invalidate(expected.id());
                if token.id() != expected.id() {
                    self.invalidate(token.id());
                }
            }
            TokenMutation::Delete { expected } => self.invalidate(expected.id()),
        }
    }
}
