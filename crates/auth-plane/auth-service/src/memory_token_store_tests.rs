use crate::{
    ApiToken, TokenAuditEvent, TokenStore, owner_index::OwnerIndex, owner_index::OwnerIndexEntry,
    token_audit_outbox::TokenAuditOutbox, token_rotation_transaction::TokenRotationTransaction,
    token_store::TokenMutation,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::{PlatformError, PlatformResult};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone, Default)]
struct MemoryState {
    tokens: HashMap<String, ApiToken>,
    owner_indexes: HashMap<String, OwnerIndex>,
    rotations: HashMap<String, TokenRotationTransaction>,
    standalone_audits: HashMap<String, TokenAuditEvent>,
}

impl MemoryState {
    fn upsert_owner_token(&mut self, token: &ApiToken) {
        self.owner_indexes
            .entry(token.owner().to_string())
            .or_insert_with(|| OwnerIndex::new(token.owner()))
            .upsert(OwnerIndexEntry::from_token(token));
    }

    fn remove_owner_token(&mut self, owner: &str, id: &str) {
        if let Some(index) = self.owner_indexes.get_mut(owner) {
            index.remove(id);
        }
    }

    fn remove_token_indexes(&mut self, token: &ApiToken) {
        self.remove_owner_token(token.owner(), token.id());
    }
}

#[derive(Clone, Default)]
pub struct MemoryTokenStore {
    state: Arc<Mutex<MemoryState>>,
}

#[async_trait]
impl TokenStore for MemoryTokenStore {
    async fn put(&self, token: &ApiToken) -> PlatformResult<()> {
        token.validate()?;
        let mut state = self.state.lock();
        if state.tokens.contains_key(token.id()) {
            return Err(PlatformError::token_exists(token.id()));
        }
        state.tokens.insert(token.id().to_string(), token.clone());
        state.upsert_owner_token(token);
        Ok(())
    }

    async fn get(&self, id: &str) -> PlatformResult<Option<ApiToken>> {
        ApiToken::validate_id(id)?;
        Ok(self.state.lock().tokens.get(id).cloned())
    }

    async fn delete(&self, id: &str) -> PlatformResult<()> {
        ApiToken::validate_id(id)?;
        let mut state = self.state.lock();
        let owner = state
            .tokens
            .remove(id)
            .map(|token| token.owner().to_string());
        if let Some(owner) = owner {
            state.remove_owner_token(&owner, id);
        }
        Ok(())
    }

    async fn delete_if(&self, expected: &ApiToken) -> PlatformResult<bool> {
        expected.validate()?;
        let mut state = self.state.lock();
        let Some(token) = state.tokens.get(expected.id()) else {
            return Ok(false);
        };
        if token.to_value() != expected.to_value() {
            return Ok(false);
        }
        state.tokens.remove(expected.id());
        state.remove_token_indexes(expected);
        Ok(true)
    }

    async fn owner_index(&self, owner: &str) -> PlatformResult<Option<OwnerIndex>> {
        Ok(self.state.lock().owner_indexes.get(owner).cloned())
    }

    async fn put_owner_index(&self, index: &OwnerIndex) -> PlatformResult<()> {
        self.state
            .lock()
            .owner_indexes
            .insert(index.owner().to_string(), index.clone());
        Ok(())
    }

    async fn owner_indexes(&self) -> PlatformResult<Vec<OwnerIndex>> {
        let state = self.state.lock();
        let mut indexes: Vec<_> = state.owner_indexes.values().cloned().collect();
        indexes.sort_by(|left, right| left.owner().cmp(right.owner()));
        Ok(indexes)
    }

    async fn index_owner_token(&self, token: &ApiToken) -> PlatformResult<()> {
        self.state.lock().upsert_owner_token(token);
        Ok(())
    }

    async fn remove_owner_index_token(&self, owner: &str, id: &str) -> PlatformResult<()> {
        self.state.lock().remove_owner_token(owner, id);
        Ok(())
    }

    async fn ids(&self) -> PlatformResult<Vec<String>> {
        let mut ids: Vec<String> = self.state.lock().tokens.keys().cloned().collect();
        ids.sort();
        Ok(ids)
    }

    async fn update(&self, expected: &ApiToken, token: &ApiToken) -> PlatformResult<()> {
        token.validate()?;
        let mut state = self.state.lock();
        let Some(stored) = state.tokens.get(token.id()) else {
            return Err(PlatformError::unknown_token(token.id()));
        };
        if stored.to_value() != expected.to_value() {
            return Err(PlatformError::token_conflict(token.id()));
        }
        state.tokens.insert(token.id().to_string(), token.clone());
        state.remove_token_indexes(expected);
        state.upsert_owner_token(token);
        Ok(())
    }

    async fn commit(
        &self,
        mutation: TokenMutation,
        event: TokenAuditEvent,
    ) -> PlatformResult<bool> {
        self.commit_events(mutation, vec![event]).await
    }

    async fn commit_events(
        &self,
        mutation: TokenMutation,
        mut events: Vec<TokenAuditEvent>,
    ) -> PlatformResult<bool> {
        for event in &mut events {
            TokenAuditOutbox::ensure_event_id(event);
        }
        let mut state = self.state.lock();
        match mutation {
            TokenMutation::Create(token) => {
                token.validate()?;
                if state.tokens.contains_key(token.id()) {
                    return Err(PlatformError::token_exists(token.id()));
                }
                state.tokens.insert(token.id().to_string(), token.clone());
                state.upsert_owner_token(&token);
                Self::store_standalone_audits(&mut state, events);
                Ok(true)
            }
            TokenMutation::Update { expected, token } => {
                token.validate()?;
                let Some(stored) = state.tokens.get(token.id()) else {
                    return Err(PlatformError::unknown_token(token.id()));
                };
                if stored.to_value() != expected.to_value() {
                    return Err(PlatformError::token_conflict(token.id()));
                }
                state.tokens.insert(token.id().to_string(), token.clone());
                state.remove_token_indexes(&expected);
                state.upsert_owner_token(&token);
                Self::store_standalone_audits(&mut state, events);
                Ok(true)
            }
            TokenMutation::Delete { expected } => {
                expected.validate()?;
                let Some(stored) = state.tokens.get(expected.id()) else {
                    return Ok(false);
                };
                if stored.to_value() != expected.to_value() {
                    return Ok(false);
                }
                state.tokens.remove(expected.id());
                state.remove_token_indexes(&expected);
                Self::store_standalone_audits(&mut state, events);
                Ok(true)
            }
        }
    }

    async fn commit_rotation(&self, transaction: &TokenRotationTransaction) -> PlatformResult<()> {
        let mut state = self.state.lock();
        if let Some(existing) = state.rotations.get(transaction.transaction_id())
            && !existing.has_same_rotation(transaction)
        {
            return Err(PlatformError::token_conflict(transaction.original().id()));
        }
        state.rotations.insert(
            transaction.transaction_id().to_string(),
            transaction.clone(),
        );
        Ok(())
    }

    async fn append_audit(&self, mut event: TokenAuditEvent) -> PlatformResult<()> {
        TokenAuditOutbox::ensure_event_id(&mut event);
        let event_id = TokenAuditOutbox::require_event_id(&event)?.to_string();
        self.state.lock().standalone_audits.insert(event_id, event);
        Ok(())
    }

    async fn pending_audits(&self) -> PlatformResult<Vec<TokenAuditEvent>> {
        let state = self.state.lock();
        let mut events: Vec<_> = state.standalone_audits.values().cloned().collect();
        events.sort_by(|left, right| {
            left.timestamp
                .cmp(&right.timestamp)
                .then_with(|| left.event_id().cmp(&right.event_id()))
        });
        Ok(events)
    }

    async fn acknowledge_audit(&self, event_id: &str) -> PlatformResult<()> {
        self.state.lock().standalone_audits.remove(event_id);
        Ok(())
    }

    async fn pending_rotations(&self) -> PlatformResult<Vec<TokenRotationTransaction>> {
        let state = self.state.lock();
        let mut transactions: Vec<_> = state.rotations.values().cloned().collect();
        transactions.sort_by(|left, right| left.transaction_id().cmp(right.transaction_id()));
        Ok(transactions)
    }

    async fn acknowledge_rotation(&self, transaction_id: &str) -> PlatformResult<()> {
        self.state.lock().rotations.remove(transaction_id);
        Ok(())
    }
}

impl MemoryTokenStore {
    fn store_standalone_audits(state: &mut MemoryState, events: Vec<TokenAuditEvent>) {
        for event in events {
            if let Some(event_id) = event.event_id().map(str::to_string) {
                state.standalone_audits.insert(event_id, event);
            }
        }
    }
}
