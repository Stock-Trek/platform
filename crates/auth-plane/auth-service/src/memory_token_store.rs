use crate::{
    ApiToken, OwnerIndex, OwnerIndexEntry, TokenAuditEvent, TokenMutation,
    TokenRotationTransaction, TokenStore, token_audit_outbox::TokenAuditOutbox,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::{BpError, BpResult};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
struct StoredEntry {
    token: Option<ApiToken>,
    outbox: Vec<TokenAuditEvent>,
}

#[derive(Clone, Default)]
struct MemoryState {
    tokens: HashMap<String, StoredEntry>,
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
    async fn put(&self, token: &ApiToken) -> BpResult<()> {
        token.validate()?;
        let mut state = self.state.lock();
        if state.tokens.contains_key(token.id()) {
            return Err(BpError::token_exists(token.id()));
        }
        state.tokens.insert(
            token.id().to_string(),
            StoredEntry {
                token: Some(token.clone()),
                outbox: Vec::new(),
            },
        );
        state.upsert_owner_token(token);
        Ok(())
    }

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        ApiToken::validate_id(id)?;
        let state = self.state.lock();
        Ok(state.tokens.get(id).and_then(|entry| entry.token.clone()))
    }

    async fn delete(&self, id: &str) -> BpResult<()> {
        ApiToken::validate_id(id)?;
        let mut state = self.state.lock();
        let owner = state
            .tokens
            .remove(id)
            .and_then(|entry| entry.token)
            .map(|token| token.owner().to_string());
        if let Some(owner) = owner {
            state.remove_owner_token(&owner, id);
        }
        Ok(())
    }

    async fn delete_if(&self, expected: &ApiToken) -> BpResult<bool> {
        expected.validate()?;
        let mut state = self.state.lock();
        let Some(entry) = state.tokens.get_mut(expected.id()) else {
            return Ok(false);
        };
        let Some(stored) = &entry.token else {
            return Ok(false);
        };
        if stored.to_value() != expected.to_value() {
            return Ok(false);
        }
        entry.token = None;
        if entry.outbox.is_empty() {
            state.tokens.remove(expected.id());
        }
        state.remove_token_indexes(expected);
        Ok(true)
    }

    async fn owner_index(&self, owner: &str) -> BpResult<Option<OwnerIndex>> {
        Ok(self.state.lock().owner_indexes.get(owner).cloned())
    }

    async fn put_owner_index(&self, index: &OwnerIndex) -> BpResult<()> {
        self.state
            .lock()
            .owner_indexes
            .insert(index.owner().to_string(), index.clone());
        Ok(())
    }

    async fn owner_indexes(&self) -> BpResult<Vec<OwnerIndex>> {
        let state = self.state.lock();
        let mut indexes: Vec<_> = state.owner_indexes.values().cloned().collect();
        indexes.sort_by(|left, right| left.owner().cmp(right.owner()));
        Ok(indexes)
    }

    async fn index_owner_token(&self, token: &ApiToken) -> BpResult<()> {
        self.state.lock().upsert_owner_token(token);
        Ok(())
    }

    async fn remove_owner_index_token(&self, owner: &str, id: &str) -> BpResult<()> {
        self.state.lock().remove_owner_token(owner, id);
        Ok(())
    }

    async fn ids(&self) -> BpResult<Vec<String>> {
        let state = self.state.lock();
        let mut ids: Vec<String> = state
            .tokens
            .iter()
            .filter(|(_, entry)| entry.token.is_some())
            .map(|(id, _)| id.clone())
            .collect();
        ids.sort();
        Ok(ids)
    }

    async fn update(&self, expected: &ApiToken, token: &ApiToken) -> BpResult<()> {
        token.validate()?;
        let mut state = self.state.lock();
        let Some(entry) = state.tokens.get_mut(token.id()) else {
            return Err(BpError::unknown_token(token.id()));
        };
        let Some(stored) = &entry.token else {
            return Err(BpError::unknown_token(token.id()));
        };
        if stored.to_value() != expected.to_value() {
            return Err(BpError::token_conflict(token.id()));
        }
        entry.token = Some(token.clone());
        state.remove_token_indexes(expected);
        state.upsert_owner_token(token);
        Ok(())
    }

    async fn commit(&self, mutation: TokenMutation, event: TokenAuditEvent) -> BpResult<bool> {
        self.commit_events(mutation, vec![event]).await
    }

    async fn commit_events(
        &self,
        mutation: TokenMutation,
        mut events: Vec<TokenAuditEvent>,
    ) -> BpResult<bool> {
        for event in &mut events {
            TokenAuditOutbox::ensure_event_id(event);
        }
        let mut state = self.state.lock();
        match mutation {
            TokenMutation::Create(token) => {
                token.validate()?;
                if state.tokens.contains_key(token.id()) {
                    return Err(BpError::token_exists(token.id()));
                }
                state.tokens.insert(
                    token.id().to_string(),
                    StoredEntry {
                        token: Some(token.clone()),
                        outbox: events,
                    },
                );
                state.upsert_owner_token(&token);
                Ok(true)
            }
            TokenMutation::Update { expected, token } => {
                token.validate()?;
                let Some(entry) = state.tokens.get_mut(token.id()) else {
                    return Err(BpError::unknown_token(token.id()));
                };
                let Some(stored) = &entry.token else {
                    return Err(BpError::unknown_token(token.id()));
                };
                if stored.to_value() != expected.to_value() {
                    return Err(BpError::token_conflict(token.id()));
                }
                entry.token = Some(token.clone());
                entry.outbox.extend(events);
                state.remove_token_indexes(&expected);
                state.upsert_owner_token(&token);
                Ok(true)
            }
            TokenMutation::Delete { expected } => {
                expected.validate()?;
                let Some(entry) = state.tokens.get_mut(expected.id()) else {
                    return Ok(false);
                };
                let Some(stored) = &entry.token else {
                    return Ok(false);
                };
                if stored.to_value() != expected.to_value() {
                    return Ok(false);
                }
                entry.token = None;
                entry.outbox.extend(events);
                state.remove_token_indexes(&expected);
                Ok(true)
            }
        }
    }

    async fn commit_rotation(&self, transaction: &TokenRotationTransaction) -> BpResult<()> {
        let mut state = self.state.lock();
        if let Some(existing) = state.rotations.get(transaction.transaction_id())
            && !existing.has_same_rotation(transaction)
        {
            return Err(BpError::token_conflict(transaction.original().id()));
        }
        state.rotations.insert(
            transaction.transaction_id().to_string(),
            transaction.clone(),
        );
        Ok(())
    }

    async fn append_audit(&self, mut event: TokenAuditEvent) -> BpResult<()> {
        TokenAuditOutbox::ensure_event_id(&mut event);
        let mut state = self.state.lock();
        if let Some(token_id) = event.token_id.as_deref()
            && let Some(entry) = state.tokens.get_mut(token_id)
            && entry.token.is_some()
        {
            entry.outbox.push(event);
            return Ok(());
        }
        let event_id = TokenAuditOutbox::require_event_id(&event)?.to_string();
        state.standalone_audits.insert(event_id, event);
        Ok(())
    }

    async fn pending_audits(&self) -> BpResult<Vec<TokenAuditEvent>> {
        let state = self.state.lock();
        let mut events = Vec::new();
        for entry in state.tokens.values() {
            events.extend(entry.outbox.iter().cloned());
        }
        events.extend(state.standalone_audits.values().cloned());
        Ok(events)
    }

    async fn acknowledge_audit(&self, event_id: &str) -> BpResult<()> {
        let mut state = self.state.lock();
        state.standalone_audits.remove(event_id);
        let mut remove = Vec::new();
        for (id, entry) in state.tokens.iter_mut() {
            TokenAuditOutbox::remove_events(&mut entry.outbox, event_id);
            if entry.token.is_none() && entry.outbox.is_empty() {
                remove.push(id.clone());
            }
        }
        for id in remove {
            state.tokens.remove(&id);
        }
        Ok(())
    }

    async fn pending_rotations(&self) -> BpResult<Vec<TokenRotationTransaction>> {
        let state = self.state.lock();
        let mut transactions: Vec<_> = state.rotations.values().cloned().collect();
        transactions.sort_by(|left, right| left.transaction_id().cmp(right.transaction_id()));
        Ok(transactions)
    }

    async fn acknowledge_rotation(&self, transaction_id: &str) -> BpResult<()> {
        self.state.lock().rotations.remove(transaction_id);
        Ok(())
    }
}
