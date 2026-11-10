use crate::{
    ApiToken, TokenAuditEvent, TokenMutation, TokenRotationTransaction, TokenStore,
    token_audit_outbox::TokenAuditOutbox,
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
    rotations: HashMap<String, TokenRotationTransaction>,
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
        Ok(())
    }

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        ApiToken::validate_id(id)?;
        let state = self.state.lock();
        Ok(state.tokens.get(id).and_then(|entry| entry.token.clone()))
    }

    async fn delete(&self, id: &str) -> BpResult<()> {
        ApiToken::validate_id(id)?;
        self.state.lock().tokens.remove(id);
        Ok(())
    }

    async fn delete_if(&self, expected: &ApiToken) -> BpResult<bool> {
        expected.validate()?;
        let mut state = self.state.lock();
        let Some(entry) = state.tokens.get(expected.id()) else {
            return Ok(false);
        };
        let Some(stored) = &entry.token else {
            return Ok(false);
        };
        if stored.to_value() != expected.to_value() {
            return Ok(false);
        }
        state.tokens.remove(expected.id());
        Ok(true)
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
        Ok(())
    }

    async fn commit(&self, mutation: TokenMutation, mut event: TokenAuditEvent) -> BpResult<bool> {
        TokenAuditOutbox::ensure_event_id(&mut event);
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
                        token: Some(token),
                        outbox: vec![event],
                    },
                );
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
                entry.token = Some(token);
                entry.outbox.push(event);
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
                entry.outbox.push(event);
                Ok(true)
            }
        }
    }

    async fn commit_rotation(&self, transaction: &TokenRotationTransaction) -> BpResult<()> {
        let mut state = self.state.lock();
        if let Some(existing) = state.rotations.get(transaction.transaction_id()) {
            return if existing.to_value() == transaction.to_value() {
                Ok(())
            } else {
                Err(BpError::token_conflict(transaction.original().id()))
            };
        }
        state.rotations.insert(
            transaction.transaction_id().to_string(),
            transaction.clone(),
        );
        Ok(())
    }

    async fn pending_audits(&self) -> BpResult<Vec<TokenAuditEvent>> {
        let state = self.state.lock();
        let mut events = Vec::new();
        for entry in state.tokens.values() {
            events.extend(entry.outbox.iter().cloned());
        }
        Ok(events)
    }

    async fn acknowledge_audit(&self, event_id: &str) -> BpResult<()> {
        let mut state = self.state.lock();
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
