use crate::{ApiToken, token_store::TokenStore};
use async_trait::async_trait;
use platform_errors::{BpError, BpResult};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Clone, Default)]
pub struct MemoryTokenStore {
    tokens: Arc<Mutex<HashMap<String, ApiToken>>>,
}

impl MemoryTokenStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl TokenStore for MemoryTokenStore {
    async fn put(&self, token: &ApiToken) -> BpResult<()> {
        ApiToken::validate_id(token.id())?;
        let mut tokens = self.tokens.lock().expect("poisoned token mutex");
        if tokens.contains_key(token.id()) {
            return Err(BpError::token_exists(token.id()));
        }
        tokens.insert(token.id().to_string(), token.clone());
        Ok(())
    }

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        ApiToken::validate_id(id)?;
        let tokens = self.tokens.lock().expect("poisoned token mutex");
        Ok(tokens.get(id).cloned())
    }

    async fn delete(&self, id: &str) -> BpResult<()> {
        ApiToken::validate_id(id)?;
        let mut tokens = self.tokens.lock().expect("poisoned token mutex");
        tokens.remove(id);
        Ok(())
    }

    async fn ids(&self) -> BpResult<Vec<String>> {
        let tokens = self.tokens.lock().expect("poisoned token mutex");
        let mut ids: Vec<String> = tokens.keys().cloned().collect();
        ids.sort();
        Ok(ids)
    }
}
