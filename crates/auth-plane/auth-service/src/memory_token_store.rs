use crate::{ApiToken, token_store::TokenStore};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::{BpError, BpResult};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone, Default)]
pub struct MemoryTokenStore {
    tokens: Arc<Mutex<HashMap<String, ApiToken>>>,
}

#[async_trait]
impl TokenStore for MemoryTokenStore {
    async fn put(&self, token: &ApiToken) -> BpResult<()> {
        token.validate()?;
        let mut tokens = self.tokens.lock();
        if tokens.contains_key(token.id()) {
            return Err(BpError::token_exists(token.id()));
        }
        tokens.insert(token.id().to_string(), token.clone());
        Ok(())
    }

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        ApiToken::validate_id(id)?;
        let tokens = self.tokens.lock();
        Ok(tokens.get(id).cloned())
    }

    async fn delete(&self, id: &str) -> BpResult<()> {
        ApiToken::validate_id(id)?;
        let mut tokens = self.tokens.lock();
        tokens.remove(id);
        Ok(())
    }

    async fn ids(&self) -> BpResult<Vec<String>> {
        let tokens = self.tokens.lock();
        let mut ids: Vec<String> = tokens.keys().cloned().collect();
        ids.sort();
        Ok(ids)
    }

    async fn update(&self, token: &ApiToken) -> BpResult<()> {
        token.validate()?;
        let mut tokens = self.tokens.lock();
        let Some(stored) = tokens.get_mut(token.id()) else {
            return Err(BpError::unknown_token(token.id()));
        };
        *stored = token.clone();
        Ok(())
    }
}
