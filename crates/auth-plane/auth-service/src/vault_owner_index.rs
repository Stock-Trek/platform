use crate::{
    ApiToken, OwnerIndex, OwnerIndexEntry,
    auth_service::MAX_UPDATE_ATTEMPTS,
    owner_index::OWNER_INDEX_PREFIX,
    vault_client::{VaultClient, WriteCondition},
};
use platform_errors::{AuthError, BpError, BpResult};
use reqwest::{Method, StatusCode};
use serde_json::Value;

impl VaultClient {
    pub(crate) async fn owner_index(&self, owner: &str) -> BpResult<Option<OwnerIndex>> {
        let key = OwnerIndex::storage_key(owner);
        let Some((data, _)) = self.get_value_entry(&key).await? else {
            return Ok(None);
        };
        match OwnerIndex::from_value(&data) {
            Ok(index) if index.owner() == owner => Ok(Some(index)),
            Ok(_) => {
                tracing::warn!(owner, "owner index belongs to a different owner");
                Ok(None)
            }
            Err(error) => {
                tracing::warn!(%error, owner, "ignoring malformed owner index");
                Ok(None)
            }
        }
    }

    pub(crate) async fn put_owner_index(&self, index: &OwnerIndex) -> BpResult<()> {
        let key = OwnerIndex::storage_key(index.owner());
        let value = index.to_value();
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            match self.get_value_entry(&key).await? {
                None => match self
                    .put_entry(&key, value.clone(), WriteCondition::Create)
                    .await
                {
                    Ok(()) => return Ok(()),
                    Err(BpError::Auth(AuthError::TokenExists { .. })) => continue,
                    Err(error) => return Err(error),
                },
                Some((_, version)) => match self
                    .put_entry(&key, value.clone(), WriteCondition::Update(version))
                    .await
                {
                    Ok(()) => return Ok(()),
                    Err(BpError::Auth(AuthError::Conflict { .. })) => continue,
                    Err(error) => return Err(error),
                },
            }
        }
        Err(Self::owner_index_conflict(&key))
    }

    pub(crate) async fn upsert_owner_index_token(&self, token: &ApiToken) -> BpResult<()> {
        let owner = token.owner();
        let key = OwnerIndex::storage_key(owner);
        let entry = OwnerIndexEntry::from_token(token);
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            match self.get_value_entry(&key).await? {
                None => {
                    let mut index = OwnerIndex::new(owner);
                    index.upsert(entry.clone());
                    match self
                        .put_entry(&key, index.to_value(), WriteCondition::Create)
                        .await
                    {
                        Ok(()) => return Ok(()),
                        Err(BpError::Auth(AuthError::TokenExists { .. })) => continue,
                        Err(error) => return Err(error),
                    }
                }
                Some((data, version)) => {
                    let mut index = self.owner_index_from_document(owner, &data)?;
                    index.upsert(entry.clone());
                    match self
                        .put_entry(&key, index.to_value(), WriteCondition::Update(version))
                        .await
                    {
                        Ok(()) => return Ok(()),
                        Err(BpError::Auth(AuthError::Conflict { .. })) => continue,
                        Err(error) => return Err(error),
                    }
                }
            }
        }
        Err(Self::owner_index_conflict(&key))
    }

    pub(crate) async fn remove_owner_index_token(&self, owner: &str, id: &str) -> BpResult<()> {
        let key = OwnerIndex::storage_key(owner);
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            let Some((data, version)) = self.get_value_entry(&key).await? else {
                return Ok(());
            };
            let mut index = self.owner_index_from_document(owner, &data)?;
            if !index.tokens().iter().any(|entry| entry.id() == id) {
                return Ok(());
            }
            index.remove(id);
            match self
                .put_entry(&key, index.to_value(), WriteCondition::Update(version))
                .await
            {
                Ok(()) => return Ok(()),
                Err(BpError::Auth(AuthError::Conflict { .. })) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Self::owner_index_conflict(&key))
    }

    pub(crate) async fn owner_indexes(&self) -> BpResult<Vec<OwnerIndex>> {
        let mut indexes = Vec::new();
        for key in self.owner_index_keys().await? {
            let Some((data, _)) = self.get_value_entry(&key).await? else {
                continue;
            };
            match OwnerIndex::from_value(&data) {
                Ok(index) => indexes.push(index),
                Err(error) => {
                    tracing::warn!(%error, key, "ignoring malformed owner index");
                }
            }
        }
        indexes.sort_by(|left, right| left.owner().cmp(right.owner()));
        Ok(indexes)
    }

    async fn owner_index_keys(&self) -> BpResult<Vec<String>> {
        let prefix = OWNER_INDEX_PREFIX.trim_end_matches('/');
        let mut url = self.url_for_path("metadata", prefix)?;
        url.query_pairs_mut().append_pair("list", "true");
        let response = self
            .send("list", prefix, self.request(Method::GET, url))
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(Vec::new());
        }
        let response = self.require_success("list", prefix, response).await?;
        let value = self.parse_json("list", prefix, response).await?;
        let keys = value
            .pointer("/data/keys")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| {
                BpError::auth_store(
                    "list",
                    prefix,
                    false,
                    "malformed Vault response".to_string(),
                )
            })?;
        let mut keys: Vec<String> = keys
            .iter()
            .filter_map(Value::as_str)
            .filter(|key| !key.ends_with('/'))
            .map(|key| {
                if key.starts_with(OWNER_INDEX_PREFIX) {
                    key.to_string()
                } else {
                    format!("{OWNER_INDEX_PREFIX}{key}")
                }
            })
            .collect();
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    fn owner_index_from_document(&self, owner: &str, data: &Value) -> BpResult<OwnerIndex> {
        let index = OwnerIndex::from_value(data)?;
        if index.owner() != owner {
            return Err(BpError::auth_store(
                "read",
                OwnerIndex::storage_key(owner),
                false,
                "owner index hash collision".to_string(),
            ));
        }
        Ok(index)
    }

    fn owner_index_conflict(key: &str) -> BpError {
        BpError::auth_store(
            "write",
            key,
            true,
            "owner index update conflicted too many times".to_string(),
        )
    }
}
