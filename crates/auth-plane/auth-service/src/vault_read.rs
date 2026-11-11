use crate::{
    ApiToken,
    token_audit_outbox::TokenAuditOutbox,
    token_rotation_transaction::TokenRotationTransaction,
    vault_client::{TOMBSTONE_FIELD, VaultClient},
};
use platform_errors::{BpError, BpResult};
use reqwest::{Method, StatusCode};
use serde_json::Value;

impl VaultClient {
    pub(crate) async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        Ok(self.get_entry(id).await?.map(|(token, _)| token))
    }

    pub(crate) async fn get_entry(&self, id: &str) -> BpResult<Option<(ApiToken, u64)>> {
        ApiToken::validate_id(id)?;
        let Some((data, version)) = self.get_value_entry(id).await? else {
            return Ok(None);
        };
        if Self::is_tombstone(&data) {
            return Ok(None);
        }
        let token = Self::token_from_value(id, &data)?;
        Ok(Some((token, version)))
    }

    pub(crate) async fn get_value_entry(&self, path: &str) -> BpResult<Option<(Value, u64)>> {
        let response = self
            .send(
                "read",
                path,
                self.request(Method::GET, self.url_for_path("data", path)?),
            )
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = self.require_success("read", path, response).await?;
        let value = self.parse_json("read", path, response).await?;
        let data = value.pointer("/data/data").cloned().ok_or_else(|| {
            BpError::auth_store("read", path, false, "malformed Vault response".to_string())
        })?;
        let version = value
            .pointer("/data/metadata/version")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                BpError::auth_store("read", path, false, "missing Vault version".to_string())
            })?;
        Ok(Some((data, version)))
    }

    pub(crate) fn token_from_value(id: &str, data: &Value) -> BpResult<ApiToken> {
        ApiToken::from_value(id, data)
            .map_err(|error| BpError::auth_store("read", id, false, error))
    }

    pub(crate) fn is_tombstone(value: &Value) -> bool {
        value
            .get(TOMBSTONE_FIELD)
            .and_then(Value::as_bool)
            .is_some_and(|deleted| deleted)
    }

    pub(crate) async fn keys(&self) -> BpResult<Vec<String>> {
        let mut keys = self.list_directory("").await?;
        keys.retain(|key| {
            !key.ends_with('/')
                && (ApiToken::validate_id(key).is_ok()
                    || TokenRotationTransaction::is_storage_key(key)
                    || TokenAuditOutbox::is_storage_key(key))
        });
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    pub(crate) async fn keys_with_prefix(&self, prefix: &str) -> BpResult<Vec<String>> {
        let directory = prefix
            .rsplit_once('/')
            .map_or("", |(directory, _)| directory);
        let mut keys: Vec<String> = self
            .list_directory(directory)
            .await?
            .into_iter()
            .filter(|key| key.starts_with(prefix))
            .collect();
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    async fn list_directory(&self, directory: &str) -> BpResult<Vec<String>> {
        let mut url = self.url_for_path("metadata", directory)?;
        url.query_pairs_mut().append_pair("list", "true");
        let response = self
            .send("list", directory, self.request(Method::GET, url))
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(Vec::new());
        }
        let response = self.require_success("list", directory, response).await?;
        let value = self.parse_json("list", directory, response).await?;
        let keys = value
            .pointer("/data/keys")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| {
                BpError::auth_store(
                    "list",
                    directory,
                    false,
                    "malformed Vault response".to_string(),
                )
            })?;
        Ok(keys
            .iter()
            .filter_map(|key| key.as_str().map(str::to_string))
            .map(|key| {
                if directory.is_empty() {
                    key
                } else {
                    format!("{directory}/{key}")
                }
            })
            .collect())
    }

    pub(crate) async fn ids(&self) -> BpResult<Vec<String>> {
        Ok(self
            .keys()
            .await?
            .into_iter()
            .filter(|key| {
                !TokenRotationTransaction::is_storage_key(key)
                    && !TokenAuditOutbox::is_storage_key(key)
            })
            .collect())
    }
}
