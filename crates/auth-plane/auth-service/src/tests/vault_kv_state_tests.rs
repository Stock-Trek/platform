use serde_json::{Value, json};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub(crate) struct CapturedRequest {
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) query: Option<String>,
    pub(crate) token: Option<String>,
    pub(crate) body: Value,
}

#[derive(Default)]
pub(crate) struct VaultKvState {
    entries: HashMap<String, Vec<Value>>,
}

impl VaultKvState {
    pub(crate) fn put_response(&mut self, id: &str, body: &Value) -> (u16, Value) {
        let Some(data) = body.get("data").cloned() else {
            return (400, json!({ "errors": ["missing data"] }));
        };
        let cas = body
            .pointer("/options/cas")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        match self.write(id, data, cas) {
            Ok(version) => (
                200,
                json!({
                    "data": {
                        "created_time": "",
                        "custom_metadata": null,
                        "deletion_time": "",
                        "destroyed": false,
                        "version": version
                    }
                }),
            ),
            Err(error) => (400, error),
        }
    }

    pub(crate) fn get_response(&self, id: &str) -> (u16, Value) {
        let Some((data, version)) = self.read(id) else {
            return (404, json!({ "errors": [] }));
        };
        (
            200,
            json!({
                "data": {
                    "data": data,
                    "metadata": {
                        "created_time": "",
                        "custom_metadata": null,
                        "deletion_time": "",
                        "destroyed": false,
                        "version": version
                    }
                }
            }),
        )
    }

    pub(crate) fn list_response(&self) -> (u16, Value) {
        match self.list() {
            Some(keys) => (200, json!({ "data": { "keys": keys } })),
            None => (404, json!({ "errors": [] })),
        }
    }

    pub(crate) fn delete(&mut self, id: &str) {
        self.entries.remove(id);
    }

    pub(crate) fn seed(&mut self, key: &str) {
        self.entries
            .entry(key.to_string())
            .or_insert_with(|| vec![json!({})]);
    }

    fn write(&mut self, id: &str, data: Value, cas: u64) -> Result<u64, Value> {
        let versions = self.entries.entry(id.to_string()).or_default();
        let current = versions.len() as u64;
        if cas != current {
            return Err(json!({
                "errors": ["stale version"]
            }));
        }
        versions.push(data);
        Ok(current + 1)
    }

    pub(crate) fn read(&self, id: &str) -> Option<(Value, u64)> {
        let versions = self.entries.get(id)?;
        let version = versions.len() as u64;
        Some((versions.last()?.clone(), version))
    }

    fn list(&self) -> Option<Vec<String>> {
        if self.entries.is_empty() {
            return None;
        }
        let mut keys: Vec<String> = self.entries.keys().cloned().collect();
        keys.sort();
        Some(keys)
    }
}
