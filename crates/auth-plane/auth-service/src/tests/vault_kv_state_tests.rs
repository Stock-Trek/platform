use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap};

#[derive(Clone, Debug)]
pub struct CapturedRequest {
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub token: Option<String>,
    pub body: Value,
}

#[derive(Default)]
pub struct VaultKvState {
    entries: HashMap<String, Vec<Value>>,
}

impl VaultKvState {
    pub fn put_response(&mut self, id: &str, body: &Value) -> (u16, Value) {
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

    pub fn get_response(&self, id: &str) -> (u16, Value) {
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

    pub fn list_response(&self, directory: &str) -> (u16, Value) {
        match self.list(directory) {
            Some(keys) => (200, json!({ "data": { "keys": keys } })),
            None => (404, json!({ "errors": [] })),
        }
    }

    pub fn delete(&mut self, id: &str) {
        self.entries.remove(id);
    }

    pub fn seed(&mut self, key: &str) {
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

    pub fn read(&self, id: &str) -> Option<(Value, u64)> {
        let versions = self.entries.get(id)?;
        let version = versions.len() as u64;
        Some((versions.last()?.clone(), version))
    }

    fn list(&self, directory: &str) -> Option<Vec<String>> {
        let prefix = if directory.is_empty() {
            String::new()
        } else {
            format!("{}/", directory.trim_end_matches('/'))
        };
        let mut keys = BTreeSet::new();
        for key in self.entries.keys() {
            let Some(rest) = key.strip_prefix(&prefix) else {
                continue;
            };
            if rest.is_empty() {
                continue;
            }
            let child = match rest.split_once('/') {
                Some((first, _)) => format!("{first}/"),
                None => rest.to_string(),
            };
            keys.insert(child);
        }
        if keys.is_empty() {
            None
        } else {
            Some(keys.into_iter().collect())
        }
    }
}
