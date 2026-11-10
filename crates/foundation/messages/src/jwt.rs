use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JwtRequest {
    owner: String,
}

impl JwtRequest {
    pub fn new(owner: impl Into<String>) -> Self {
        Self {
            owner: owner.into(),
        }
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JwtResponse {
    token: String,
    expires_at_unix: i64,
}

impl JwtResponse {
    pub fn new(token: impl Into<String>, expires_at_unix: i64) -> Self {
        Self {
            token: token.into(),
            expires_at_unix,
        }
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn expires_at_unix(&self) -> i64 {
        self.expires_at_unix
    }
}
