use super::credential_entry::CredentialEntry;
use platform_config::EnvVars;
use platform_errors::{BpError, BpResult};
use secrecy::SecretString;
use std::{collections::BTreeSet, fmt};

pub const CREDENTIALS: &str = "PORTFOLIO_BINANCE_CREDENTIALS";

#[derive(Clone)]
pub struct BinanceCredentialConfig {
    account: String,
    api_key: String,
    secret: SecretString,
}

impl BinanceCredentialConfig {
    pub fn new(account: impl Into<String>, api_key: impl Into<String>, secret: &str) -> Self {
        Self {
            account: account.into(),
            api_key: api_key.into(),
            secret: SecretString::from(secret.to_string()),
        }
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Vec<Self>> {
        let Some(payload) = vars.optional(CREDENTIALS)? else {
            return Ok(Vec::new());
        };
        let entries: Vec<CredentialEntry> = serde_json::from_str(&payload)
            .map_err(|error| BpError::config_invalid_source(CREDENTIALS, error))?;
        let mut seen = BTreeSet::new();
        let mut credentials = Vec::with_capacity(entries.len());
        for entry in entries {
            if entry.account.trim().is_empty() {
                return Err(BpError::config_invalid(CREDENTIALS, "account is empty"));
            }
            if entry.api_key.trim().is_empty() {
                return Err(BpError::config_invalid(CREDENTIALS, "api_key is empty"));
            }
            if entry.secret.trim().is_empty() {
                return Err(BpError::config_invalid(CREDENTIALS, "secret is empty"));
            }
            if !seen.insert(entry.account.clone()) {
                return Err(BpError::config_invalid(
                    CREDENTIALS,
                    format!("duplicate account {}", entry.account),
                ));
            }
            credentials.push(Self {
                account: entry.account,
                api_key: entry.api_key,
                secret: SecretString::from(entry.secret),
            });
        }
        Ok(credentials)
    }

    pub fn account(&self) -> &str {
        &self.account
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    pub fn secret(&self) -> &SecretString {
        &self.secret
    }
}

impl fmt::Debug for BinanceCredentialConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BinanceCredentialConfig")
            .field("account", &self.account)
            .field("api_key", &self.api_key)
            .field("secret", &"[redacted]")
            .finish()
    }
}
