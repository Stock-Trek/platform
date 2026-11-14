use crate::SigV4Credentials;
use platform_config::SIGV4_CREDENTIALS;
use platform_errors::{BuildError, BuildResult};
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RegistryEntry {
    access_key_id: String,
    secret_access_key: String,
    owner: String,
    #[serde(default)]
    session_token: Option<String>,
}

#[derive(Clone)]
struct RegistryCaller {
    owner: String,
    credentials: SigV4Credentials,
}

#[derive(Clone)]
pub struct SigV4Registry {
    callers: BTreeMap<String, RegistryCaller>,
}

impl SigV4Registry {
    pub fn from_json(value: &str) -> BuildResult<Self> {
        let entries = serde_json::from_str::<Vec<RegistryEntry>>(value)
            .map_err(|error| BuildError::config_invalid_source(SIGV4_CREDENTIALS, error))?;
        Self::from_entries(entries)
    }

    pub fn from_parts(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        owner: impl Into<String>,
        session_token: Option<String>,
    ) -> BuildResult<Self> {
        let entries = vec![RegistryEntry {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            owner: owner.into(),
            session_token,
        }];
        Self::from_entries(entries)
    }

    pub fn credentials_for_owner(&self, owner: &str) -> BuildResult<&SigV4Credentials> {
        let mut matching = self.callers.values().filter(|caller| caller.owner == owner);
        let caller = matching.next().ok_or_else(|| {
            BuildError::config_invalid(
                SIGV4_CREDENTIALS,
                format!("no credentials for owner {owner}"),
            )
        })?;
        if matching.next().is_some() {
            return Err(BuildError::config_invalid(
                SIGV4_CREDENTIALS,
                format!("multiple credentials for owner {owner}"),
            ));
        }
        Ok(&caller.credentials)
    }

    pub fn caller(&self, access_key_id: &str) -> Option<(&SigV4Credentials, &str)> {
        self.callers
            .get(access_key_id)
            .map(|caller| (&caller.credentials, caller.owner.as_str()))
    }

    fn from_entries(entries: Vec<RegistryEntry>) -> BuildResult<Self> {
        if entries.is_empty() {
            return Err(BuildError::config_invalid(
                SIGV4_CREDENTIALS,
                "must contain at least one credential",
            ));
        }
        let mut callers = BTreeMap::new();
        let mut owners = BTreeMap::new();
        for entry in entries {
            Self::validate_owner(&entry.owner)?;
            let credentials = SigV4Credentials::new(
                entry.access_key_id,
                entry.secret_access_key,
                entry.session_token,
            )?;
            let access_key_id = credentials.access_key_id().to_string();
            if owners
                .insert(entry.owner.clone(), access_key_id.clone())
                .is_some()
            {
                return Err(BuildError::config_invalid(
                    SIGV4_CREDENTIALS,
                    format!("duplicate owner {}", entry.owner),
                ));
            }
            if callers
                .insert(
                    access_key_id,
                    RegistryCaller {
                        owner: entry.owner,
                        credentials,
                    },
                )
                .is_some()
            {
                return Err(BuildError::config_invalid(
                    SIGV4_CREDENTIALS,
                    "duplicate access key id",
                ));
            }
        }
        Ok(Self { callers })
    }

    fn validate_owner(owner: &str) -> BuildResult<()> {
        if owner.is_empty() {
            return Err(BuildError::config_invalid(
                SIGV4_CREDENTIALS,
                "owner must not be empty",
            ));
        }
        if owner.trim() != owner || owner.chars().any(char::is_control) {
            return Err(BuildError::config_invalid(
                SIGV4_CREDENTIALS,
                "owner must not contain surrounding whitespace or control characters",
            ));
        }
        Ok(())
    }
}

impl std::fmt::Debug for SigV4Registry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SigV4Registry")
            .field(
                "owners",
                &self.callers.values().map(|c| &c.owner).collect::<Vec<_>>(),
            )
            .finish()
    }
}
