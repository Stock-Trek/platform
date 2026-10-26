use serde::{Deserialize, Serialize, de::IntoDeserializer};
use strum::Display;

#[derive(Debug, Display, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum GitHubEventType {
    Create,
    Delete,
    InstallationRepositories,
    Installation,
    Ping,
    Push,
    Repository,
    #[default]
    #[serde(other)]
    Other,
}

impl From<&str> for GitHubEventType {
    fn from(value: &str) -> Self {
        let deserializer: serde::de::value::StrDeserializer<serde::de::value::Error> =
            value.into_deserializer();
        Self::deserialize(deserializer).unwrap_or_default()
    }
}
