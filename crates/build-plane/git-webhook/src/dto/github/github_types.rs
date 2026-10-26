use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GitHubInstallation {
    pub id: u64,
    #[serde(default)]
    pub repository_selection: GitHubRepositorySelection,
    #[serde(default)]
    pub html_url: String,
}

#[derive(Debug, Display, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum GitHubRepositorySelection {
    All,
    Selected,
    #[default]
    #[serde(other)]
    Other,
}

#[derive(Debug, Display, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum GitHubRefType {
    Tag,
    Branch,
    #[default]
    #[serde(other)]
    Other,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GitHubRepository {
    pub id: u64,
}
