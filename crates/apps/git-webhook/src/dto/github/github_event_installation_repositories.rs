use crate::dto::github::github_types::{GitHubInstallation, GitHubRepository};
use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GitHubEventInstallationRepositories {
    pub action: GitHubEventInstallationRepositoriesAction,
    pub installation: GitHubInstallation,
    #[serde(default)]
    pub repositories_added: Vec<GitHubRepository>,
    #[serde(default)]
    pub repositories_removed: Vec<GitHubRepository>,
}

#[derive(Debug, Display, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum GitHubEventInstallationRepositoriesAction {
    Added,
    Removed,
    #[default]
    #[serde(other)]
    Other,
}
