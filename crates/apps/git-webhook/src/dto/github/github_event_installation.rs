use crate::dto::github::github_types::{GitHubInstallation, GitHubRepository};
use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GitHubEventInstallation {
    pub action: GitHubEventInstallationAction,
    pub installation: GitHubInstallation,
    #[serde(default)]
    pub repositories: Vec<GitHubRepository>,
}

#[derive(Debug, Display, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum GitHubEventInstallationAction {
    Created,
    Deleted,
    NewPermissionsAccepted,
    Suspend,
    Unsuspend,
    #[default]
    #[serde(other)]
    Other,
}
