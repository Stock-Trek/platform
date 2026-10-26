use crate::dto::github::github_types::{GitHubInstallation, GitHubRepository};
use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GitHubEventRepository {
    pub action: GitHubEventRepositoryAction,
    pub installation: GitHubInstallation,
    pub repository: GitHubRepository,
}

#[derive(Debug, Display, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum GitHubEventRepositoryAction {
    Archived,
    Created,
    Deleted,
    Edited,
    Privatized,
    Publicized,
    Renamed,
    Transferred,
    Unarchived,
    #[default]
    #[serde(other)]
    Other,
}
