use super::github_repository_selection::GitHubRepositorySelection;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GitHubInstallation {
    pub id: u64,
    #[serde(default)]
    pub repository_selection: GitHubRepositorySelection,
    #[serde(default)]
    pub html_url: String,
}
