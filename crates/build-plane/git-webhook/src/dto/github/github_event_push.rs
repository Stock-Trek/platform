use crate::dto::github::{
    github_installation::GitHubInstallation, github_repository::GitHubRepository,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GitHubEventPush {
    pub after: String,
    pub before: String,
    pub created: bool,
    pub deleted: bool,
    pub installation: GitHubInstallation,
    pub r#ref: String,
    pub repository: GitHubRepository,
}
