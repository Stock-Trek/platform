use crate::dto::github::{
    github_installation::GitHubInstallation, github_ref_type::GitHubRefType,
    github_repository::GitHubRepository,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GitHubEventRef {
    pub installation: GitHubInstallation,
    pub r#ref: String,
    pub ref_type: GitHubRefType,
    pub repository: GitHubRepository,
}
