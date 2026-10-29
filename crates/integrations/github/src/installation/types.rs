use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub(super) struct RepositoriesParams {
    pub(super) per_page: u32,
    pub(super) page: u32,
}

#[derive(Debug, Serialize)]
pub(super) struct CommitsParams<'a> {
    pub(super) sha: &'a str,
    pub(super) per_page: u32,
}

#[derive(Debug, Serialize)]
pub(super) struct TreeParams {
    pub(super) recursive: u8,
}

#[derive(Debug, Deserialize)]
pub(super) struct GitHubCommit {
    pub(super) sha: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct GitHubTree {
    #[serde(default)]
    pub(super) tree: Vec<GitHubTreeEntry>,
    #[serde(default)]
    pub(super) truncated: bool,
}

#[derive(Debug, Deserialize)]
pub(super) struct GitHubTreeEntry {
    pub(super) path: String,
    #[serde(default)]
    pub(super) mode: String,
    #[serde(rename = "type")]
    pub(super) kind: String,
    pub(super) sha: String,
    #[serde(default)]
    pub(super) size: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub(super) struct GitHubBlob {
    #[serde(default)]
    pub(super) content: Option<String>,
    #[serde(default)]
    pub(super) encoding: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct InstallationRepositoriesResponse {
    pub(super) repositories: Vec<GitHubRepo>,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct GitHubRepo {
    pub(super) id: u64,
    pub(super) default_branch: String,
}
