use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub(crate) struct RepositoriesParams {
    pub(crate) per_page: u32,
    pub(crate) page: u32,
}

#[derive(Debug, Serialize)]
pub(crate) struct CommitsParams<'a> {
    pub(crate) sha: &'a str,
    pub(crate) per_page: u32,
}

#[derive(Debug, Serialize)]
pub(crate) struct TreeParams {
    pub(crate) recursive: u8,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GitHubCommit {
    pub(crate) sha: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GitHubTree {
    #[serde(default)]
    pub(crate) tree: Vec<GitHubTreeEntry>,
    #[serde(default)]
    pub(crate) truncated: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GitHubTreeEntry {
    pub(crate) path: String,
    #[serde(default)]
    pub(crate) mode: String,
    #[serde(rename = "type")]
    pub(crate) kind: String,
    pub(crate) sha: String,
    #[serde(default)]
    pub(crate) size: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GitHubBlob {
    #[serde(default)]
    pub(crate) content: Option<String>,
    #[serde(default)]
    pub(crate) encoding: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InstallationRepositoriesResponse {
    pub(crate) repositories: Vec<GitHubRepo>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct GitHubRepo {
    pub(crate) id: u64,
    pub(crate) default_branch: String,
}
