use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct RepositoriesParams {
    pub per_page: u32,
    pub page: u32,
}

#[derive(Debug, Serialize)]
pub struct CommitsParams<'a> {
    pub sha: &'a str,
    pub per_page: u32,
}

#[derive(Debug, Serialize)]
pub struct TreeParams {
    pub recursive: u8,
}

#[derive(Debug, Deserialize)]
pub struct GitHubCommit {
    pub sha: String,
}

#[derive(Debug, Deserialize)]
pub struct GitHubTree {
    #[serde(default)]
    pub tree: Vec<GitHubTreeEntry>,
    #[serde(default)]
    pub truncated: bool,
}

#[derive(Debug, Deserialize)]
pub struct GitHubTreeEntry {
    pub path: String,
    #[serde(default)]
    pub mode: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub sha: String,
    #[serde(default)]
    pub size: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct GitHubBlob {
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub encoding: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct InstallationRepositoriesResponse {
    pub repositories: Vec<GitHubRepo>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GitHubRepo {
    pub id: u64,
    pub default_branch: String,
}
