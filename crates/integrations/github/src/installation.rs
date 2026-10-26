use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use octocrab::{Octocrab, models::InstallationId};
use platform_config::git::DirectoryLimits;
use platform_errors::{BpError, BpResult, GitHubError, TimeoutError};
use platform_git::git_host::{GitFile, GitHost};
use platform_time::{deadline::Deadline, timeouts::Timeouts};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};

const PROVIDER: &str = "github";
const REPOS_PER_PAGE: u32 = 100;

#[derive(Debug, Serialize)]
struct RepositoriesParams {
    per_page: u32,
    page: u32,
}

#[derive(Debug, Serialize)]
struct CommitsParams<'a> {
    sha: &'a str,
    per_page: u32,
}

#[derive(Debug, Serialize)]
struct TreeParams {
    recursive: u8,
}

#[derive(Debug, Deserialize)]
struct GitHubCommit {
    sha: String,
}

#[derive(Debug, Deserialize)]
struct GitHubTree {
    #[serde(default)]
    tree: Vec<GitHubTreeEntry>,
    #[serde(default)]
    truncated: bool,
}

#[derive(Debug, Deserialize)]
struct GitHubTreeEntry {
    path: String,
    #[serde(default)]
    mode: String,
    #[serde(rename = "type")]
    kind: String,
    sha: String,
    #[serde(default)]
    size: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct GitHubBlob {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    encoding: Option<String>,
}

#[derive(Debug, Deserialize)]
struct InstallationRepositoriesResponse {
    repositories: Vec<GitHubRepo>,
}

#[derive(Debug, Clone, Deserialize)]
struct GitHubRepo {
    id: u64,
    default_branch: String,
}

#[derive(Clone)]
pub struct GitHubInstallation {
    client: Arc<Octocrab>,
    host: String,
    timeouts: Timeouts,
    directory_limits: DirectoryLimits,
}

impl GitHost<u64> for GitHubInstallation {
    fn provider(&self) -> &'static str {
        PROVIDER
    }

    fn host(&self) -> &str {
        &self.host
    }

    fn parse_repo_id(&self, repo_id: &str) -> BpResult<u64> {
        repo_id.parse().map_err(|error| {
            BpError::GitHub(GitHubError::InvalidRepositoryId {
                repo_id: repo_id.to_owned(),
                source: error,
            })
        })
    }

    async fn repo_ids(&self, deadline: Deadline) -> BpResult<Vec<u64>> {
        let timeout = self.timeouts.operation_timeout(deadline)?;
        let repos = tokio::time::timeout(timeout, self.repos())
            .await
            .map_err(|_| {
                BpError::Timeout(TimeoutError::Operation {
                    operation: "GitHub list repositories",
                })
            })??;
        Ok(repos.into_iter().map(|repo| repo.id).collect())
    }

    async fn repo_default_branch(&self, repo_id: u64, deadline: Deadline) -> BpResult<String> {
        let timeout = self.timeouts.operation_timeout(deadline)?;
        let repo = tokio::time::timeout(timeout, self.repo(repo_id))
            .await
            .map_err(|_| {
                BpError::Timeout(TimeoutError::Operation {
                    operation: "GitHub get repository",
                })
            })?;
        repo.map(|r| r.default_branch)
    }

    async fn ref_commit(
        &self,
        repo_id: u64,
        ref_name: &str,
        deadline: Deadline,
    ) -> BpResult<String> {
        let timeout = self.timeouts.operation_timeout(deadline)?;
        let params = CommitsParams {
            sha: ref_name,
            per_page: 1,
        };
        let commits = tokio::time::timeout(
            timeout,
            self.client.get::<Vec<GitHubCommit>, _, _>(
                format!("/repositories/{repo_id}/commits"),
                Some(&params),
            ),
        )
        .await
        .map_err(|_| {
            BpError::Timeout(TimeoutError::Operation {
                operation: "GitHub get commit",
            })
        })?
        .map_err(|error| {
            BpError::GitHub(GitHubError::GetCommit {
                repo_id,
                ref_name: ref_name.to_owned(),
                source: error,
            })
        })?;
        commits
            .into_iter()
            .next()
            .map(|commit| commit.sha)
            .ok_or_else(|| {
                BpError::GitHub(GitHubError::CommitNotFound {
                    repo_id,
                    ref_name: ref_name.to_owned(),
                })
            })
    }

    async fn directory_files(
        &self,
        repo_id: u64,
        ref_name: &str,
        directory: &str,
        deadline: Deadline,
    ) -> BpResult<Vec<GitFile>> {
        let timeout = self.timeouts.operation_timeout(deadline)?;
        tokio::time::timeout(
            timeout,
            self.collect_directory_files(repo_id, ref_name, directory),
        )
        .await
        .map_err(|_| {
            BpError::Timeout(TimeoutError::Operation {
                operation: "GitHub get repository contents",
            })
        })?
    }
}

impl GitHubInstallation {
    pub fn new(
        app: &Octocrab,
        host: &str,
        installation_id: u64,
        timeouts: Timeouts,
    ) -> BpResult<Self> {
        Self::with_limits(
            app,
            host,
            installation_id,
            timeouts,
            DirectoryLimits::default(),
        )
    }

    pub fn with_limits(
        app: &Octocrab,
        host: &str,
        installation_id: u64,
        timeouts: Timeouts,
        directory_limits: DirectoryLimits,
    ) -> BpResult<Self> {
        let client = app
            .installation(InstallationId::from(installation_id))
            .map_err(|error| {
                BpError::GitHub(GitHubError::Installation {
                    installation_id,
                    source: error,
                })
            })?;
        Ok(Self {
            client: Arc::new(client),
            host: host.to_owned(),
            timeouts,
            directory_limits,
        })
    }
}

// helpers
impl GitHubInstallation {
    async fn collect_directory_files(
        &self,
        repo_id: u64,
        ref_name: &str,
        directory: &str,
    ) -> BpResult<Vec<GitFile>> {
        let directory = directory.trim_matches('/');
        if !directory.is_empty() {
            Self::validate_relative_path(repo_id, directory, "")?;
        }

        // The Contents API silently caps directory listings at 1,000 entries.
        // The Git Trees API returns the complete tree (and reports when GitHub
        // had to truncate it), so use that for directory discovery instead.
        let tree = self.tree(repo_id, ref_name).await?;
        if tree.truncated {
            return Err(BpError::GitHub(GitHubError::TreeTruncated {
                repo_id,
                ref_name: ref_name.to_owned(),
            }));
        }

        if !directory.is_empty()
            && !tree
                .tree
                .iter()
                .any(|entry| entry.path == directory && entry.kind == "tree")
        {
            return Err(BpError::GitHub(GitHubError::DirectoryNotFound {
                repo_id,
                path: directory.to_owned(),
                ref_name: ref_name.to_owned(),
            }));
        }

        let prefix = if directory.is_empty() {
            String::new()
        } else {
            format!("{directory}/")
        };
        let mut files = Vec::new();
        let mut total_bytes = 0_u64;
        for entry in tree.tree {
            let Some(relative) = entry.path.strip_prefix(&prefix) else {
                continue;
            };
            if relative.is_empty() {
                continue;
            }

            let depth = relative.matches('/').count();
            if depth > self.directory_limits.max_depth {
                return Err(BpError::GitHub(GitHubError::DirectoryDepthExceeded {
                    repo_id,
                    path: entry.path,
                    max_depth: self.directory_limits.max_depth,
                }));
            }
            Self::validate_relative_path(repo_id, relative, directory)?;

            match entry.kind.as_str() {
                "tree" => {}
                "blob" => {
                    // Symlinks are blobs with mode 120000; like submodules they
                    // are not part of the source archive.
                    if entry.mode == "120000" {
                        continue;
                    }
                    if files.len() >= self.directory_limits.max_files {
                        return Err(BpError::GitHub(GitHubError::TooManyFiles {
                            repo_id,
                            directory: directory.to_owned(),
                            max_files: self.directory_limits.max_files,
                        }));
                    }
                    if let Some(size) = entry.size
                        && total_bytes.saturating_add(size) > self.directory_limits.max_total_bytes
                    {
                        return Err(BpError::GitHub(GitHubError::DirectoryTooLarge {
                            repo_id,
                            directory: directory.to_owned(),
                            max_bytes: self.directory_limits.max_total_bytes,
                        }));
                    }
                    let contents = self.blob(repo_id, &entry.sha).await?;
                    total_bytes = total_bytes.saturating_add(contents.len() as u64);
                    if total_bytes > self.directory_limits.max_total_bytes {
                        return Err(BpError::GitHub(GitHubError::DirectoryTooLarge {
                            repo_id,
                            directory: directory.to_owned(),
                            max_bytes: self.directory_limits.max_total_bytes,
                        }));
                    }
                    files.push(GitFile {
                        path: relative.to_owned(),
                        contents,
                    });
                }
                "commit" => {}
                kind => {
                    return Err(BpError::GitHub(GitHubError::UnsupportedContentType {
                        repo_id,
                        path: entry.path,
                        content_type: kind.to_owned(),
                    }));
                }
            }
        }
        files.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(files)
    }

    async fn tree(&self, repo_id: u64, ref_name: &str) -> BpResult<GitHubTree> {
        let params = TreeParams { recursive: 1 };
        self.client
            .get::<GitHubTree, _, _>(
                format!(
                    "/repositories/{repo_id}/git/trees/{}",
                    Self::encode_path(ref_name)
                ),
                Some(&params),
            )
            .await
            .map_err(|source| {
                BpError::GitHub(GitHubError::GetTree {
                    repo_id,
                    ref_name: ref_name.to_owned(),
                    source,
                })
            })
    }

    async fn blob(&self, repo_id: u64, sha: &str) -> BpResult<Vec<u8>> {
        let blob = self
            .client
            .get::<GitHubBlob, _, _>(
                format!("/repositories/{repo_id}/git/blobs/{sha}"),
                None::<&()>,
            )
            .await
            .map_err(|source| {
                BpError::GitHub(GitHubError::GetBlob {
                    repo_id,
                    sha: sha.to_owned(),
                    source,
                })
            })?;
        Self::decode_content(
            repo_id,
            sha,
            blob.content.as_deref(),
            blob.encoding.as_deref(),
        )?
        .ok_or_else(|| {
            BpError::GitHub(GitHubError::MissingContent {
                repo_id,
                path: sha.to_owned(),
            })
        })
    }

    fn decode_content(
        repo_id: u64,
        path: &str,
        content: Option<&str>,
        encoding: Option<&str>,
    ) -> BpResult<Option<Vec<u8>>> {
        let Some(content) = content else {
            return Ok(None);
        };
        match encoding {
            Some("base64") => {
                let compact = content
                    .chars()
                    .filter(|character| !character.is_ascii_whitespace())
                    .collect::<String>();
                BASE64.decode(compact).map(Some).map_err(|source| {
                    BpError::GitHub(GitHubError::DecodeContent {
                        repo_id,
                        path: path.to_owned(),
                        encoding: "base64",
                        source,
                    })
                })
            }
            Some("utf8") => Ok(Some(content.as_bytes().to_vec())),
            Some("none") | None if content.is_empty() => Ok(None),
            Some(encoding) => Err(BpError::GitHub(GitHubError::UnsupportedContentEncoding {
                repo_id,
                path: path.to_owned(),
                encoding: encoding.to_owned(),
            })),
            None => Ok(Some(content.as_bytes().to_vec())),
        }
    }

    fn encode_path(path: &str) -> String {
        path.split('/')
            .map(|segment| {
                let mut encoded = String::new();
                for byte in segment.bytes() {
                    match byte {
                        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                            encoded.push(char::from(byte))
                        }
                        byte => encoded.push_str(&format!("%{byte:02X}")),
                    }
                }
                encoded
            })
            .collect::<Vec<_>>()
            .join("/")
    }

    fn validate_relative_path(repo_id: u64, path: &str, directory: &str) -> BpResult<()> {
        let path = Path::new(path);
        if path.as_os_str().is_empty()
            || path.to_string_lossy().contains('\\')
            || path
                .components()
                .any(|component| !matches!(component, std::path::Component::Normal(_)))
        {
            return Err(BpError::GitHub(GitHubError::InvalidContentPath {
                repo_id,
                path: path.display().to_string(),
                directory: directory.to_owned(),
            }));
        }
        Ok(())
    }

    async fn repos(&self) -> BpResult<Vec<GitHubRepo>> {
        let mut repos = Vec::new();
        let mut page = 1;
        loop {
            let params = RepositoriesParams {
                per_page: REPOS_PER_PAGE,
                page,
            };
            let response = self
                .client
                .get::<InstallationRepositoriesResponse, _, _>(
                    "/installation/repositories",
                    Some(&params),
                )
                .await
                .map_err(|error| BpError::GitHub(GitHubError::ListRepositories(error)))?;
            let received = response.repositories.len();
            repos.extend(response.repositories);
            if received < REPOS_PER_PAGE as usize {
                break;
            }
            page += 1;
        }
        Ok(repos)
    }

    async fn repo(&self, repo_id: u64) -> BpResult<GitHubRepo> {
        self.client
            .get::<GitHubRepo, _, _>(format!("/repositories/{repo_id}"), None::<&()>)
            .await
            .map_err(|error| {
                if Self::is_not_found(&error) {
                    BpError::GitHub(GitHubError::RepositoryNotFound {
                        repo_id,
                        source: error,
                    })
                } else {
                    BpError::GitHub(GitHubError::GetRepository {
                        repo_id,
                        source: error,
                    })
                }
            })
    }

    fn is_not_found(error: &octocrab::Error) -> bool {
        matches!(
            error,
            octocrab::Error::GitHub { source, .. } if source.status_code.as_u16() == 404
        )
    }
}
