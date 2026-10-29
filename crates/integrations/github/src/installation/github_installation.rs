use super::types::{CommitsParams, GitHubCommit};
use octocrab::{Octocrab, models::InstallationId};
use platform_config::git::DirectoryLimits;
use platform_errors::{BpError, BpResult, GitHubError, TimeoutError};
use platform_git::git_host::{GitFile, GitHost};
use platform_time::{deadline::Deadline, timeouts::Timeouts};
use std::sync::Arc;

const PROVIDER: &str = "github";

#[derive(Clone)]
pub struct GitHubInstallation {
    pub(super) client: Arc<Octocrab>,
    pub(super) host: String,
    pub(super) timeouts: Timeouts,
    pub(super) directory_limits: DirectoryLimits,
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
