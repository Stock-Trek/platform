use super::{
    GitHubInstallation,
    types::{GitHubRepo, InstallationRepositoriesResponse, RepositoriesParams},
};
use platform_errors::{BpError, BpResult, GitHubError};

const REPOS_PER_PAGE: u32 = 100;

impl GitHubInstallation {
    pub(super) async fn repos(&self) -> BpResult<Vec<GitHubRepo>> {
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

    pub(super) async fn repo(&self, repo_id: u64) -> BpResult<GitHubRepo> {
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
