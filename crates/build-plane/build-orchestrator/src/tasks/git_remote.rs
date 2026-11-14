use platform_errors::PlatformResult;
use platform_github::{GitHubAuth, GitHubInstallation};
use platform_messages::GitSource;

pub enum GitRemote {
    GitHub(GitHubInstallation),
}

impl GitRemote {
    pub fn from_source(github_auth: &GitHubAuth, source: &GitSource) -> PlatformResult<Self> {
        match source {
            GitSource::GitHub {
                host,
                installation_id,
                ..
            } => Ok(Self::GitHub(
                github_auth.installation(host, *installation_id)?,
            )),
        }
    }
}
