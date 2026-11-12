use platform_errors::BuildResult;
use platform_time::Deadline;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitFile {
    pub path: String,
    pub contents: Vec<u8>,
}

#[allow(async_fn_in_trait)]
pub trait GitHost<RepoId>: Send + Sync {
    fn provider(&self) -> &'static str;
    fn host(&self) -> &str;
    fn parse_repo_id(&self, repo_id: &str) -> BuildResult<RepoId>;
    async fn repo_ids(&self, deadline: Deadline) -> BuildResult<Vec<RepoId>>;
    async fn repo_default_branch(&self, repo_id: RepoId, deadline: Deadline)
    -> BuildResult<String>;
    async fn ref_commit(
        &self,
        repo_id: RepoId,
        ref_name: &str,
        deadline: Deadline,
    ) -> BuildResult<String>;
    async fn directory_files(
        &self,
        repo_id: RepoId,
        ref_name: &str,
        directory: &str,
        deadline: Deadline,
    ) -> BuildResult<Vec<GitFile>>;
}
