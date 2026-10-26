use platform_errors::BpResult;
use platform_time::deadline::Deadline;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitFile {
    pub path: String,
    pub contents: Vec<u8>,
}

#[allow(async_fn_in_trait)]
pub trait GitHost<RepoId>: Send + Sync {
    fn provider(&self) -> &'static str;
    fn host(&self) -> &str;
    fn parse_repo_id(&self, repo_id: &str) -> BpResult<RepoId>;
    async fn repo_ids(&self, deadline: Deadline) -> BpResult<Vec<RepoId>>;
    async fn repo_default_branch(&self, repo_id: RepoId, deadline: Deadline) -> BpResult<String>;
    async fn ref_commit(
        &self,
        repo_id: RepoId,
        ref_name: &str,
        deadline: Deadline,
    ) -> BpResult<String>;
    async fn directory_files(
        &self,
        repo_id: RepoId,
        ref_name: &str,
        directory: &str,
        deadline: Deadline,
    ) -> BpResult<Vec<GitFile>>;
}
