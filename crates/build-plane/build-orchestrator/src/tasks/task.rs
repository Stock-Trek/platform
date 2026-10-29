use crate::{
    aws_ops::build_request::BuildRequest,
    context::RsOrchestratorContext,
    tasks::{git_remote::GitRemote, s3_keys::S3Keys, source_archive::SourceArchive},
};
use platform_errors::BpResult;
use platform_git::git_host::GitHost;
use platform_messages::sqs_message::{GitSource, SqsDetail, SqsMessage, SqsRefType};
use platform_time::request_context::RequestContext;
use std::fmt::Display;

const USER_GENERATED_DIRECTORY: &str = "src/algorithms";

pub enum Task {
    AllReposAdd {
        source: GitSource,
    },
    MultiRepoAdd {
        source: GitSource,
        repo_ids: Vec<String>,
    },
    RepoAdd {
        source: GitSource,
        repo_id: String,
    },
    RefCommit {
        source: GitSource,
        repo_id: String,
        ref_name: String,
        ref_type: SqsRefType,
        commit_hash: Option<String>,
    },
}

impl Task {
    pub fn route(&self) -> &'static str {
        match self {
            Self::AllReposAdd { .. } => "all_repos_add",
            Self::MultiRepoAdd { .. } => "multi_repo_add",
            Self::RepoAdd { .. } => "repo_add",
            Self::RefCommit { .. } => "ref_commit",
        }
    }

    pub async fn handle(
        &self,
        request_context: RequestContext<&RsOrchestratorContext>,
    ) -> BpResult<()> {
        match self {
            Self::AllReposAdd { source } => {
                match GitRemote::from_source(&request_context.context.github_auth, source)? {
                    GitRemote::GitHub(remote) => {
                        Self::all_repos_add(request_context, source, &remote).await
                    }
                }
            }
            Self::MultiRepoAdd { source, repo_ids } => {
                Self::multi_repo_task(request_context, source, repo_ids).await
            }
            Self::RepoAdd { source, repo_id } => {
                match GitRemote::from_source(&request_context.context.github_auth, source)? {
                    GitRemote::GitHub(remote) => {
                        Self::repo_add(request_context, source, &remote, repo_id).await
                    }
                }
            }
            Self::RefCommit {
                source,
                repo_id,
                ref_name,
                ref_type,
                commit_hash,
            } => match GitRemote::from_source(&request_context.context.github_auth, source)? {
                GitRemote::GitHub(remote) => {
                    let repo_id = remote.parse_repo_id(repo_id)?;
                    Self::ref_commit(
                        request_context,
                        source,
                        &remote,
                        repo_id,
                        ref_name,
                        *ref_type,
                        commit_hash.clone(),
                    )
                    .await
                }
            },
        }
    }

    async fn all_repos_add<RepoId, Remote>(
        request_context: RequestContext<&RsOrchestratorContext>,
        source: &GitSource,
        remote: &Remote,
    ) -> BpResult<()>
    where
        RepoId: Display,
        Remote: GitHost<RepoId>,
    {
        let repo_ids = remote
            .repo_ids(request_context.deadline)
            .await?
            .into_iter()
            .map(|repo_id| repo_id.to_string())
            .collect::<Vec<_>>();
        Self::multi_repo_task(request_context, source, &repo_ids).await
    }

    async fn multi_repo_task(
        request_context: RequestContext<&RsOrchestratorContext>,
        source: &GitSource,
        repo_ids: &[String],
    ) -> BpResult<()> {
        let messages = repo_ids
            .iter()
            .map(|repo_id| SqsMessage {
                source: source.clone(),
                detail: SqsDetail::RepoAdd {
                    repo_id: repo_id.clone(),
                },
            })
            .collect::<Vec<_>>();
        request_context
            .context
            .sqs
            .send_batch(&messages, request_context.deadline)
            .await
    }

    async fn repo_add<RepoId, Remote>(
        request_context: RequestContext<&RsOrchestratorContext>,
        source: &GitSource,
        remote: &Remote,
        repo_id: &str,
    ) -> BpResult<()>
    where
        RepoId: Display + Clone,
        Remote: GitHost<RepoId>,
    {
        let repo_id = remote.parse_repo_id(repo_id)?;
        let default_branch = remote
            .repo_default_branch(repo_id.clone(), request_context.deadline)
            .await?;
        Self::ref_commit(
            request_context,
            source,
            remote,
            repo_id,
            &default_branch,
            SqsRefType::Branch,
            None,
        )
        .await
    }

    async fn ref_commit<RepoId, Remote>(
        request_context: RequestContext<&RsOrchestratorContext>,
        source: &GitSource,
        remote: &Remote,
        repo_id: RepoId,
        ref_name: &str,
        ref_type: SqsRefType,
        commit_hash: Option<String>,
    ) -> BpResult<()>
    where
        RepoId: Display + Clone,
        Remote: GitHost<RepoId>,
    {
        let repo_id_string = repo_id.to_string();
        let commit_hash = match commit_hash {
            Some(commit_hash) => commit_hash,
            None => {
                remote
                    .ref_commit(repo_id.clone(), ref_name, request_context.deadline)
                    .await?
            }
        };
        let keys = S3Keys::new(remote, repo_id.clone(), ref_name, ref_type, &commit_hash)?;
        tracing::debug!(
            artifact = %keys.artifact_key(),
            compile_output = %keys.compile_output_key(),
            "dispatching build"
        );
        let files = remote
            .directory_files(
                repo_id,
                &commit_hash,
                USER_GENERATED_DIRECTORY,
                request_context.deadline,
            )
            .await?;
        let archive = SourceArchive::create(&files)?;
        let uploaded = request_context
            .context
            .s3
            .put_object(&keys.source_key(), archive, request_context.deadline)
            .await?;
        let signed_post = request_context
            .context
            .s3
            .signed_post(&keys.artifact_prefix(), request_context.deadline)
            .await?;
        request_context
            .context
            .lambda
            .run_build(
                BuildRequest {
                    source: source.clone(),
                    repo_id: repo_id_string,
                    ref_name: ref_name.to_string(),
                    ref_type,
                    commit_hash: Some(commit_hash),
                    signed_get_url: uploaded.signed_get_url,
                    signed_post,
                },
                request_context.deadline,
            )
            .await
    }
}

impl From<SqsMessage> for Task {
    fn from(value: SqsMessage) -> Self {
        let SqsMessage { source, detail } = value;
        match detail {
            SqsDetail::AllReposAdd => Self::AllReposAdd { source },
            SqsDetail::MultiRepoAdd { repo_ids } => Self::MultiRepoAdd { source, repo_ids },
            SqsDetail::RepoAdd { repo_id } => Self::RepoAdd { source, repo_id },
            SqsDetail::RefCreate {
                repo_id,
                ref_name,
                ref_type,
            } => Self::RefCommit {
                source,
                repo_id,
                ref_name,
                ref_type,
                commit_hash: None,
            },
            SqsDetail::Commit {
                repo_id,
                ref_name,
                ref_type,
                commit_hash,
            } => Self::RefCommit {
                source,
                repo_id,
                ref_name,
                ref_type,
                commit_hash: Some(commit_hash),
            },
        }
    }
}
