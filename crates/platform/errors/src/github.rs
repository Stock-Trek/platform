use crate::retryability::Retryability;

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum GitHubError {
    #[error("failed to build GitHub client: {0}")]
    BuildClient(#[source] octocrab::Error),
    #[error("no commit found for {ref_name:?} in repository {repo_id}")]
    CommitNotFound { repo_id: u64, ref_name: String },
    #[error("path {path:?} was not found in repository {repo_id} at {ref_name:?}: {source}")]
    ContentNotFound {
        repo_id: u64,
        path: String,
        ref_name: String,
        #[source]
        source: octocrab::Error,
    },
    #[error("failed to decode {encoding:?} content at {path:?} in repository {repo_id}: {source}")]
    DecodeContent {
        repo_id: u64,
        path: String,
        encoding: &'static str,
        #[source]
        source: base64::DecodeError,
    },
    #[error("directory {path:?} in repository {repo_id} exceeds the maximum depth of {max_depth}")]
    DirectoryDepthExceeded {
        repo_id: u64,
        path: String,
        max_depth: usize,
    },
    #[error("directory {path:?} was not found in repository {repo_id} at {ref_name:?}")]
    DirectoryNotFound {
        repo_id: u64,
        path: String,
        ref_name: String,
    },
    #[error(
        "directory {directory:?} in repository {repo_id} exceeds the maximum total size of {max_bytes} bytes"
    )]
    DirectoryTooLarge {
        repo_id: u64,
        directory: String,
        max_bytes: u64,
    },
    #[error("failed to get blob {sha} in repository {repo_id}: {source}")]
    GetBlob {
        repo_id: u64,
        sha: String,
        #[source]
        source: octocrab::Error,
    },
    #[error("failed to get commit for {ref_name:?} in repository {repo_id}: {source}")]
    GetCommit {
        repo_id: u64,
        ref_name: String,
        #[source]
        source: octocrab::Error,
    },
    #[error("failed to get contents at {path:?} in repository {repo_id}: {source}")]
    GetContent {
        repo_id: u64,
        path: String,
        #[source]
        source: octocrab::Error,
    },
    #[error("failed to get repository {repo_id}: {source}")]
    GetRepository {
        repo_id: u64,
        #[source]
        source: octocrab::Error,
    },
    #[error("failed to get tree for {ref_name:?} in repository {repo_id}: {source}")]
    GetTree {
        repo_id: u64,
        ref_name: String,
        #[source]
        source: octocrab::Error,
    },
    #[error("{host:?} is not a hostname, IP address, or host:port")]
    HostInvalid { host: String },
    #[error("host {host:?} is not in the allowed GitHub hosts list")]
    HostNotAllowed { host: String },
    #[error("failed to create client for installation {installation_id}: {source}")]
    Installation {
        installation_id: u64,
        #[source]
        source: octocrab::Error,
    },
    #[error("content path {path:?} in repository {repo_id} is outside directory {directory:?}")]
    InvalidContentPath {
        repo_id: u64,
        path: String,
        directory: String,
    },
    #[error("GitHub repository id {repo_id:?} is not numeric: {source}")]
    InvalidRepositoryId {
        repo_id: String,
        #[source]
        source: std::num::ParseIntError,
    },
    #[error("failed to list installation repositories: {0}")]
    ListRepositories(#[source] octocrab::Error),
    #[error("content for {path:?} in repository {repo_id} is missing")]
    MissingContent { repo_id: u64, path: String },
    #[error("repository {repo_id} was not found: {source}")]
    RepositoryNotFound {
        repo_id: u64,
        #[source]
        source: octocrab::Error,
    },
    #[error("directory {directory:?} in repository {repo_id} contains more than {max_files} files")]
    TooManyFiles {
        repo_id: u64,
        directory: String,
        max_files: usize,
    },
    #[error("directory listing for repository {repo_id} at {ref_name:?} was truncated by GitHub")]
    TreeTruncated { repo_id: u64, ref_name: String },
    #[error("unsupported content encoding {encoding:?} at {path:?} in repository {repo_id}")]
    UnsupportedContentEncoding {
        repo_id: u64,
        path: String,
        encoding: String,
    },
    #[error("unsupported content type {content_type:?} at {path:?} in repository {repo_id}")]
    UnsupportedContentType {
        repo_id: u64,
        path: String,
        content_type: String,
    },
}

impl GitHubError {
    pub fn error_type(&self) -> &'static str {
        self.into()
    }

    pub fn retryability(&self) -> Retryability {
        match self {
            Self::BuildClient(_)
            | Self::CommitNotFound { .. }
            | Self::ContentNotFound { .. }
            | Self::DecodeContent { .. }
            | Self::DirectoryDepthExceeded { .. }
            | Self::DirectoryNotFound { .. }
            | Self::DirectoryTooLarge { .. }
            | Self::HostInvalid { .. }
            | Self::HostNotAllowed { .. }
            | Self::InvalidContentPath { .. }
            | Self::InvalidRepositoryId { .. }
            | Self::MissingContent { .. }
            | Self::RepositoryNotFound { .. }
            | Self::TooManyFiles { .. }
            | Self::TreeTruncated { .. }
            | Self::UnsupportedContentEncoding { .. }
            | Self::UnsupportedContentType { .. } => Retryability::Permanent,
            Self::GetCommit { source, .. }
            | Self::GetRepository { source, .. }
            | Self::Installation { source, .. }
            | Self::ListRepositories(source) => Retryability::from_octocrab(source),
            Self::GetBlob { source, .. }
            | Self::GetContent { source, .. }
            | Self::GetTree { source, .. } => Retryability::from_octocrab(source),
        }
    }
}
