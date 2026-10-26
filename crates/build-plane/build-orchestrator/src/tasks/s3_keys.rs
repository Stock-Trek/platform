use platform_errors::{BpError, BpResult, ValidationError};
use platform_git::git_host::GitHost;
use platform_messages::sqs_message::SqsRefType;
use std::fmt::Display;

const SOURCE_PREFIX: &str = "source/";
const ARTIFACT_PREFIX: &str = "artifact/";

pub struct S3Keys;

impl S3Keys {
    pub fn ref_key<RepoId, Remote>(
        git_remote: &Remote,
        repo_id: RepoId,
        ref_name: &str,
        ref_type: SqsRefType,
        commit_hash: &str,
    ) -> BpResult<String>
    where
        RepoId: Display,
        Remote: GitHost<RepoId>,
    {
        let repo = Self::sanitize_path(&repo_id.to_string())?;
        let ref_name = Self::sanitize_path(ref_name)?;
        let commit_hash = Self::sanitize_path(commit_hash)?;
        let ref_type_name = match ref_type {
            SqsRefType::Branch => "heads",
            SqsRefType::Tag => "tags",
        };
        Ok(format!(
            "{}/{}/{}/{}/{}/{}/",
            git_remote.provider(),
            git_remote.host(),
            repo,
            ref_type_name,
            ref_name,
            commit_hash
        ))
    }

    pub fn source_prefix<RepoId, Remote>(
        git_remote: &Remote,
        repo_id: RepoId,
        ref_name: &str,
        ref_type: SqsRefType,
        commit_hash: &str,
    ) -> BpResult<String>
    where
        RepoId: Display,
        Remote: GitHost<RepoId>,
    {
        Ok(format!(
            "{}{SOURCE_PREFIX}",
            Self::ref_key(git_remote, repo_id, ref_name, ref_type, commit_hash)?
        ))
    }

    pub fn source_key<RepoId, Remote>(
        git_remote: &Remote,
        repo_id: RepoId,
        ref_name: &str,
        ref_type: SqsRefType,
        commit_hash: &str,
    ) -> BpResult<String>
    where
        RepoId: Display,
        Remote: GitHost<RepoId>,
    {
        Ok(format!(
            "{}algorithms.zip",
            Self::source_prefix(git_remote, repo_id, ref_name, ref_type, commit_hash)?
        ))
    }

    pub fn artifact_prefix<RepoId, Remote>(
        git_remote: &Remote,
        repo_id: RepoId,
        ref_name: &str,
        ref_type: SqsRefType,
        commit_hash: &str,
    ) -> BpResult<String>
    where
        RepoId: Display,
        Remote: GitHost<RepoId>,
    {
        Ok(format!(
            "{}{ARTIFACT_PREFIX}",
            Self::ref_key(git_remote, repo_id, ref_name, ref_type, commit_hash)?
        ))
    }

    pub fn artifact_key<RepoId, Remote>(
        git_remote: &Remote,
        repo_id: RepoId,
        ref_name: &str,
        ref_type: SqsRefType,
        commit_hash: &str,
    ) -> BpResult<String>
    where
        RepoId: Display,
        Remote: GitHost<RepoId>,
    {
        Ok(format!(
            "{}runner.cwasm",
            Self::artifact_prefix(git_remote, repo_id, ref_name, ref_type, commit_hash)?
        ))
    }

    pub fn compile_output_key<RepoId, Remote>(
        git_remote: &Remote,
        repo_id: RepoId,
        ref_name: &str,
        ref_type: SqsRefType,
        commit_hash: &str,
    ) -> BpResult<String>
    where
        RepoId: Display,
        Remote: GitHost<RepoId>,
    {
        Ok(format!(
            "{}compile-output.txt",
            Self::artifact_prefix(git_remote, repo_id, ref_name, ref_type, commit_hash)?
        ))
    }
}

impl S3Keys {
    fn sanitize_path(value: &str) -> BpResult<String> {
        if Self::is_valid_ref_name(value) {
            Ok(value.to_string())
        } else {
            Err(BpError::Validation(ValidationError::InvalidPath {
                value: value.to_string(),
            }))?
        }
    }

    fn is_valid_ref_name(value: &str) -> bool {
        !value.is_empty()
            && value != "@"
            && !value.starts_with('/')
            && !value.ends_with('/')
            && !value.ends_with('.')
            && !value.contains("//")
            && !value.contains("..")
            && !value.contains("@{")
            && value.split('/').all(|segment| {
                !segment.is_empty() && !segment.starts_with('.') && !segment.ends_with(".lock")
            })
            && value.chars().all(|character| {
                !character.is_ascii_control()
                    && character != ' '
                    && !matches!(character, '~' | '^' | ':' | '?' | '*' | '[' | '\\')
            })
    }
}
