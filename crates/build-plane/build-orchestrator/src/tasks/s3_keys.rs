use platform_errors::{BpError, BpResult, ValidationError};
use platform_git::git_host::GitHost;
use platform_messages::sqs_message::SqsRefType;
use std::fmt::Display;

const SOURCE_PREFIX: &str = "source/";
const ARTIFACT_PREFIX: &str = "artifact/";

pub struct S3Keys {
    ref_key: String,
}

impl S3Keys {
    pub fn new<RepoId, Remote>(
        git_remote: &Remote,
        repo_id: RepoId,
        ref_name: &str,
        ref_type: SqsRefType,
        commit_hash: &str,
    ) -> BpResult<Self>
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
        Ok(Self {
            ref_key: format!(
                "{}/{}/{}/{}/{}/{}/",
                git_remote.provider(),
                git_remote.host(),
                repo,
                ref_type_name,
                ref_name,
                commit_hash
            ),
        })
    }

    pub fn ref_key(&self) -> &str {
        &self.ref_key
    }

    pub fn source_prefix(&self) -> String {
        format!("{}{SOURCE_PREFIX}", self.ref_key())
    }

    pub fn source_key(&self) -> String {
        format!("{}algorithms.zip", self.source_prefix())
    }

    pub fn artifact_prefix(&self) -> String {
        format!("{}{ARTIFACT_PREFIX}", self.ref_key())
    }

    pub fn artifact_key(&self) -> String {
        format!("{}runner.cwasm", self.artifact_prefix())
    }

    pub fn compile_output_key(&self) -> String {
        format!("{}compile-output.txt", self.artifact_prefix())
    }

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
