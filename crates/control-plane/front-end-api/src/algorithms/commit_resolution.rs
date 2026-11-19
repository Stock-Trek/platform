use sha1::{Digest, Sha1};

const FULL_COMMIT_LENGTH: usize = 40;

#[derive(Debug)]
pub(crate) enum CommitError {
    Unresolved,
}

pub(super) struct KnownCommits(Vec<String>);

impl KnownCommits {
    pub(super) fn new() -> Self {
        Self(Vec::new())
    }

    pub(super) fn resolve(
        &mut self,
        commit: Option<&str>,
        algorithm_id: &str,
        git_ref: &str,
    ) -> Result<String, CommitError> {
        let full = match commit {
            None => Self::head_commit(algorithm_id, git_ref),
            Some(candidate) if candidate.len() == FULL_COMMIT_LENGTH => candidate.to_string(),
            Some(candidate) => self.by_prefix(candidate)?,
        };
        if !self.0.contains(&full) {
            self.0.push(full.clone());
        }
        Ok(full)
    }

    fn by_prefix(&self, candidate: &str) -> Result<String, CommitError> {
        let matches: Vec<&String> = self
            .0
            .iter()
            .filter(|full| full.starts_with(candidate))
            .collect();
        match matches.as_slice() {
            [full] => Ok((*full).clone()),
            _ => Err(CommitError::Unresolved),
        }
    }

    fn head_commit(algorithm_id: &str, git_ref: &str) -> String {
        let digest = Sha1::digest(format!("{algorithm_id} {git_ref}").as_bytes());
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
