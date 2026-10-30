use serde::{Deserialize, Serialize};
use strum::Display;

/// The most payload bytes (the sum of the message bodies) SQS accepts in a
/// single `SendMessageBatch` request.
pub const SQS_MAX_BATCH_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SqsMessage {
    pub source: GitSource,
    pub detail: SqsDetail,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SqsDetail {
    AllReposAdd,
    MultiRepoAdd {
        repo_ids: Vec<String>,
    },
    RepoAdd {
        repo_id: String,
    },
    RefCreate {
        repo_id: String,
        ref_name: String,
        ref_type: SqsRefType,
    },
    Commit {
        repo_id: String,
        ref_name: String,
        ref_type: SqsRefType,
        commit_hash: String,
    },
}

#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum SqsRefType {
    Branch,
    Tag,
}

#[derive(Debug, Display, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum GitSource {
    GitHub {
        host: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery_id: Option<String>,
        installation_id: u64,
    },
}

impl GitSource {
    pub fn name(&self) -> &'static str {
        match self {
            GitSource::GitHub { .. } => "github",
        }
    }
    pub fn host(&self) -> &str {
        match self {
            GitSource::GitHub { host, .. } => host,
        }
    }
    pub fn delivery_id(&self) -> Option<&str> {
        match self {
            GitSource::GitHub { delivery_id, .. } => delivery_id.as_deref(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SqsMessageError {
    #[error(
        "SQS message body is {size} bytes, which exceeds the maximum batch payload of {limit} bytes"
    )]
    MessageTooLarge { size: usize, limit: usize },
    #[error(transparent)]
    Serialize(#[from] serde_json::Error),
}

impl SqsMessage {
    pub fn chunk(self) -> Result<Vec<SqsMessage>, SqsMessageError> {
        let SqsMessage { source, detail } = self;
        let SqsDetail::MultiRepoAdd { repo_ids } = &detail else {
            return Ok(vec![SqsMessage { source, detail }]);
        };
        if repo_ids.is_empty() {
            return Ok(vec![SqsMessage { source, detail }]);
        }
        let envelope = serde_json::to_string(&SqsMessage {
            source: source.clone(),
            detail: SqsDetail::MultiRepoAdd {
                repo_ids: Vec::new(),
            },
        })?
        .len();
        if envelope > SQS_MAX_BATCH_BYTES {
            return Err(SqsMessageError::MessageTooLarge {
                size: envelope,
                limit: SQS_MAX_BATCH_BYTES,
            });
        }
        let mut messages = Vec::new();
        let mut current: Vec<String> = Vec::new();
        let mut extra = 0;
        for repo_id in repo_ids {
            let encoded = serde_json::to_string(repo_id)?.len();
            if !current.is_empty() && envelope + extra + encoded + 1 > SQS_MAX_BATCH_BYTES {
                messages.push(SqsMessage {
                    source: source.clone(),
                    detail: SqsDetail::MultiRepoAdd {
                        repo_ids: std::mem::take(&mut current),
                    },
                });
                extra = 0;
            }
            let addition = if current.is_empty() {
                encoded
            } else {
                encoded + 1
            };
            if envelope + extra + addition > SQS_MAX_BATCH_BYTES {
                return Err(SqsMessageError::MessageTooLarge {
                    size: envelope + addition,
                    limit: SQS_MAX_BATCH_BYTES,
                });
            }
            extra += addition;
            current.push(repo_id.clone());
        }
        messages.push(SqsMessage {
            source,
            detail: SqsDetail::MultiRepoAdd { repo_ids: current },
        });
        Ok(messages)
    }
}
