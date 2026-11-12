use aws_sdk_sqs::Client as SqsClient;
use platform_errors::{BuildError, BuildResult, SqsError};
use platform_messages::SQS_MAX_BATCH_BYTES;
use platform_time::timeouts::Timeouts;

use crate::sqs::SqsConfig;

const SQS_MAX_BATCH_ENTRIES: usize = 10;

#[derive(Clone)]
pub struct Sqs {
    pub(super) client: SqsClient,
    pub(super) config: SqsConfig,
    pub(super) timeouts: Timeouts,
}

impl Sqs {
    pub fn new(client: SqsClient, timeouts: Timeouts, config: SqsConfig) -> Self {
        Self {
            client,
            config,
            timeouts,
        }
    }

    /// Splits the serialized message bodies into batches that respect both the
    /// entry count and the total payload size limits.
    pub(super) fn batch_indices(bodies: &[String]) -> BuildResult<Vec<Vec<usize>>> {
        let mut batches = Vec::new();
        let mut current = Vec::new();
        let mut current_bytes = 0;
        for (index, body) in bodies.iter().enumerate() {
            let size = body.len();
            if size > SQS_MAX_BATCH_BYTES {
                return Err(BuildError::Sqs(SqsError::MessageTooLarge {
                    size,
                    limit: SQS_MAX_BATCH_BYTES,
                }));
            }
            if !current.is_empty()
                && (current.len() == SQS_MAX_BATCH_ENTRIES
                    || current_bytes + size > SQS_MAX_BATCH_BYTES)
            {
                batches.push(std::mem::take(&mut current));
                current_bytes = 0;
            }
            current.push(index);
            current_bytes += size;
        }
        if !current.is_empty() {
            batches.push(current);
        }
        Ok(batches)
    }
}
