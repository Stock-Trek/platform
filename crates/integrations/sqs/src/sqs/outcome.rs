use platform_errors::{BuildError, BuildResult, SqsBatchFailure, SqsError};
use platform_messages::SqsMessage;
use platform_metrics::SqsMetrics;
use serde::Serialize;

use crate::sqs::Sqs;

impl Sqs {
    pub(super) fn serialize<T: Serialize>(messages: &[T]) -> BuildResult<Vec<String>> {
        messages
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .map_err(BuildError::from)
            .inspect_err(|_| SqsMetrics::record_messages_failed(messages.len()))
    }

    pub(super) fn batch_failures(
        sources: Option<&[SqsMessage]>,
        indices: &[usize],
        code: &str,
        detail: String,
    ) -> Vec<SqsBatchFailure> {
        indices
            .iter()
            .map(|&index| SqsBatchFailure {
                index: Some(index),
                source: Self::source(sources, index),
                code: code.to_owned(),
                detail: Some(detail.clone()),
            })
            .collect()
    }

    pub(super) fn source(sources: Option<&[SqsMessage]>, index: usize) -> Option<SqsMessage> {
        sources.and_then(|sources| sources.get(index).cloned())
    }

    pub(super) fn finish(message_count: usize, failures: Vec<SqsBatchFailure>) -> BuildResult<()> {
        if failures.is_empty() {
            SqsMetrics::record_messages_sent(message_count);
            Ok(())
        } else {
            let unsent = failures.len();
            let sent = message_count - unsent;
            SqsMetrics::record_messages_sent(sent);
            SqsMetrics::record_messages_failed(unsent);
            Err(BuildError::Sqs(SqsError::PartialBatch {
                sent,
                unsent,
                failures,
            }))
        }
    }
}
