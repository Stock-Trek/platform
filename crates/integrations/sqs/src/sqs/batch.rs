use aws_sdk_sqs::{error::ProvideErrorMetadata, types::SendMessageBatchRequestEntry};
use platform_errors::{BuildResult, SqsBatchFailure};
use platform_messages::SqsMessage;
use platform_metrics::sqs::SqsMetrics;
use platform_time::deadline_budget::Deadline;
use serde::Serialize;
use std::time::Duration;

use super::Sqs;

impl Sqs {
    /// Sends every message, splitting them into as many service batches as the
    /// entry and payload limits require.
    ///
    /// Every batch is attempted even when an earlier one fails, so a failure
    /// does not leave later messages unsent. Once the deadline has passed no
    /// further batches are attempted and the messages in them are reported as
    /// unsent. If any message could not be sent, the returned error names each
    /// message and the reason it failed.
    pub async fn send_batch(&self, messages: &[SqsMessage], deadline: Deadline) -> BuildResult<()> {
        let bodies = Self::serialize(messages)?;
        self.send_bodies(&bodies, Some(messages), deadline).await
    }

    /// Serializes and sends arbitrary JSON messages with the same batching,
    /// retry, timeout, and failure reporting as [`Self::send_batch`].
    pub async fn send_json<T>(&self, messages: &[T], deadline: Deadline) -> BuildResult<()>
    where
        T: Serialize + Sync,
    {
        let bodies = Self::serialize(messages)?;
        self.send_bodies(&bodies, None, deadline).await
    }

    async fn send_bodies(
        &self,
        bodies: &[String],
        sources: Option<&[SqsMessage]>,
        deadline: Deadline,
    ) -> BuildResult<()> {
        let batches = Self::batch_indices(bodies)
            .inspect_err(|_| SqsMetrics::record_messages_failed(bodies.len()))?;
        let mut failures = Vec::new();
        for (position, batch) in batches.iter().enumerate() {
            let timeout = match self.timeouts.operation_timeout(deadline) {
                Ok(timeout) => timeout,
                Err(error) => {
                    for remaining in &batches[position..] {
                        failures.extend(Self::batch_failures(
                            sources,
                            remaining,
                            "DeadlineExceeded",
                            error.to_string(),
                        ));
                    }
                    break;
                }
            };
            let batch_failures = self.send_batch_chunk(bodies, sources, batch, timeout).await;
            failures.extend(
                self.retry_failed_individually(bodies, sources, batch_failures, deadline)
                    .await,
            );
        }
        Self::finish(bodies.len(), failures)
    }

    async fn send_batch_chunk(
        &self,
        bodies: &[String],
        sources: Option<&[SqsMessage]>,
        indices: &[usize],
        timeout: Duration,
    ) -> Vec<SqsBatchFailure> {
        let entries = match indices
            .iter()
            .map(|&index| {
                SendMessageBatchRequestEntry::builder()
                    .id(index.to_string())
                    .message_body(&bodies[index])
                    .build()
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(entries) => entries,
            Err(error) => {
                return Self::batch_failures(sources, indices, "BpError", error.to_string());
            }
        };
        let result = tokio::time::timeout(
            timeout,
            self.client
                .send_message_batch()
                .queue_url(&self.config.queue_url)
                .set_entries(Some(entries))
                .send(),
        )
        .await;
        match result {
            Ok(Ok(output)) => output
                .failed()
                .iter()
                .map(|failure| {
                    let index = failure.id().parse::<usize>().ok();
                    SqsBatchFailure {
                        index,
                        source: index.and_then(|index| Self::source(sources, index)),
                        code: failure.code().to_owned(),
                        detail: failure.message().map(str::to_owned),
                    }
                })
                .collect(),
            Ok(Err(error)) => {
                let error = error.into_service_error();
                let code = error.code().unwrap_or("RequestFailure").to_owned();
                Self::batch_failures(sources, indices, &code, error.to_string())
            }
            Err(_) => Self::batch_failures(
                sources,
                indices,
                "RequestTimeout",
                "SQS send_message_batch timed out".to_owned(),
            ),
        }
    }
}
