use aws_sdk_sqs::{
    Client as SqsClient, error::ProvideErrorMetadata, types::SendMessageBatchRequestEntry,
};
use platform_errors::{BpError, BpResult, SqsBatchFailure, SqsError};
use platform_messages::sqs_message::{SQS_MAX_BATCH_BYTES, SqsMessage};
use platform_metrics::sqs::SqsMetrics;
use platform_time::{deadline::Deadline, timeouts::Timeouts};
use std::time::Duration;

/// The most entries SQS accepts in a single `SendMessageBatch` request.
const SQS_MAX_BATCH_ENTRIES: usize = 10;
#[derive(Clone)]
pub struct SqsConfig {
    pub queue_url: String,
}

impl SqsConfig {
    pub fn new(queue_url: String) -> Self {
        Self { queue_url }
    }
}

#[derive(Clone)]
pub struct Sqs {
    client: SqsClient,
    config: SqsConfig,
    timeouts: Timeouts,
}

impl Sqs {
    pub fn new(client: SqsClient, timeouts: Timeouts, config: SqsConfig) -> Self {
        Self {
            client,
            config,
            timeouts,
        }
    }

    /// Sends every message, splitting them into as many service batches as the
    /// entry and payload limits require.
    ///
    /// Every batch is attempted even when an earlier one fails, so a failure
    /// does not leave later messages unsent. Once the deadline has passed no
    /// further batches are attempted and the messages in them are reported as
    /// unsent. If any message could not be sent, the returned error names each
    /// message and the reason it failed.
    pub async fn send_batch(&self, messages: &[SqsMessage], deadline: Deadline) -> BpResult<()> {
        let bodies = messages
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .inspect_err(|_| SqsMetrics::record_messages_failed(messages.len()))?;
        let batches = Self::batch_indices(&bodies)
            .inspect_err(|_| SqsMetrics::record_messages_failed(messages.len()))?;
        let mut failures = Vec::new();
        for (position, batch) in batches.iter().enumerate() {
            let timeout = match self.timeouts.operation_timeout(deadline) {
                Ok(timeout) => timeout,
                Err(error) => {
                    for remaining in &batches[position..] {
                        failures.extend(Self::batch_failures(
                            messages,
                            remaining,
                            "DeadlineExceeded",
                            error.to_string(),
                        ));
                    }
                    break;
                }
            };
            let batch_failures = self
                .send_batch_chunk(messages, &bodies, batch, timeout)
                .await;
            failures.extend(
                self.retry_failed_individually(messages, &bodies, batch_failures, deadline)
                    .await,
            );
        }
        if failures.is_empty() {
            SqsMetrics::record_messages_sent(messages.len());
            Ok(())
        } else {
            let unsent = failures.len();
            let sent = messages.len() - unsent;
            SqsMetrics::record_messages_sent(sent);
            SqsMetrics::record_messages_failed(unsent);
            Err(BpError::Sqs(SqsError::PartialBatch {
                sent,
                unsent,
                failures,
            }))
        }
    }

    async fn send_batch_chunk(
        &self,
        messages: &[SqsMessage],
        bodies: &[String],
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
                return Self::batch_failures(messages, indices, "BpError", error.to_string());
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
                        source: index.and_then(|index| messages.get(index).cloned()),
                        code: failure.code().to_owned(),
                        detail: failure.message().map(str::to_owned),
                    }
                })
                .collect(),
            Ok(Err(error)) => {
                let error = error.into_service_error();
                let code = error.code().unwrap_or("RequestFailure").to_owned();
                Self::batch_failures(messages, indices, &code, error.to_string())
            }
            Err(_) => Self::batch_failures(
                messages,
                indices,
                "RequestTimeout",
                "SQS send_message_batch timed out".to_owned(),
            ),
        }
    }

    /// Sends each message with an individual `SendMessage` call.
    ///
    /// This is used to drain the unsent remainder of a partially failed batch.
    /// Every message is attempted even when an earlier one fails, so a single
    /// failure does not leave later messages unsent. If any message could not
    /// be sent, the returned error names each message and the reason it failed.
    pub async fn send_individually(
        &self,
        messages: &[SqsMessage],
        deadline: Deadline,
    ) -> BpResult<()> {
        let bodies = messages
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .inspect_err(|_| SqsMetrics::record_messages_failed(messages.len()))?;
        let mut failures = Vec::new();
        for (index, (message, body)) in messages.iter().zip(&bodies).enumerate() {
            let timeout = match self.timeouts.operation_timeout(deadline) {
                Ok(timeout) => timeout,
                Err(error) => {
                    failures.extend(messages[index..].iter().enumerate().map(
                        |(offset, message)| SqsBatchFailure {
                            index: Some(index + offset),
                            source: Some(message.clone()),
                            code: "DeadlineExceeded".to_owned(),
                            detail: Some(error.to_string()),
                        },
                    ));
                    break;
                }
            };
            if let Some(failure) = self.send_one(index, message, body, timeout).await {
                failures.push(failure);
            }
        }
        if failures.is_empty() {
            SqsMetrics::record_messages_sent(messages.len());
            Ok(())
        } else {
            let unsent = failures.len();
            let sent = messages.len() - unsent;
            SqsMetrics::record_messages_sent(sent);
            SqsMetrics::record_messages_failed(unsent);
            Err(BpError::Sqs(SqsError::PartialBatch {
                sent,
                unsent,
                failures,
            }))
        }
    }

    async fn retry_failed_individually(
        &self,
        messages: &[SqsMessage],
        bodies: &[String],
        failures: Vec<SqsBatchFailure>,
        deadline: Deadline,
    ) -> Vec<SqsBatchFailure> {
        let mut remaining = Vec::new();
        for failure in failures {
            if failure.is_permanent() {
                remaining.push(failure);
                continue;
            }
            let Some(index) = failure.index.filter(|&index| index < messages.len()) else {
                remaining.push(failure);
                continue;
            };
            let timeout = match self.timeouts.operation_timeout(deadline) {
                Ok(timeout) => timeout,
                Err(_) => {
                    remaining.push(failure);
                    continue;
                }
            };
            if let Some(failure) = self
                .send_one(index, &messages[index], &bodies[index], timeout)
                .await
            {
                remaining.push(failure);
            }
        }
        remaining
    }

    async fn send_one(
        &self,
        index: usize,
        message: &SqsMessage,
        body: &str,
        timeout: Duration,
    ) -> Option<SqsBatchFailure> {
        let result = tokio::time::timeout(
            timeout,
            self.client
                .send_message()
                .queue_url(&self.config.queue_url)
                .message_body(body)
                .send(),
        )
        .await;
        match result {
            Ok(Ok(_)) => None,
            Ok(Err(error)) => {
                let error = error.into_service_error();
                let code = error.code().unwrap_or("RequestFailure").to_owned();
                Some(SqsBatchFailure {
                    index: Some(index),
                    source: Some(message.clone()),
                    code,
                    detail: Some(error.to_string()),
                })
            }
            Err(_) => Some(SqsBatchFailure {
                index: Some(index),
                source: Some(message.clone()),
                code: "RequestTimeout".to_owned(),
                detail: Some(format!("SQS send_message timed out for entry {index}")),
            }),
        }
    }

    fn batch_failures(
        messages: &[SqsMessage],
        indices: &[usize],
        code: &str,
        detail: String,
    ) -> Vec<SqsBatchFailure> {
        indices
            .iter()
            .map(|&index| SqsBatchFailure {
                index: Some(index),
                source: messages.get(index).cloned(),
                code: code.to_owned(),
                detail: Some(detail.clone()),
            })
            .collect()
    }

    /// Splits the serialized message bodies into batches that respect both the
    /// entry count and the total payload size limits.
    fn batch_indices(bodies: &[String]) -> BpResult<Vec<Vec<usize>>> {
        let mut batches = Vec::new();
        let mut current = Vec::new();
        let mut current_bytes = 0;
        for (index, body) in bodies.iter().enumerate() {
            let size = body.len();
            if size > SQS_MAX_BATCH_BYTES {
                return Err(BpError::Sqs(SqsError::MessageTooLarge {
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
