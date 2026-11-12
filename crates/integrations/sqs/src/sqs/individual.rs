use aws_sdk_sqs::error::ProvideErrorMetadata;
use platform_errors::{BuildResult, SqsBatchFailure};
use platform_messages::SqsMessage;
use platform_time::deadline::Deadline;
use std::time::Duration;

use super::Sqs;

impl Sqs {
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
    ) -> BuildResult<()> {
        let bodies = Self::serialize(messages)?;
        self.send_individually_bodies(&bodies, Some(messages), deadline)
            .await
    }

    async fn send_individually_bodies(
        &self,
        bodies: &[String],
        sources: Option<&[SqsMessage]>,
        deadline: Deadline,
    ) -> BuildResult<()> {
        let mut failures = Vec::new();
        for (index, body) in bodies.iter().enumerate() {
            let timeout = match self.timeouts.operation_timeout(deadline) {
                Ok(timeout) => timeout,
                Err(error) => {
                    failures.extend(bodies[index..].iter().enumerate().map(|(offset, _)| {
                        SqsBatchFailure {
                            index: Some(index + offset),
                            source: Self::source(sources, index + offset),
                            code: "DeadlineExceeded".to_owned(),
                            detail: Some(error.to_string()),
                        }
                    }));
                    break;
                }
            };
            let source = sources.and_then(|sources| sources.get(index));
            if let Some(failure) = self.send_one(index, source, body, timeout).await {
                failures.push(failure);
            }
        }
        Self::finish(bodies.len(), failures)
    }

    pub(super) async fn retry_failed_individually(
        &self,
        bodies: &[String],
        sources: Option<&[SqsMessage]>,
        failures: Vec<SqsBatchFailure>,
        deadline: Deadline,
    ) -> Vec<SqsBatchFailure> {
        let mut remaining = Vec::new();
        for failure in failures {
            if failure.is_permanent() {
                remaining.push(failure);
                continue;
            }
            let Some(index) = failure.index.filter(|&index| index < bodies.len()) else {
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
            let source = sources.and_then(|sources| sources.get(index));
            if let Some(failure) = self.send_one(index, source, &bodies[index], timeout).await {
                remaining.push(failure);
            }
        }
        remaining
    }

    async fn send_one(
        &self,
        index: usize,
        source: Option<&SqsMessage>,
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
                    source: source.cloned(),
                    code,
                    detail: Some(error.to_string()),
                })
            }
            Err(_) => Some(SqsBatchFailure {
                index: Some(index),
                source: source.cloned(),
                code: "RequestTimeout".to_owned(),
                detail: Some(format!("SQS send_message timed out for entry {index}")),
            }),
        }
    }
}
