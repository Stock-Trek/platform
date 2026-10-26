use crate::retryability::Retryability;
use platform_messages::sqs_message::SqsMessage;

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum SqsError {
    #[error(
        "SQS message body is {size} bytes, which exceeds the maximum batch payload of {limit} bytes"
    )]
    MessageTooLarge { size: usize, limit: usize },
    #[error("SQS record is missing a body")]
    MissingMessageBody,
    #[error("SQS record is missing a message id; cannot report a batch item failure")]
    MissingMessageId,
    #[error(
        "SQS PartialBatch sent {sent} message(s) and failed to send {unsent} message(s): {}",
        Self::format_batch_failures(.failures)
    )]
    PartialBatch {
        sent: usize,
        unsent: usize,
        failures: Vec<SqsBatchFailure>,
    },
}

impl SqsError {
    pub fn any_sent(&self) -> bool {
        matches!(self, Self::PartialBatch { sent, .. } if *sent > 0)
    }

    pub fn error_type(&self) -> &'static str {
        self.into()
    }

    pub fn retryability(&self) -> Retryability {
        match self {
            Self::PartialBatch { failures, .. }
                if !failures.is_empty() && failures.iter().all(SqsBatchFailure::is_permanent) =>
            {
                Retryability::Permanent
            }
            Self::PartialBatch { .. } => Retryability::Retryable,
            Self::MissingMessageId | Self::MissingMessageBody | Self::MessageTooLarge { .. } => {
                Retryability::Permanent
            }
        }
    }

    fn format_batch_failures(failures: &[SqsBatchFailure]) -> String {
        failures
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[derive(Debug)]
pub struct SqsBatchFailure {
    pub index: Option<usize>,
    pub source: Option<SqsMessage>,
    pub code: String,
    pub detail: Option<String>,
}

impl SqsBatchFailure {
    pub fn is_permanent(&self) -> bool {
        Retryability::sqs_batch_failure_retryability(&self.code) == Retryability::Permanent
    }
}

impl std::fmt::Display for SqsBatchFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "index={:?} source={:?} code={:?} detail={:?}",
            self.index, self.source, self.code, self.detail
        )
    }
}
