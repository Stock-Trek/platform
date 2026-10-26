use crate::retryability::Retryability;
use aws_sdk_lambda::operation::invoke::InvokeError as SdkInvokeError;

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum LambdaError {
    #[error("Lambda function returned {function_error}: {detail}", detail = .detail.as_deref().unwrap_or("no detail"))]
    Function {
        function_error: String,
        detail: Option<String>,
    },
    #[error("Lambda invoke error: {0}")]
    SdkInvoke(#[from] SdkInvokeError),
    #[error("Lambda invoke returned unexpected status code {status_code}")]
    Status { status_code: i32 },
    #[error("build failed with {error_type}: {message}")]
    BuildFailed {
        error_type: String,
        message: String,
        retryable: bool,
    },
    #[error("Lambda response did not contain a build outcome")]
    MissingPayload,
    #[error("Lambda response contained an invalid build outcome: {source}")]
    InvalidPayload {
        #[source]
        source: serde_json::Error,
    },
}

impl LambdaError {
    pub fn error_type(&self) -> &'static str {
        self.into()
    }

    pub fn retryability(&self) -> Retryability {
        match self {
            Self::SdkInvoke(error) => Retryability::from_lambda_invoke(error),
            Self::Function { .. } => Retryability::Retryable,
            Self::BuildFailed { retryable, .. } => {
                if *retryable {
                    Retryability::Retryable
                } else {
                    Retryability::Permanent
                }
            }
            Self::Status { .. } | Self::MissingPayload | Self::InvalidPayload { .. } => {
                Retryability::Permanent
            }
        }
    }
}
