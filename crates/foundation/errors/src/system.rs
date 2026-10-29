use crate::retryability::Retryability;
use platform_types::new_types::ArithmeticError;
use std::error::Error as StdError;

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum SystemError {
    #[error(transparent)]
    Arithmetic(#[from] ArithmeticError),
    #[error(transparent)]
    External(#[from] Box<dyn StdError + Send + Sync + 'static>),
    #[error("Concurrency semaphore was closed while waiting for a permit")]
    SemaphoreClosed,
    #[error("System Time was before epoch")]
    SystemTimeBeforeEpoch,
    #[error("Background task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
    #[error("failed to install tracing subscriber")]
    Tracing(#[from] tracing::subscriber::SetGlobalDefaultError),
}

impl SystemError {
    pub fn retryability(&self) -> Retryability {
        match self {
            Self::External(_) => Retryability::Retryable,
            Self::Task(error) => Retryability::from_join_error(error),
            Self::Arithmetic(_)
            | Self::Tracing(_)
            | Self::SystemTimeBeforeEpoch
            | Self::SemaphoreClosed => Retryability::Permanent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn panicked_task_is_permanent() {
        let error = tokio::spawn(async { panic!("task panicked") })
            .await
            .unwrap_err();

        assert_eq!(
            SystemError::Task(error).retryability(),
            Retryability::Permanent
        );
    }

    #[tokio::test]
    async fn cancelled_task_is_retryable() {
        let task = tokio::spawn(async { std::future::pending::<()>().await });
        task.abort();
        let error = task.await.unwrap_err();

        assert_eq!(
            SystemError::Task(error).retryability(),
            Retryability::Retryable
        );
    }
}
