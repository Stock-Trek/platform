use crate::Handler;
use lambda_runtime::LambdaEvent;
use platform_errors::{BuildError, BuildResult, ErrorType};
use platform_messages::{BuildOutcome, BuildRequest};
use platform_metrics::BuildMetrics;
use std::time::{Instant, SystemTime};

pub struct BuildAgent;

impl BuildAgent {
    pub async fn handle(
        handler: Handler,
        event: LambdaEvent<BuildRequest>,
    ) -> BuildResult<BuildOutcome> {
        let started_at = Instant::now();
        let deadline = Instant::now()
            + event
                .context
                .deadline()
                .duration_since(SystemTime::now())
                .unwrap_or_default();
        match handler.handle(event.payload, deadline).await {
            Ok(()) => {
                BuildMetrics::record_success(started_at.elapsed());
                Ok(BuildOutcome::success())
            }
            Err(error) => Self::handle_error(error, started_at),
        }
    }

    pub fn handle_error(error: BuildError, started_at: Instant) -> BuildResult<BuildOutcome> {
        let duration = started_at.elapsed();
        if error.retryability().is_retryable() {
            BuildMetrics::record_retryable_failure(error.error_type(), duration);
            tracing::error!(
                error_type = error.error_type(),
                %error,
                "build failed with a retryable error"
            );
            // Returning Err marks the asynchronous invocation as failed, so
            // Lambda's retry policy retries the build. Only transient failures
            // should do this.
            return Err(error);
        }

        BuildMetrics::record_permanent_failure(error.error_type(), duration);
        tracing::error!(
            error_type = error.error_type(),
            %error,
            "build failed permanently"
        );
        // A permanent failure, such as a user compile error, cannot be fixed by
        // retrying. Return Ok with a structured failure payload so Lambda does
        // not retry the invocation. The payload is captured by the
        // asynchronous invocation destination and logs, not the orchestrator.
        Ok(BuildOutcome::failure(
            error.error_type(),
            error.to_string(),
            false,
        ))
    }
}
