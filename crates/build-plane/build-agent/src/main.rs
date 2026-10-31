use crate::{config::AgentConfig, handler::Handler, source::SourceLimits};
use lambda_runtime::{LambdaEvent, service_fn};
use metrics_cloudwatch_embedded::lambda::service::run;
use platform_errors::{BpError, BpResult, ErrorType};
use platform_messages::build_request::{BuildOutcome, BuildRequest};
use platform_metrics::build::BuildMetrics;
use platform_runtime::Service;
use std::time::{Instant, SystemTime};

mod artifacts;
mod config;
mod handler;
mod s3;
mod source;

const SERVICE: Service = Service::new("platform-build-agent");

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = AgentConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    let recorder =
        SERVICE.init_lambda_metrics("platform/agent", config.lambda_function_name.clone())?;
    let source_limits = SourceLimits::new(
        config.source_download_timeout,
        config.max_source_archive_size_bytes,
        config.max_extracted_source_size_bytes,
    );
    let handler = Handler::with_source_limits(
        config.build_timeout,
        config.failure_publish_budget,
        config.source_dir,
        config.build_cache_dir,
        source_limits,
    );
    run(
        recorder,
        service_fn(move |event| {
            let handler = handler.clone();
            async move { BuildAgent::handle(handler, event).await }
        }),
    )
    .await
    .map_err(BpError::external)
}

struct BuildAgent;

impl BuildAgent {
    async fn handle(handler: Handler, event: LambdaEvent<BuildRequest>) -> BpResult<BuildOutcome> {
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

    fn handle_error(error: BpError, started_at: Instant) -> BpResult<BuildOutcome> {
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

#[cfg(test)]
mod tests;
