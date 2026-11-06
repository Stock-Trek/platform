use crate::{config::AgentConfig, handler::BuildAgent, handler::Handler, source::SourceLimits};
use lambda_runtime::service_fn;
use metrics_cloudwatch_embedded::lambda::service::run;
use platform_errors::{BpError, BpResult};
use platform_runtime::Service;

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
