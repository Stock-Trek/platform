use crate::{config::OrchestratorConfig, context::RsOrchestratorContext, sqs_handler::SqsHandler};
use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::{LambdaEvent, service_fn};
use metrics_cloudwatch_embedded::lambda::service::run;
use platform_config::tracing_setup::TracingSetup;
use platform_errors::{BpError, BpResult};
use platform_metrics::descriptions::DescriptionsMetrics;
use std::sync::Arc;

mod aws_ops;
mod config;
mod context;
mod sqs_handler;
mod tasks;

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = OrchestratorConfig::from_env()?;
    TracingSetup::setup(&config.log_filter)?;
    let recorder = metrics_cloudwatch_embedded::Builder::new()
        .cloudwatch_namespace("platform/orchestrator")
        .with_dimension("function", config.lambda_function_name.clone())
        .lambda_cold_start_metric(platform_metrics::descriptions::COLD_STARTS_TOTAL)
        .init()?;
    DescriptionsMetrics::describe();
    let context = Arc::new(RsOrchestratorContext::new(&config).await?);
    let handler = {
        move |event: LambdaEvent<SqsEvent>| {
            let context = context.clone();
            async move { SqsHandler.handle(&context, event).await }
        }
    };
    run(recorder, service_fn(handler))
        .await
        .map_err(BpError::external)
}
