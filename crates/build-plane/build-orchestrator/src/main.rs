use crate::{config::OrchestratorConfig, context::RsOrchestratorContext, sqs_handler::SqsHandler};
use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::{LambdaEvent, service_fn};
use metrics_cloudwatch_embedded::lambda::service::run;
use platform_errors::{BpError, BpResult};
use platform_runtime::Service;
use std::sync::Arc;

mod aws_ops;
mod config;
mod context;
mod sqs_handler;
mod tasks;

const SERVICE: Service = Service::new("platform-build-orchestrator");

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = OrchestratorConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    let recorder = SERVICE
        .init_lambda_metrics("platform/orchestrator", config.lambda_function_name.clone())?;
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
