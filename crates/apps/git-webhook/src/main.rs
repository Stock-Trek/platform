use crate::{config::WebhookConfig, context::RsWebhookContext, webhook_handler::WebhookHandler};
use lambda_http::{Request, service_fn};
use metrics_cloudwatch_embedded::lambda::service::run_http;
use platform_config::tracing_setup::TracingSetup;
use platform_errors::{BpError, BpResult};
use platform_metrics::descriptions::DescriptionsMetrics;
use std::sync::Arc;

mod aws;
mod config;
mod context;
mod dedup;
mod dto;
mod event_response;
mod infisical;
mod responses;
mod webhook_handler;

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = WebhookConfig::from_env()?;
    TracingSetup::setup(&config.log_filter)?;
    let recorder = metrics_cloudwatch_embedded::Builder::new()
        .cloudwatch_namespace("platform/webhook")
        .with_dimension("function", config.lambda_function_name.clone())
        .lambda_cold_start_metric(platform_metrics::descriptions::COLD_STARTS_TOTAL)
        .init()?;
    DescriptionsMetrics::describe();
    let context = Arc::new(RsWebhookContext::new(&config).await?);
    let handler = {
        move |event: Request| {
            let context = context.clone();
            async move { WebhookHandler.handle(&context, event).await }
        }
    };
    run_http(recorder, service_fn(handler))
        .await
        .map_err(BpError::external)
}
