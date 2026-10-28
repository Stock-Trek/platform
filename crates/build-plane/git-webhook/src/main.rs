use crate::{config::WebhookConfig, context::RsWebhookContext, webhook_handler::WebhookHandler};
use lambda_http::{Request, service_fn};
use metrics_cloudwatch_embedded::lambda::service::run_http;
use platform_errors::{BpError, BpResult};
use platform_runtime::Service;
use std::sync::Arc;

mod config;
mod context;
mod dedup;
mod dto;
mod event_response;
mod infisical;
mod responses;
mod webhook_handler;

const SERVICE: Service = Service::new("platform-git-webhook");

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = WebhookConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    let recorder =
        SERVICE.init_lambda_metrics("platform/webhook", config.lambda_function_name.clone())?;
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
