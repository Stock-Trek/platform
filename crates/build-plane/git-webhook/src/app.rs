use crate::{config::WebhookConfig, context::RsWebhookContext, webhook_handler::WebhookHandler};
use lambda_http::{Request, service_fn};
use metrics_cloudwatch_embedded::lambda::service::run_http;
use platform_errors::{PlatformError, PlatformResult};
use platform_runtime::Service;
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-git-webhook");

pub struct GitWebhookApp;

impl GitWebhookApp {
    pub async fn run() -> PlatformResult<()> {
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
            .map_err(PlatformError::external)
    }
}
