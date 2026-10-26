use crate::aws_ops::build_request::BuildRequest;
use aws_sdk_lambda::{Client as LambdaClient, primitives::Blob, types::InvocationType};
use platform_errors::{BpError, BpResult, TimeoutError};
use platform_time::{deadline::Deadline, timeouts::Timeouts};

pub struct LambdaConfig {
    pub function_name: String,
}

impl LambdaConfig {
    pub fn new(function_name: String) -> Self {
        Self { function_name }
    }
}

pub struct Lambda {
    client: LambdaClient,
    config: LambdaConfig,
    timeouts: Timeouts,
}

impl Lambda {
    pub fn new(client: LambdaClient, timeouts: Timeouts, config: LambdaConfig) -> Self {
        Self {
            client,
            config,
            timeouts,
        }
    }

    pub async fn run_build(&self, request: BuildRequest, deadline: Deadline) -> BpResult<()> {
        let payload = request.payload()?;
        tracing::info!(
            function = %self.config.function_name,
            "Invoking build agent Lambda"
        );
        let timeout = self.timeouts.operation_timeout(deadline)?;
        tokio::time::timeout(
            timeout,
            self.client
                .invoke()
                .function_name(self.config.function_name.as_str())
                .invocation_type(InvocationType::Event)
                .payload(Blob::new(payload))
                .send(),
        )
        .await
        .map_err(|_| {
            BpError::Timeout(TimeoutError::Operation {
                operation: "Lambda invoke",
            })
        })?
        .map_err(|error| error.into_service_error())?;
        Ok(())
    }
}
