use aws_sdk_lambda::{Client as LambdaClient, primitives::Blob, types::InvocationType};
use platform_errors::PlatformResult;

pub(crate) struct Runner {
    client: LambdaClient,
    function_name: String,
}

impl Runner {
    pub(crate) fn new(client: LambdaClient, function_name: String) -> Self {
        Self {
            client,
            function_name,
        }
    }

    pub(crate) async fn invoke(&self, request: &str) -> PlatformResult<()> {
        self.client
            .invoke()
            .function_name(&self.function_name)
            .invocation_type(InvocationType::Event)
            .payload(Blob::new(request.as_bytes().to_vec()))
            .send()
            .await
            .map_err(|error| error.into_service_error())?;
        Ok(())
    }
}
