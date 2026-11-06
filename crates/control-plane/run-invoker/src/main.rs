mod config;
mod invoker;
mod presigned_post;
mod process;
mod run_request;
#[cfg(test)]
mod tests;

use crate::{config::RunInvokerConfig, invoker::RunInvoker, presigned_post::PresignedPostSigner};
use platform_aws::Aws;
use platform_errors::{BpError, BpResult, StorageError};
use platform_run_store::RunStore;
use platform_runtime::Service;

const SERVICE: Service = Service::new("platform-run-invoker");

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = RunInvokerConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    let sdk_config = Aws::load_sdk_config(config.aws_timeouts).await;
    let store = RunStore::new(Aws::dynamodb_client(&sdk_config), config.table.clone());
    let s3 = Aws::s3_client(&sdk_config);
    let credentials_provider = sdk_config
        .credentials_provider()
        .ok_or(BpError::Storage(StorageError::MissingCredentials))?;
    let signer = PresignedPostSigner::from_sdk_config(&sdk_config, config.logs_bucket.clone());
    RunInvoker::new(config, store, s3, signer, credentials_provider)
        .run()
        .await
}
