mod config;
mod invoker;
mod run_request;
mod runner;

use crate::{config::RunInvokerConfig, invoker::RunInvoker, runner::Runner};
use platform_aws::{Aws, PresignedPostSigner};
use platform_errors::{BuildError, BuildResult, StorageError};
use platform_run_store::RunStore;
use platform_runtime::Service;

const SERVICE: Service = Service::new("platform-run-invoker");

#[tokio::main]
async fn main() -> BuildResult<()> {
    let config = RunInvokerConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    let _metrics = SERVICE.init_metrics("platform/run-invoker")?;
    let sdk_config = Aws::load_sdk_config(config.aws_timeouts).await;
    let store = RunStore::new(Aws::dynamodb_client(&sdk_config), config.table.clone());
    let s3 = Aws::s3_client(&sdk_config);
    let credentials_provider = sdk_config
        .credentials_provider()
        .ok_or(BuildError::Storage(StorageError::MissingCredentials))?;
    let signer = PresignedPostSigner::from_sdk_config(&sdk_config, config.logs_bucket.clone());
    let runner = Runner::new(
        Aws::lambda_client(&sdk_config),
        config.runner_function_name.clone(),
    );
    RunInvoker::new(config, store, s3, runner, signer, credentials_provider)
        .run()
        .await
}
