mod config;
mod ingester;
mod publisher;
mod runner;
mod sequence;
mod source;

use crate::{config::IngesterConfig, runner::IngesterRunner};
use platform_errors::BpResult;

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = IngesterConfig::from_env()?;
    platform_runtime::Runtime::init_logging(&config.log_filter)?;
    IngesterRunner::new(config).run().await
}
