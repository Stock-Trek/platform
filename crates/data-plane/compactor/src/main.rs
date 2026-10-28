mod compactor;
mod config;
mod partition_output;
mod runner;
mod source;
mod spill;

use crate::{config::CompactorConfig, runner::CompactorRunner};
use platform_errors::BpResult;

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = CompactorConfig::from_env()?;
    platform_runtime::Runtime::init_logging(&config.log_filter)?;
    CompactorRunner::new(config).run().await
}
