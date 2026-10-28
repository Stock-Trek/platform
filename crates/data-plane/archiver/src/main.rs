mod config;
mod runner;
mod sink;
mod source;

use crate::{config::ArchiverConfig, runner::ArchiverRunner};
use platform_errors::BpResult;

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = ArchiverConfig::from_env()?;
    platform_runtime::Runtime::init_logging(&config.log_filter)?;
    ArchiverRunner::new(config).run().await
}
