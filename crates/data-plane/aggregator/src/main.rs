mod shard_writer;
mod supervisor;
mod tasks;

use platform_aggregator::{AggregatorConfig, ArchiveRecoveryConfig, S3ArchiveRecovery};
use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use platform_runtime::Service;
use std::sync::Arc;
use tasks::Tasks;

const SERVICE: Service = Service::new("platform-aggregator");

#[tokio::main]
async fn main() -> BpResult<()> {
    let vars = EnvVars::from_env();
    let config = AggregatorConfig::from_vars(&vars)?;
    let archive_recovery =
        ArchiveRecoveryConfig::from_vars_for_policy(&vars, config.replay_start_policy)?
            .map(S3ArchiveRecovery::new)
            .map(Arc::new);
    SERVICE.init_logging(&config.log_filter)?;
    Tasks::run(config, archive_recovery).await
}
