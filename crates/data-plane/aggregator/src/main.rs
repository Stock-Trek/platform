mod shard_writer;
mod supervisor;
mod tasks;

use platform_aggregator::{AggregatorConfig, ArchiveRecoveryConfig, S3ArchiveRecovery};
use platform_config::EnvVars;
use platform_errors::BuildResult;
use platform_runtime::Service;
use std::sync::Arc;
use tasks::Tasks;

const SERVICE: Service = Service::new("platform-aggregator");

#[tokio::main]
async fn main() -> BuildResult<()> {
    let vars = EnvVars::from_env();
    let config = AggregatorConfig::from_vars(&vars)?;
    let archive_recovery =
        ArchiveRecoveryConfig::from_vars_for_policy(&vars, config.replay_start_policy)?
            .map(S3ArchiveRecovery::new)
            .map(Arc::new);
    SERVICE.init_logging(&config.log_filter)?;
    let _metrics = SERVICE.init_metrics("platform/aggregator")?;
    Tasks::run(config, archive_recovery).await
}
