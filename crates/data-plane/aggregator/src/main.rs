mod shard_writer;
mod supervisor;
mod tasks;

use platform_aggregator::{AggregatorConfig, ArchiveRecoveryConfig, S3ArchiveRecovery};
use platform_config::EnvVars;
use platform_errors::PlatformResult;
use platform_runtime::{HealthRunner, Service};
use std::sync::Arc;
use tasks::Tasks;

const SERVICE: Service = Service::new("platform-aggregator");

#[tokio::main]
async fn main() -> PlatformResult<()> {
    let vars = EnvVars::from_env();
    let config = AggregatorConfig::from_vars(&vars)?;
    let archive_recovery =
        ArchiveRecoveryConfig::from_vars_for_policy(&vars, config.replay_start_policy)?
            .map(S3ArchiveRecovery::new)
            .map(Arc::new);
    SERVICE.init_logging(&config.log_filter)?;
    let _metrics = SERVICE.init_metrics("platform/aggregator")?;
    let health = HealthRunner::from_service_env("AGGREGATOR").await?;
    let state = health.state().clone();
    health
        .run(Tasks::run_with_health(config, archive_recovery, state))
        .await
}
