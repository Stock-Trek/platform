use platform_errors::{BuildError, BuildResult};
use platform_rebuilder::config::RebuildConfig;
use platform_runtime::{Runtime, Service};

const SERVICE: Service = Service::new("platform-rebuilder");

#[tokio::main]
async fn main() -> BuildResult<()> {
    let config = RebuildConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    let _metrics = SERVICE.init_metrics("platform/rebuilder")?;
    tokio::select! {
        result = config.run() => result,
        signal = Runtime::shutdown_signal() => {
            signal?;
            Err(BuildError::external(
                "rebuild interrupted by shutdown before completion",
            ))
        }
    }
}
