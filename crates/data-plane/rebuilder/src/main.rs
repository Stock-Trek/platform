use platform_errors::{PlatformError, PlatformResult};
use platform_rebuilder::RebuildConfig;
use platform_runtime::{Runtime, Service};

const SERVICE: Service = Service::new("platform-rebuilder");

#[tokio::main]
async fn main() -> PlatformResult<()> {
    let config = RebuildConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    let _metrics = SERVICE.init_metrics("platform/rebuilder")?;
    tokio::select! {
        result = config.run() => result,
        signal = Runtime::shutdown_signal() => {
            signal?;
            Err(PlatformError::external(
                "rebuild interrupted by shutdown before completion",
            ))
        }
    }
}
