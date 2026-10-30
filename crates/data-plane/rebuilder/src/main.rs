use platform_errors::{BpError, BpResult};
use platform_rebuilder::config::RebuildConfig;
use platform_runtime::{Runtime, Service};

const SERVICE: Service = Service::new("platform-rebuilder");

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = RebuildConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    tokio::select! {
        result = platform_rebuilder::run(config) => result,
        signal = Runtime::shutdown_signal() => {
            signal?;
            Err(BpError::external(
                "rebuild interrupted by shutdown before completion",
            ))
        }
    }
}
