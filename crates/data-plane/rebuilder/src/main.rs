use platform_errors::BpResult;
use platform_rebuilder::config::RebuildConfig;
use platform_runtime::Service;

const SERVICE: Service = Service::new("platform-rebuilder");

#[tokio::main]
async fn main() -> BpResult<()> {
    let config = RebuildConfig::from_env()?;
    SERVICE.init_logging(&config.log_filter)?;
    platform_rebuilder::run(config).await
}
