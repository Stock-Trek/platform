use crate::{config::AuthRelayConfig, relay::AuthRelay, runner::AuthRelayRunner};
use platform_errors::BuildResult;
use platform_runtime::Service;

const SERVICE: Service = Service::new("platform-auth-relay");

pub struct AuthRelayApp;

impl AuthRelayApp {
    pub async fn run() -> BuildResult<()> {
        let config = AuthRelayConfig::from_env()?;
        SERVICE.init_logging(&config.log_filter)?;
        let _metrics = SERVICE.init_metrics("platform/auth-relay")?;
        let relay = AuthRelay::connect(&config).await?;
        AuthRelayRunner::new(config, relay).run().await
    }
}
