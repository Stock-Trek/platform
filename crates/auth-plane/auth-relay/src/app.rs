use crate::{config::AuthRelayConfig, relay::AuthRelay, runner::AuthRelayRunner};
use platform_errors::BpResult;
use platform_runtime::Service;

const SERVICE: Service = Service::new("platform-auth-relay");

pub struct AuthRelayApp;

impl AuthRelayApp {
    pub async fn run() -> BpResult<()> {
        let config = AuthRelayConfig::from_env()?;
        SERVICE.init_logging(&config.log_filter)?;
        let relay = AuthRelay::connect(&config).await?;
        AuthRelayRunner::new(config, relay).run().await
    }
}
