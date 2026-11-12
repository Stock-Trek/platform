use crate::{config::AuthRelayConfig, relay::AuthRelay};
use platform_errors::BuildResult;
use tracing::info;

pub struct AuthRelayRunner {
    relay: AuthRelay,
    audit_queue_url: String,
}

impl AuthRelayRunner {
    pub fn new(config: AuthRelayConfig, relay: AuthRelay) -> Self {
        Self {
            relay,
            audit_queue_url: config.audit_queue_url,
        }
    }

    pub async fn run(self) -> BuildResult<()> {
        info!(
            audit_queue_url = %self.audit_queue_url,
            "auth relay drain started"
        );
        let delivered = self.relay.drain().await?;
        info!(delivered, "auth relay drain completed");
        Ok(())
    }
}
