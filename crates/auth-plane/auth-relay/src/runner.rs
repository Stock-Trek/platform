use crate::{config::AuthRelayConfig, relay::AuthRelay};
use platform_errors::BpResult;
use platform_runtime::Runtime;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{info, warn};

pub struct AuthRelayRunner {
    config: AuthRelayConfig,
    relay: AuthRelay,
}

impl AuthRelayRunner {
    pub fn new(config: AuthRelayConfig, relay: AuthRelay) -> Self {
        Self { config, relay }
    }

    pub async fn run(self) -> BpResult<()> {
        info!(
            poll_interval_secs = self.config.poll_interval.as_secs(),
            audit_queue_url = %self.config.audit_queue_url,
            "auth relay started"
        );
        let result = Runtime::run_until_shutdown(self.run_cycles()).await;
        self.drain_on_shutdown().await;
        result
    }

    async fn run_cycles(&self) -> BpResult<()> {
        let mut ticker = interval(self.config.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            self.drain_once().await;
        }
    }

    pub(crate) async fn drain_once(&self) {
        match self.relay.drain().await {
            Ok(delivered) if delivered > 0 => {
                info!(delivered, "drained the token audit outbox");
            }
            Ok(_) => {}
            Err(error) => {
                warn!(%error, "failed to drain the token audit outbox");
            }
        }
    }

    async fn drain_on_shutdown(&self) {
        if let Err(error) = self.relay.drain().await {
            warn!(%error, "failed to drain the token audit outbox during shutdown");
        }
    }
}
