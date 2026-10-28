//! Process-level runtime helpers shared by the data-plane services.

use platform_config::tracing_setup::TracingSetup;
use platform_errors::BpResult;
use tracing::info;

/// Process-level lifecycle helpers shared by the data-plane services.
pub struct Runtime;

impl Runtime {
    /// Installs the global tracing subscriber using an already validated filter.
    pub fn init_logging(log_filter: &str) -> BpResult<()> {
        TracingSetup::setup(log_filter)
    }

    /// Resolves once the process receives a shutdown signal.
    ///
    /// Service loops select on this future so every service logs and handles
    /// shutdown in the same way.
    pub async fn shutdown_signal() -> BpResult<()> {
        tokio::signal::ctrl_c().await?;
        info!("shutdown requested");
        Ok(())
    }
}
