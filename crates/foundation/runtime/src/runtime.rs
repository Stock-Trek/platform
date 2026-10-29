//! Process-level runtime helpers shared by services across the workspace.

use platform_config::tracing_setup::TracingSetup;
use platform_errors::BpResult;
use std::future::Future;
use tracing::info;

/// Process-level lifecycle helpers shared by services across the workspace.
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

    /// Runs `work` until it completes or the process receives a shutdown signal.
    ///
    /// Service loops pass their main work here instead of repeating the same
    /// `tokio::select!` shutdown arm. When shutdown wins, the work future is
    /// dropped and `Ok(())` is returned so callers can run finalisation steps.
    pub async fn run_until_shutdown<F>(work: F) -> BpResult<()>
    where
        F: Future<Output = BpResult<()>>,
    {
        tokio::select! {
            result = work => result,
            signal = Self::shutdown_signal() => {
                signal?;
                Ok(())
            }
        }
    }
}
