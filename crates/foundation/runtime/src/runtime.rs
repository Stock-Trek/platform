//! Process-level runtime helpers shared by services across the workspace.

use platform_errors::PlatformResult;
use std::future::Future;
use tracing::info;

/// Process-level lifecycle helpers shared by services across the workspace.
pub struct Runtime;

impl Runtime {
    /// Resolves once the process receives a shutdown signal.
    ///
    /// Service loops select on this future so every service logs and handles
    /// shutdown in the same way. On Unix this listens for both `SIGINT`
    /// (Ctrl-C) and `SIGTERM`, the signal containers use to stop a process.
    pub async fn shutdown_signal() -> PlatformResult<()> {
        #[cfg(unix)]
        {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            tokio::select! {
                result = tokio::signal::ctrl_c() => result?,
                _ = terminate.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await?;
        }
        info!("shutdown requested");
        Ok(())
    }

    /// Runs `work` until it completes or the process receives a shutdown signal.
    ///
    /// Service loops pass their main work here instead of repeating the same
    /// `tokio::select!` shutdown arm. When shutdown wins, the work future is
    /// dropped and `Ok(())` is returned so callers can run finalisation steps.
    pub async fn run_until_shutdown<F>(work: F) -> PlatformResult<()>
    where
        F: Future<Output = PlatformResult<()>>,
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
