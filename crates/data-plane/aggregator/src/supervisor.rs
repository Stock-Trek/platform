use platform_errors::BuildResult;
use platform_runtime::Runtime;
use platform_time::RetryBackoff;
use std::{
    future::Future,
    time::{Duration, Instant},
};
use tracing::{error, warn};

pub struct Supervisor;

impl Supervisor {
    pub const INITIAL_RESTART_DELAY: Duration = Duration::from_secs(1);
    pub const MAX_RESTART_DELAY: Duration = Duration::from_secs(30);
    pub const HEALTHY_RUN_THRESHOLD: Duration = Duration::from_secs(60);

    pub async fn supervise<F, Fut>(name: &'static str, mut run: F) -> BuildResult<()>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = BuildResult<()>>,
    {
        let mut restart =
            RetryBackoff::exponential(Self::INITIAL_RESTART_DELAY, Self::MAX_RESTART_DELAY);
        let shutdown = Runtime::shutdown_signal();
        tokio::pin!(shutdown);

        loop {
            let started_at = Instant::now();
            match run().await {
                Ok(()) => {
                    warn!(
                        task = name,
                        "aggregator task exited without a shutdown request; restarting"
                    );
                }
                Err(error) if error.retryability().is_retryable() => {
                    error!(task = name, %error, "aggregator task stopped; restarting");
                }
                Err(error) => {
                    error!(
                        task = name,
                        %error,
                        "aggregator task failed permanently; not restarting"
                    );
                    return Err(error);
                }
            }

            if started_at.elapsed() >= Self::HEALTHY_RUN_THRESHOLD {
                restart.reset();
            }
            let delay = restart.next_delay();
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                result = &mut shutdown => {
                    result?;
                    return Ok(());
                }
            }
        }
    }
}
