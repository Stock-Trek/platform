use platform_errors::BpResult;
use platform_runtime::Runtime;
use std::future::Future;
use std::time::{Duration, Instant};
use tracing::{error, warn};

#[cfg(test)]
mod tests;

pub struct Supervisor;

impl Supervisor {
    const INITIAL_RESTART_DELAY: Duration = Duration::from_secs(1);
    const MAX_RESTART_DELAY: Duration = Duration::from_secs(30);
    const HEALTHY_RUN_THRESHOLD: Duration = Duration::from_secs(60);

    pub async fn supervise<F, Fut>(name: &'static str, mut run: F) -> BpResult<()>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = BpResult<()>>,
    {
        let mut restart_delay = Self::INITIAL_RESTART_DELAY;
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
                restart_delay = Self::INITIAL_RESTART_DELAY;
            }
            let delay = Self::jittered_delay(restart_delay);
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                result = &mut shutdown => {
                    result?;
                    return Ok(());
                }
            }
            restart_delay = Self::next_restart_delay(restart_delay);
        }
    }

    fn next_restart_delay(current: Duration) -> Duration {
        (current * 2).min(Self::MAX_RESTART_DELAY)
    }

    fn jittered_delay(base: Duration) -> Duration {
        // Jitter the upper half of the delay so replicas that failed together do
        // not all reconnect in lockstep when a shared dependency recovers.
        let jitter = 0.5 + rand::random::<f64>() * 0.5;
        base.mul_f64(jitter).min(Self::MAX_RESTART_DELAY)
    }
}
