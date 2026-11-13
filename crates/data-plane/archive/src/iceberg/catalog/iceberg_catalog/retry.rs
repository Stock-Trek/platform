use rand::random;
use std::time::Duration;
use tokio::time::sleep;

const JITTER_FLOOR: f64 = 0.5;

pub(super) struct RetryPolicy {
    max_attempts: usize,
    base_backoff: Duration,
}

impl RetryPolicy {
    pub(super) const fn new(max_attempts: usize, base_backoff: Duration) -> Self {
        Self {
            max_attempts,
            base_backoff,
        }
    }

    pub(super) const fn max_attempts(&self) -> usize {
        self.max_attempts
    }

    pub(super) fn should_retry(&self, attempt: usize) -> bool {
        attempt < self.max_attempts
    }

    pub(super) fn backoff(&self, attempt: usize) -> Duration {
        let multiplier = u32::try_from(attempt).unwrap_or(u32::MAX);
        let base = self.base_backoff.saturating_mul(multiplier);
        let jitter = JITTER_FLOOR + random::<f64>() * (1.0 - JITTER_FLOOR);
        base.mul_f64(jitter)
    }

    pub(super) async fn sleep(&self, attempt: usize) {
        sleep(self.backoff(attempt)).await;
    }
}
