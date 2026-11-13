use platform_time::Jitter;
use std::time::Duration;

const MAX_EXPONENT: u32 = 4;

pub(crate) struct RetryBackoff {
    base: Duration,
    failures: u32,
}

impl RetryBackoff {
    pub(crate) fn new(base: Duration) -> Self {
        Self { base, failures: 0 }
    }

    pub(crate) fn reset(&mut self) {
        self.failures = 0;
    }

    pub(crate) fn delay(&mut self) -> Duration {
        let multiplier = 2_u32.pow(self.failures.min(MAX_EXPONENT));
        self.failures = self.failures.saturating_add(1);
        let base = self.base.saturating_mul(multiplier);
        Jitter::delay(base)
    }
}
