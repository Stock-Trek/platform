use crate::Jitter;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffStrategy {
    Constant,
    Linear,
    Exponential,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    max_attempts: usize,
    initial_delay: Duration,
    max_delay: Duration,
    strategy: BackoffStrategy,
    jitter: bool,
}

impl RetryPolicy {
    pub const fn constant(max_attempts: usize, delay: Duration) -> Self {
        Self {
            max_attempts,
            initial_delay: delay,
            max_delay: delay,
            strategy: BackoffStrategy::Constant,
            jitter: true,
        }
    }

    pub const fn linear(max_attempts: usize, initial_delay: Duration, max_delay: Duration) -> Self {
        Self {
            max_attempts,
            initial_delay,
            max_delay,
            strategy: BackoffStrategy::Linear,
            jitter: true,
        }
    }

    pub const fn exponential(
        max_attempts: usize,
        initial_delay: Duration,
        max_delay: Duration,
    ) -> Self {
        Self {
            max_attempts,
            initial_delay,
            max_delay,
            strategy: BackoffStrategy::Exponential,
            jitter: true,
        }
    }

    pub const fn with_jitter(mut self) -> Self {
        self.jitter = true;
        self
    }

    pub const fn without_jitter(mut self) -> Self {
        self.jitter = false;
        self
    }

    pub const fn max_attempts(&self) -> usize {
        self.max_attempts
    }

    pub const fn should_retry(&self, attempt: usize) -> bool {
        attempt < self.max_attempts
    }

    pub fn delay(&self, attempt: usize) -> Duration {
        let delay = self.base_delay(attempt);
        if self.jitter {
            Jitter::delay(delay).min(self.max_delay)
        } else {
            delay
        }
    }

    fn base_delay(&self, attempt: usize) -> Duration {
        let steps = u32::try_from(attempt).unwrap_or(u32::MAX);
        let multiplier = match self.strategy {
            BackoffStrategy::Constant => 1,
            BackoffStrategy::Linear => steps.saturating_add(1),
            BackoffStrategy::Exponential => 2_u32.checked_pow(steps).unwrap_or(u32::MAX),
        };
        self.initial_delay
            .saturating_mul(multiplier)
            .min(self.max_delay)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryBackoff {
    policy: RetryPolicy,
    attempt: usize,
}

impl RetryBackoff {
    pub const fn new(policy: RetryPolicy) -> Self {
        Self { policy, attempt: 0 }
    }

    pub const fn constant(delay: Duration) -> Self {
        Self::new(RetryPolicy::constant(usize::MAX, delay))
    }

    pub const fn linear(initial_delay: Duration, max_delay: Duration) -> Self {
        Self::new(RetryPolicy::linear(usize::MAX, initial_delay, max_delay))
    }

    pub const fn exponential(initial_delay: Duration, max_delay: Duration) -> Self {
        Self::new(RetryPolicy::exponential(
            usize::MAX,
            initial_delay,
            max_delay,
        ))
    }

    pub const fn with_jitter(mut self) -> Self {
        self.policy = self.policy.with_jitter();
        self
    }

    pub const fn without_jitter(mut self) -> Self {
        self.policy = self.policy.without_jitter();
        self
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    pub fn next_delay(&mut self) -> Duration {
        let delay = self.policy.delay(self.attempt);
        self.attempt = self.attempt.saturating_add(1);
        delay
    }
}
