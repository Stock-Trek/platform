use platform_errors::{BpError, BpResult, TimeoutError};
use std::time::{Duration, Instant, SystemTime};

#[derive(Debug, Clone, Copy)]
pub struct Deadline {
    at: Instant,
    buffer: Duration,
}

impl Deadline {
    pub fn new(deadline: SystemTime, buffer: Duration) -> Self {
        let remaining = deadline
            .duration_since(SystemTime::now())
            .unwrap_or_default();
        Self {
            at: Instant::now() + remaining,
            buffer,
        }
    }

    /// Returns whether time remains after the deadline buffer.
    pub fn has_remaining(self) -> bool {
        !self.remaining_or_zero().is_zero()
    }

    /// Returns the time left after the deadline buffer, or zero once it has
    /// passed.
    pub fn remaining_or_zero(self) -> Duration {
        self.at
            .saturating_duration_since(Instant::now())
            .saturating_sub(self.buffer)
    }

    pub fn fail_if_exceeded(self) -> BpResult<()> {
        self.checked_remaining()?;
        Ok(())
    }

    /// Returns the time left after the deadline buffer.
    pub fn checked_remaining(self) -> BpResult<Duration> {
        let remaining = self.remaining_or_zero();
        if remaining.is_zero() {
            return Err(BpError::Timeout(TimeoutError::DeadlineExceeded));
        }
        Ok(remaining)
    }
}
