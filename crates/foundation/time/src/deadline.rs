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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_remaining_reports_available_time() {
        let deadline = Deadline::new(SystemTime::now() + Duration::from_secs(60), Duration::ZERO);

        assert!(deadline.has_remaining());
        assert!(!deadline.remaining_or_zero().is_zero());
        assert!(deadline.fail_if_exceeded().is_ok());
    }

    #[test]
    fn expired_deadline_has_no_time_left() {
        let deadline = Deadline::new(SystemTime::UNIX_EPOCH, Duration::ZERO);

        assert!(!deadline.has_remaining());
        assert_eq!(deadline.remaining_or_zero(), Duration::ZERO);
        assert!(matches!(
            deadline.fail_if_exceeded(),
            Err(BpError::Timeout(TimeoutError::DeadlineExceeded))
        ));
    }

    #[test]
    fn buffer_can_consume_all_remaining_time() {
        let deadline = Deadline::new(
            SystemTime::now() + Duration::from_secs(60),
            Duration::from_secs(60),
        );

        assert!(!deadline.has_remaining());
        assert_eq!(deadline.remaining_or_zero(), Duration::ZERO);
    }
}
