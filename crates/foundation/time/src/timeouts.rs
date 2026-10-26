use crate::deadline::Deadline;
use platform_errors::BpResult;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NonZeroDuration(Duration);

impl NonZeroDuration {
    pub const fn new(duration: Duration) -> Option<Self> {
        if duration.is_zero() {
            None
        } else {
            Some(Self(duration))
        }
    }

    pub const fn get(self) -> Duration {
        self.0
    }
}

impl From<NonZeroDuration> for Duration {
    fn from(value: NonZeroDuration) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timeout {
    Disabled,
    Bounded(NonZeroDuration),
}

impl Timeout {
    /// Converts a duration into a timeout, treating zero as disabled.
    pub fn from_duration(duration: Duration) -> Self {
        NonZeroDuration::new(duration).map_or(Self::Disabled, Self::Bounded)
    }

    pub fn duration(self) -> Option<Duration> {
        match self {
            Self::Disabled => None,
            Self::Bounded(duration) => Some(duration.get()),
        }
    }
}

impl From<Timeout> for Option<Duration> {
    fn from(timeout: Timeout) -> Self {
        timeout.duration()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    pub connect: Timeout,
    pub operation: Timeout,
}

impl Timeouts {
    pub fn new(connect: Timeout, operation: Timeout) -> Self {
        Self { connect, operation }
    }

    /// Builds timeouts from durations, treating zero as disabled.
    pub fn from_durations(connect: Duration, operation: Duration) -> Self {
        Self::new(
            Timeout::from_duration(connect),
            Timeout::from_duration(operation),
        )
    }

    pub fn operation_timeout(self, deadline: Deadline) -> BpResult<Duration> {
        let remaining = deadline.checked_remaining()?;
        Ok(match self.operation {
            Timeout::Disabled => remaining,
            Timeout::Bounded(operation) => operation.get().min(remaining),
        })
    }
}
