use super::*;
use platform_errors::{BpError, TimeoutError};
use std::time::{Duration, SystemTime};

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
