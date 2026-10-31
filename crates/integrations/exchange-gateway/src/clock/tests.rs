use super::*;

#[test]
fn server_time_estimate_errors_before_sync() {
    let clock = Clock::new();
    assert!(!clock.is_synced().unwrap());
    assert!(matches!(
        clock.server_time_estimate(),
        Err(BpError::Connector(ConnectorError::ClockNotSynced))
    ));
}

#[test]
fn server_time_estimate_succeeds_after_sync() {
    let clock = Clock::new();
    let server_time = TimestampMilliseconds(1_700_000_000_000);
    clock.sync(server_time, Duration::from_millis(20)).unwrap();
    assert!(clock.is_synced().unwrap());
    let estimate = clock.server_time_estimate().unwrap();
    let difference = (estimate.0 - server_time.0).abs();
    assert!(
        difference < 1_000,
        "estimate {estimate:?} too far from server time"
    );
}
