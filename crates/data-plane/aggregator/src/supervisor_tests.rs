use crate::supervisor::Supervisor;
use platform_errors::BuildError;
use std::time::Duration;

#[test]
fn restart_delay_doubles_until_it_reaches_the_cap() {
    assert_eq!(
        Supervisor::next_restart_delay(Supervisor::INITIAL_RESTART_DELAY),
        Duration::from_secs(2)
    );
    assert_eq!(
        Supervisor::next_restart_delay(Supervisor::MAX_RESTART_DELAY),
        Supervisor::MAX_RESTART_DELAY
    );
}

#[tokio::test]
async fn permanent_task_errors_are_not_restarted() {
    let error = Supervisor::supervise("test", || async {
        Err(BuildError::config_missing("aggregator test setting"))
    })
    .await
    .unwrap_err();

    assert!(error.retryability().is_permanent());
}
