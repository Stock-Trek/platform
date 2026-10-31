use super::*;

#[test]
fn restart_delay_doubles_until_it_reaches_the_cap() {
    assert_eq!(
        next_restart_delay(INITIAL_RESTART_DELAY),
        Duration::from_secs(2)
    );
    assert_eq!(next_restart_delay(MAX_RESTART_DELAY), MAX_RESTART_DELAY);
}

#[test]
fn jitter_keeps_the_delay_within_the_upper_half_of_the_base() {
    let base = Duration::from_secs(8);
    for _ in 0..32 {
        let delay = jittered_delay(base);
        assert!(delay >= base / 2);
        assert!(delay <= base);
    }
}

#[tokio::test]
async fn permanent_task_errors_are_not_restarted() {
    let error = supervise("test", || async {
        Err(BpError::config_missing("aggregator test setting"))
    })
    .await
    .unwrap_err();

    assert!(error.retryability().is_permanent());
}
