use crate::supervisor::Supervisor;
use platform_errors::PlatformError;

#[tokio::test]
async fn permanent_task_errors_are_not_restarted() {
    let error = Supervisor::supervise("test", || async {
        Err(PlatformError::config_missing("aggregator test setting"))
    })
    .await
    .unwrap_err();

    assert!(error.retryability().is_permanent());
}
