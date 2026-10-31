use super::*;

#[tokio::test]
async fn panicked_task_is_permanent() {
    let error = tokio::spawn(async { panic!("task panicked") })
        .await
        .unwrap_err();

    assert_eq!(
        SystemError::Task(error).retryability(),
        Retryability::Permanent
    );
}

#[tokio::test]
async fn cancelled_task_is_retryable() {
    let task = tokio::spawn(async { std::future::pending::<()>().await });
    task.abort();
    let error = task.await.unwrap_err();

    assert_eq!(
        SystemError::Task(error).retryability(),
        Retryability::Retryable
    );
}
