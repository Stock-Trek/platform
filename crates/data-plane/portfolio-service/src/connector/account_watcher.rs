use std::time::Duration;
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::timeout,
};

pub(super) struct AccountWatcher {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl AccountWatcher {
    pub(super) fn new(shutdown: watch::Sender<bool>, task: JoinHandle<()>) -> Self {
        Self { shutdown, task }
    }

    pub(super) async fn stop(self) {
        let Self { shutdown, mut task } = self;
        let _ = shutdown.send(true);
        if timeout(Duration::from_secs(2), &mut task).await.is_err() {
            task.abort();
        }
    }
}
