use parking_lot::Mutex;
use platform_errors::{BuildError, BuildResult, ConnectorError};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use crate::resync_connector::{
    AutoResyncConnector, ClockSyncCommand, SharedClockSyncFailureCallback,
};

fn sync_counter(
    counter: Arc<AtomicUsize>,
    notify: UnboundedSender<()>,
) -> impl Fn() -> std::future::Ready<BuildResult<()>> {
    move || {
        counter.fetch_add(1, Ordering::SeqCst);
        let _ = notify.send(());
        std::future::ready(Ok(()))
    }
}

fn no_failure_callback() -> SharedClockSyncFailureCallback {
    Arc::new(Mutex::new(None))
}

#[tokio::test(start_paused = true)]
async fn shrinking_frequency_triggers_immediate_sync() {
    let (command_sender, command_receiver) = unbounded_channel();
    let (notify_sender, mut notify_receiver) = unbounded_channel();
    let counter = Arc::new(AtomicUsize::new(0));
    let (first_sync_sender, _) = tokio::sync::oneshot::channel();
    let start = tokio::time::Instant::now();
    let join = tokio::spawn(AutoResyncConnector::<(), ()>::clock_sync_loop(
        Duration::from_hours(3),
        first_sync_sender,
        command_receiver,
        no_failure_callback(),
        sync_counter(counter.clone(), notify_sender),
    ));

    notify_receiver.recv().await.unwrap();
    tokio::time::advance(Duration::from_hours(1)).await;
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    command_sender
        .send(ClockSyncCommand::SetFrequency(Duration::from_mins(30)))
        .unwrap();
    notify_receiver.recv().await.unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 2);
    assert_eq!(start.elapsed(), Duration::from_hours(1));

    join.abort();
}

#[tokio::test(start_paused = true)]
async fn growing_frequency_accounts_for_elapsed_time() {
    let (command_sender, command_receiver) = unbounded_channel();
    let (notify_sender, mut notify_receiver) = unbounded_channel();
    let counter = Arc::new(AtomicUsize::new(0));
    let (first_sync_sender, _) = tokio::sync::oneshot::channel();
    let start = tokio::time::Instant::now();
    let join = tokio::spawn(AutoResyncConnector::<(), ()>::clock_sync_loop(
        Duration::from_hours(4),
        first_sync_sender,
        command_receiver,
        no_failure_callback(),
        sync_counter(counter.clone(), notify_sender),
    ));

    notify_receiver.recv().await.unwrap();
    tokio::time::advance(Duration::from_hours(1)).await;
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    command_sender
        .send(ClockSyncCommand::SetFrequency(Duration::from_hours(3)))
        .unwrap();

    notify_receiver.recv().await.unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 2);
    assert_eq!(start.elapsed(), Duration::from_hours(3));

    join.abort();
}

#[tokio::test(start_paused = true)]
async fn failed_syncs_are_reported_to_failure_callback() {
    let (_command_sender, command_receiver) = unbounded_channel();
    let (failure_sender, mut failure_receiver) = unbounded_channel();
    let failure_callback: SharedClockSyncFailureCallback =
        Arc::new(Mutex::new(Some(Arc::new(move |error: &BuildError| {
            let _ = failure_sender.send(error.to_string());
        }))));
    let (first_sync_sender, first_sync_receiver) = tokio::sync::oneshot::channel();
    let join = tokio::spawn(AutoResyncConnector::<(), ()>::clock_sync_loop(
        Duration::from_mins(30),
        first_sync_sender,
        command_receiver,
        failure_callback,
        || std::future::ready(Err(BuildError::Connector(ConnectorError::NotConnected))),
    ));

    let first_sync_result = first_sync_receiver.await.unwrap();
    assert!(matches!(
        first_sync_result,
        Err(BuildError::Connector(ConnectorError::NotConnected))
    ));
    assert!(failure_receiver.try_recv().is_err());

    tokio::time::advance(Duration::from_mins(30)).await;
    assert_eq!(
        failure_receiver.recv().await.unwrap(),
        BuildError::Connector(ConnectorError::NotConnected).to_string()
    );

    join.abort();
}

#[tokio::test(start_paused = true)]
async fn first_sync_failure_is_reported_when_caller_is_gone() {
    let (_command_sender, command_receiver) = unbounded_channel();
    let (failure_sender, mut failure_receiver) = unbounded_channel();
    let failure_callback: SharedClockSyncFailureCallback =
        Arc::new(Mutex::new(Some(Arc::new(move |error: &BuildError| {
            let _ = failure_sender.send(error.to_string());
        }))));
    let (first_sync_sender, first_sync_receiver) = tokio::sync::oneshot::channel();
    drop(first_sync_receiver);
    let join = tokio::spawn(AutoResyncConnector::<(), ()>::clock_sync_loop(
        Duration::from_mins(30),
        first_sync_sender,
        command_receiver,
        failure_callback,
        || std::future::ready(Err(BuildError::Connector(ConnectorError::NotConnected))),
    ));

    assert_eq!(
        failure_receiver.recv().await.unwrap(),
        BuildError::Connector(ConnectorError::NotConnected).to_string()
    );

    join.abort();
}

#[tokio::test(start_paused = true)]
async fn panicking_syncs_are_reported_to_failure_callback() {
    let (_command_sender, command_receiver) = unbounded_channel();
    let (failure_sender, mut failure_receiver) = unbounded_channel();
    let failure_callback: SharedClockSyncFailureCallback =
        Arc::new(Mutex::new(Some(Arc::new(move |error: &BuildError| {
            let _ = failure_sender.send(error.to_string());
        }))));
    let (first_sync_sender, first_sync_receiver) = tokio::sync::oneshot::channel();
    let join = tokio::spawn(AutoResyncConnector::<(), ()>::clock_sync_loop(
        Duration::from_mins(30),
        first_sync_sender,
        command_receiver,
        failure_callback,
        || -> std::future::Ready<BuildResult<()>> { panic!("sync clock panicked") },
    ));

    assert_eq!(
        failure_receiver.recv().await.unwrap(),
        BuildError::Connector(ConnectorError::AutoResyncClockPanicked).to_string()
    );
    assert!(first_sync_receiver.await.is_err());
    assert!(join.await.unwrap_err().is_panic());
}
