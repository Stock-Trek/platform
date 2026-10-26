use crate::{
    clients::client::{HttpClient, WebsocketClient},
    connector::Connector,
    panic_guard::PanicUtils,
    submission::Submission,
};
use platform_errors::{BpError, BpResult, ConnectorError, SystemError};
use platform_exchange_types::{
    exchange::ETExchange,
    new_types::{TimestampMilliseconds, UsageCount},
    rate_limited::RateLimit,
    request::{ETHttpRequest, ETWebsocketRequest},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::mpsc::UnboundedSender, task::JoinHandle};
use tracing::{debug, warn};

type ClockSyncFailureCallback = Arc<dyn Fn(&BpError) + Send + Sync>;
type SharedClockSyncFailureCallback = Arc<Mutex<Option<ClockSyncFailureCallback>>>;

pub struct AutoResyncConnector<Exchange, Client> {
    connector: Arc<Connector<Exchange, Client>>,
    resync_handle: Mutex<Option<AutoResyncHandle>>,
    failure_callback: SharedClockSyncFailureCallback,
}

struct AutoResyncHandle {
    sender: UnboundedSender<ClockSyncCommand>,
    join: JoinHandle<()>,
}

enum ClockSyncCommand {
    SetFrequency(Duration),
    Stop,
}

impl<Exchange, Client> AutoResyncConnector<Exchange, Client> {
    async fn clock_sync_loop<F, Fut>(
        frequency: Duration,
        first_sync_sender: tokio::sync::oneshot::Sender<BpResult<()>>,
        receiver: tokio::sync::mpsc::UnboundedReceiver<ClockSyncCommand>,
        failure_callback: SharedClockSyncFailureCallback,
        sync_clock_fn: F,
    ) where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = BpResult<()>> + Send + 'static,
    {
        let panic_callback = failure_callback.clone();
        let result = PanicUtils::catch_panic_async(Self::clock_sync_loop_inner(
            frequency,
            first_sync_sender,
            receiver,
            failure_callback,
            sync_clock_fn,
        ))
        .await;
        if let Err(panic) = result {
            // Surface the panic before letting it propagate so the task never dies silently.
            Self::report_failure(
                &panic_callback,
                &BpError::Connector(ConnectorError::AutoResyncClockPanicked),
            );
            std::panic::resume_unwind(panic.into_payload());
        }
    }

    async fn clock_sync_loop_inner<F, Fut>(
        mut frequency: Duration,
        first_sync_sender: tokio::sync::oneshot::Sender<BpResult<()>>,
        mut receiver: tokio::sync::mpsc::UnboundedReceiver<ClockSyncCommand>,
        failure_callback: SharedClockSyncFailureCallback,
        sync_clock_fn: F,
    ) where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = BpResult<()>> + Send + 'static,
    {
        let first_sync_result = sync_clock_fn().await;
        if let Err(Err(error)) = first_sync_sender.send(first_sync_result) {
            Self::report_failure(&failure_callback, &error);
        }
        let mut last_sync = tokio::time::Instant::now();
        loop {
            let deadline = last_sync + frequency;
            tokio::select! {
                cmd = receiver.recv() => match cmd {
                    Some(ClockSyncCommand::SetFrequency(new_frequency)) => {
                        frequency = new_frequency;
                    }
                    Some(ClockSyncCommand::Stop) | None => break,
                },
                _ = tokio::time::sleep_until(deadline) => {
                    if let Err(error) = sync_clock_fn().await {
                        Self::report_failure(&failure_callback, &error);
                    }
                    last_sync = tokio::time::Instant::now();
                }
            }
        }
    }

    fn report_failure(failure_callback: &SharedClockSyncFailureCallback, error: &BpError) {
        warn!(error = %error, "auto-resync clock sync failed");
        let callback = match failure_callback.lock() {
            Ok(callback) => callback.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        if let Some(callback) = callback.as_ref() {
            // A panicking failure callback must not bring down the task silently.
            let _ = PanicUtils::catch_panic(|| callback(error));
        }
    }
}

impl<Exchange, Client> AutoResyncConnector<Exchange, Client>
where
    Exchange: ETExchange + Send + Sync + 'static,
    Client: Send + Sync + 'static,
{
    pub(crate) fn new(connector: Arc<Connector<Exchange, Client>>) -> Self {
        Self {
            connector,
            resync_handle: Mutex::new(None),
            failure_callback: Arc::new(Mutex::new(None)),
        }
    }
    pub fn duration_since_last_sync(&self) -> BpResult<Option<Duration>> {
        self.connector.duration_since_last_sync()
    }
    pub fn remaining_rate_limit_capacity(&self) -> BpResult<HashMap<RateLimit, UsageCount>> {
        self.connector.remaining_rate_limit_capacity()
    }
    pub fn server_time_estimate(&self) -> BpResult<TimestampMilliseconds> {
        self.connector.server_time_estimate()
    }
    pub fn on_clock_sync_failure<F>(&self, callback: F)
    where
        F: Fn(&BpError) + Send + Sync + 'static,
    {
        let mut guard = match self.failure_callback.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        *guard = Some(Arc::new(callback));
    }
    pub async fn stop_clock_sync(&self) -> BpResult<()> {
        let handle = self
            .resync_handle
            .lock()
            .map_err(|_| BpError::System(SystemError::PoisonedLock))?
            .take();
        let Some(handle) = handle else { return Ok(()) };
        debug!("stopping auto-resync clock sync");
        let _ = handle.sender.send(ClockSyncCommand::Stop);
        drop(handle.sender); // let the loop drain
        match handle.join.await {
            Ok(()) => Ok(()),
            Err(e) if e.is_panic() => {
                Err(BpError::Connector(ConnectorError::AutoResyncClockPanicked))
            }
            Err(_) => Ok(()),
        }
    }
    async fn set_clock_sync_frequency<F, Fut>(
        &self,
        frequency: Duration,
        sync_clock_fn: F,
    ) -> BpResult<()>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = BpResult<()>> + Send + 'static,
    {
        if frequency < Duration::from_mins(1) {
            return Err(BpError::Connector(ConnectorError::InvalidSyncFrequency));
        }
        let (first_sync_sender, first_sync_receiver) = tokio::sync::oneshot::channel();
        {
            let mut resync_handle = self
                .resync_handle
                .lock()
                .map_err(|_| BpError::System(SystemError::PoisonedLock))?;
            if let Some(handle) = resync_handle.as_ref() {
                if handle.join.is_finished()
                    || handle
                        .sender
                        .send(ClockSyncCommand::SetFrequency(frequency))
                        .is_err()
                {
                    *resync_handle = None;
                    return Err(BpError::Connector(ConnectorError::AutoResyncClockStopped));
                }
                debug!(?frequency, "updating auto-resync clock sync frequency");
                return Ok(());
            }
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
            debug!(?frequency, "starting auto-resync clock sync");
            let join = tokio::spawn(Self::clock_sync_loop(
                frequency,
                first_sync_sender,
                receiver,
                self.failure_callback.clone(),
                sync_clock_fn,
            ));
            *resync_handle = Some(AutoResyncHandle { sender, join });
        }
        match first_sync_receiver.await {
            Ok(result) => result,
            Err(_) => Err(BpError::Connector(ConnectorError::AutoResyncClockPanicked)),
        }
    }
}

impl<Exchange, Client> Drop for AutoResyncConnector<Exchange, Client> {
    fn drop(&mut self) {
        let handle = match self.resync_handle.get_mut() {
            Ok(handle) => handle.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(handle) = handle {
            let _ = handle.sender.send(ClockSyncCommand::Stop);
            handle.join.abort();
        }
    }
}

impl<Exchange, Client> AutoResyncConnector<Exchange, Client>
where
    Exchange: ETExchange,
    Client: HttpClient,
{
    pub async fn sync_clock_http(&self) -> BpResult<()> {
        self.connector.sync_clock_http().await
    }
    pub async fn set_clock_sync_frequency_http(&self, frequency: Duration) -> BpResult<()>
    where
        Exchange: Send + Sync + 'static,
        Exchange::ServerTimeRequestHttp: Send,
        Client: Send + Sync + 'static,
    {
        let connector = self.connector.clone();
        self.set_clock_sync_frequency(frequency, move || {
            let connector = connector.clone();
            async move { connector.sync_clock_http().await }
        })
        .await
    }
    pub fn submit_http<'connector, Request>(
        &'connector self,
        request: Request,
    ) -> BpResult<Submission<'connector, Request, Request::Response, Request::VerificationRequest>>
    where
        Request: ETHttpRequest<Exchange = Exchange> + Send + 'connector,
        Exchange: Sync,
        Client: Sync,
    {
        self.connector.submit_http(request)
    }
}

impl<Exchange, Client> AutoResyncConnector<Exchange, Client>
where
    Exchange: ETExchange,
    Client: WebsocketClient,
{
    pub async fn connect(&self) -> BpResult<()> {
        self.connector.connect().await
    }
    pub fn is_connected(&self) -> BpResult<bool> {
        self.connector.is_connected()
    }
    pub async fn disconnect(&self) -> BpResult<()> {
        self.connector.disconnect().await
    }
    pub async fn sync_clock_websocket(&self) -> BpResult<()> {
        self.connector.sync_clock_websocket().await
    }
    pub async fn set_clock_sync_frequency_websocket(&self, frequency: Duration) -> BpResult<()>
    where
        Exchange: Send + Sync + 'static,
        Exchange::ServerTimeRequestWebsocket: Send,
        Client: Send + Sync + 'static,
    {
        let connector = self.connector.clone();
        self.set_clock_sync_frequency(frequency, move || {
            let connector = connector.clone();
            async move { connector.sync_clock_websocket().await }
        })
        .await
    }
    pub fn submit_websocket<'connector, Request>(
        &'connector self,
        request: Request,
    ) -> BpResult<Submission<'connector, Request, Request::Response, Request::VerificationRequest>>
    where
        Request: ETWebsocketRequest<Exchange = Exchange> + Send + 'connector,
        Exchange: Sync,
    {
        self.connector.submit_websocket(request)
    }
}

impl<Exchange, Client> std::fmt::Debug for AutoResyncConnector<Exchange, Client> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AutoResyncConnector")
            .field("connector", &self.connector)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

    fn sync_counter(
        counter: Arc<AtomicUsize>,
        notify: UnboundedSender<()>,
    ) -> impl Fn() -> std::future::Ready<BpResult<()>> {
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
            Arc::new(Mutex::new(Some(Arc::new(move |error: &BpError| {
                let _ = failure_sender.send(error.to_string());
            }))));
        let (first_sync_sender, first_sync_receiver) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(AutoResyncConnector::<(), ()>::clock_sync_loop(
            Duration::from_mins(30),
            first_sync_sender,
            command_receiver,
            failure_callback,
            || std::future::ready(Err(BpError::Connector(ConnectorError::NotConnected))),
        ));

        let first_sync_result = first_sync_receiver.await.unwrap();
        assert!(matches!(
            first_sync_result,
            Err(BpError::Connector(ConnectorError::NotConnected))
        ));
        assert!(failure_receiver.try_recv().is_err());

        tokio::time::advance(Duration::from_mins(30)).await;
        assert_eq!(
            failure_receiver.recv().await.unwrap(),
            BpError::Connector(ConnectorError::NotConnected).to_string()
        );

        join.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn first_sync_failure_is_reported_when_caller_is_gone() {
        let (_command_sender, command_receiver) = unbounded_channel();
        let (failure_sender, mut failure_receiver) = unbounded_channel();
        let failure_callback: SharedClockSyncFailureCallback =
            Arc::new(Mutex::new(Some(Arc::new(move |error: &BpError| {
                let _ = failure_sender.send(error.to_string());
            }))));
        let (first_sync_sender, first_sync_receiver) = tokio::sync::oneshot::channel();
        drop(first_sync_receiver);
        let join = tokio::spawn(AutoResyncConnector::<(), ()>::clock_sync_loop(
            Duration::from_mins(30),
            first_sync_sender,
            command_receiver,
            failure_callback,
            || std::future::ready(Err(BpError::Connector(ConnectorError::NotConnected))),
        ));

        assert_eq!(
            failure_receiver.recv().await.unwrap(),
            BpError::Connector(ConnectorError::NotConnected).to_string()
        );

        join.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn panicking_syncs_are_reported_to_failure_callback() {
        let (_command_sender, command_receiver) = unbounded_channel();
        let (failure_sender, mut failure_receiver) = unbounded_channel();
        let failure_callback: SharedClockSyncFailureCallback =
            Arc::new(Mutex::new(Some(Arc::new(move |error: &BpError| {
                let _ = failure_sender.send(error.to_string());
            }))));
        let (first_sync_sender, first_sync_receiver) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(AutoResyncConnector::<(), ()>::clock_sync_loop(
            Duration::from_mins(30),
            first_sync_sender,
            command_receiver,
            failure_callback,
            || -> std::future::Ready<BpResult<()>> { panic!("sync clock panicked") },
        ));

        assert_eq!(
            failure_receiver.recv().await.unwrap(),
            BpError::Connector(ConnectorError::AutoResyncClockPanicked).to_string()
        );
        assert!(first_sync_receiver.await.is_err());
        assert!(join.await.unwrap_err().is_panic());
    }
}
