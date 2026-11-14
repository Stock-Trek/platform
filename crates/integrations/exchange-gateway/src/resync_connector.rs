use crate::{connector::Connector, panic_guard::PanicUtils};
use parking_lot::Mutex;
use platform_errors::{ConnectorError, PlatformError, PlatformResult};
use platform_exchange_types::{
    exchange::ETExchange,
    exchange_server_time::{ETExchangeServerTimeHttp, ETExchangeServerTimeWebsocket},
    http::HttpClient,
    websocket::WebsocketClient,
};
use std::{ops::Deref, sync::Arc, time::Duration};
use tokio::{sync::mpsc::UnboundedSender, task::JoinHandle};
use tracing::{debug, warn};

pub type ClockSyncFailureCallback = Arc<dyn Fn(&PlatformError) + Send + Sync>;
pub type SharedClockSyncFailureCallback = Arc<Mutex<Option<ClockSyncFailureCallback>>>;

pub struct AutoResyncConnector<Exchange, Client> {
    connector: Arc<Connector<Exchange, Client>>,
    resync_handle: Mutex<Option<AutoResyncHandle>>,
    failure_callback: SharedClockSyncFailureCallback,
}

struct AutoResyncHandle {
    sender: UnboundedSender<ClockSyncCommand>,
    join: JoinHandle<()>,
}

pub enum ClockSyncCommand {
    SetFrequency(Duration),
    Stop,
}

impl<Exchange, Client> AutoResyncConnector<Exchange, Client> {
    pub async fn clock_sync_loop<F, Fut>(
        frequency: Duration,
        first_sync_sender: tokio::sync::oneshot::Sender<PlatformResult<()>>,
        receiver: tokio::sync::mpsc::UnboundedReceiver<ClockSyncCommand>,
        failure_callback: SharedClockSyncFailureCallback,
        sync_clock_fn: F,
    ) where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = PlatformResult<()>> + Send + 'static,
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
                &PlatformError::Connector(ConnectorError::AutoResyncClockPanicked),
            );
            std::panic::resume_unwind(panic.into_payload());
        }
    }

    async fn clock_sync_loop_inner<F, Fut>(
        mut frequency: Duration,
        first_sync_sender: tokio::sync::oneshot::Sender<PlatformResult<()>>,
        mut receiver: tokio::sync::mpsc::UnboundedReceiver<ClockSyncCommand>,
        failure_callback: SharedClockSyncFailureCallback,
        sync_clock_fn: F,
    ) where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = PlatformResult<()>> + Send + 'static,
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

    fn report_failure(failure_callback: &SharedClockSyncFailureCallback, error: &PlatformError) {
        warn!(error = %error, "auto-resync clock sync failed");
        let callback = failure_callback.lock().clone();
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
    pub fn new(connector: Arc<Connector<Exchange, Client>>) -> Self {
        Self {
            connector,
            resync_handle: Mutex::new(None),
            failure_callback: Arc::new(Mutex::new(None)),
        }
    }
    pub fn on_clock_sync_failure<F>(&self, callback: F)
    where
        F: Fn(&PlatformError) + Send + Sync + 'static,
    {
        let mut guard = self.failure_callback.lock();
        *guard = Some(Arc::new(callback));
    }
    pub async fn stop_clock_sync(&self) -> PlatformResult<()> {
        let handle = self.resync_handle.lock().take();
        let Some(handle) = handle else { return Ok(()) };
        debug!("stopping auto-resync clock sync");
        let _ = handle.sender.send(ClockSyncCommand::Stop);
        drop(handle.sender); // let the loop drain
        match handle.join.await {
            Ok(()) => Ok(()),
            Err(e) if e.is_panic() => Err(PlatformError::Connector(
                ConnectorError::AutoResyncClockPanicked,
            )),
            Err(_) => Ok(()),
        }
    }
    async fn set_clock_sync_frequency<F, Fut>(
        &self,
        frequency: Duration,
        sync_clock_fn: F,
    ) -> PlatformResult<()>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = PlatformResult<()>> + Send + 'static,
    {
        if frequency < Duration::from_mins(1) {
            return Err(PlatformError::Connector(
                ConnectorError::InvalidSyncFrequency,
            ));
        }
        let (first_sync_sender, first_sync_receiver) = tokio::sync::oneshot::channel();
        {
            let mut resync_handle = self.resync_handle.lock();
            if let Some(handle) = resync_handle.as_ref() {
                if handle.join.is_finished()
                    || handle
                        .sender
                        .send(ClockSyncCommand::SetFrequency(frequency))
                        .is_err()
                {
                    *resync_handle = None;
                    return Err(PlatformError::Connector(
                        ConnectorError::AutoResyncClockStopped,
                    ));
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
            Err(_) => Err(PlatformError::Connector(
                ConnectorError::AutoResyncClockPanicked,
            )),
        }
    }
}

impl<Exchange, Client> Drop for AutoResyncConnector<Exchange, Client> {
    fn drop(&mut self) {
        let handle = self.resync_handle.get_mut().take();
        if let Some(handle) = handle {
            let _ = handle.sender.send(ClockSyncCommand::Stop);
            handle.join.abort();
        }
    }
}

impl<Exchange, Client> AutoResyncConnector<Exchange, Client>
where
    Exchange: ETExchange + ETExchangeServerTimeHttp,
    Client: HttpClient,
{
    #[allow(unused)]
    pub async fn set_clock_sync_frequency_http(&self, frequency: Duration) -> PlatformResult<()>
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
}

impl<Exchange, Client> AutoResyncConnector<Exchange, Client>
where
    Exchange: ETExchange + ETExchangeServerTimeWebsocket,
    Client: WebsocketClient,
{
    #[allow(unused)]
    pub async fn set_clock_sync_frequency_websocket(
        &self,
        frequency: Duration,
    ) -> PlatformResult<()>
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
}

impl<Exchange, Client> Deref for AutoResyncConnector<Exchange, Client> {
    type Target = Connector<Exchange, Client>;

    fn deref(&self) -> &Self::Target {
        &self.connector
    }
}

impl<Exchange, Client> std::fmt::Debug for AutoResyncConnector<Exchange, Client> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AutoResyncConnector")
            .field("connector", &self.connector)
            .finish()
    }
}
