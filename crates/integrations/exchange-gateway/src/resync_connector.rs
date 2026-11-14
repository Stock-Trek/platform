use crate::{connector::Connector, panic_guard::PanicUtils, submission::Submission};
use parking_lot::Mutex;
use platform_errors::{BuildError, BuildResult, ConnectorError};
use platform_exchange_types::{
    exchange::ETExchange,
    exchange_server_time::{ETExchangeServerTimeHttp, ETExchangeServerTimeWebsocket},
    http::HttpClient,
    rate_limited::RateLimit,
    request::{ETHttpRequest, ETWebsocketRequest},
    websocket::WebsocketClient,
};
use platform_types::{TimestampMilliseconds, UsageCount};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{sync::mpsc::UnboundedSender, task::JoinHandle};
use tracing::{debug, warn};

pub(crate) type ClockSyncFailureCallback = Arc<dyn Fn(&BuildError) + Send + Sync>;
pub(crate) type SharedClockSyncFailureCallback = Arc<Mutex<Option<ClockSyncFailureCallback>>>;

pub struct AutoResyncConnector<Exchange, Client> {
    connector: Arc<Connector<Exchange, Client>>,
    resync_handle: Mutex<Option<AutoResyncHandle>>,
    failure_callback: SharedClockSyncFailureCallback,
}

struct AutoResyncHandle {
    sender: UnboundedSender<ClockSyncCommand>,
    join: JoinHandle<()>,
}

pub(crate) enum ClockSyncCommand {
    SetFrequency(Duration),
    Stop,
}

impl<Exchange, Client> AutoResyncConnector<Exchange, Client> {
    pub(crate) async fn clock_sync_loop<F, Fut>(
        frequency: Duration,
        first_sync_sender: tokio::sync::oneshot::Sender<BuildResult<()>>,
        receiver: tokio::sync::mpsc::UnboundedReceiver<ClockSyncCommand>,
        failure_callback: SharedClockSyncFailureCallback,
        sync_clock_fn: F,
    ) where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = BuildResult<()>> + Send + 'static,
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
                &BuildError::Connector(ConnectorError::AutoResyncClockPanicked),
            );
            std::panic::resume_unwind(panic.into_payload());
        }
    }

    async fn clock_sync_loop_inner<F, Fut>(
        mut frequency: Duration,
        first_sync_sender: tokio::sync::oneshot::Sender<BuildResult<()>>,
        mut receiver: tokio::sync::mpsc::UnboundedReceiver<ClockSyncCommand>,
        failure_callback: SharedClockSyncFailureCallback,
        sync_clock_fn: F,
    ) where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = BuildResult<()>> + Send + 'static,
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

    fn report_failure(failure_callback: &SharedClockSyncFailureCallback, error: &BuildError) {
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
    pub(crate) fn new(connector: Arc<Connector<Exchange, Client>>) -> Self {
        Self {
            connector,
            resync_handle: Mutex::new(None),
            failure_callback: Arc::new(Mutex::new(None)),
        }
    }
    pub fn duration_since_last_sync(&self) -> BuildResult<Option<Duration>> {
        self.connector.duration_since_last_sync()
    }
    pub fn remaining_rate_limit_capacity(&self) -> BuildResult<HashMap<RateLimit, UsageCount>> {
        self.connector.remaining_rate_limit_capacity()
    }
    pub fn server_time_estimate(&self) -> BuildResult<TimestampMilliseconds> {
        self.connector.server_time_estimate()
    }
    pub fn on_clock_sync_failure<F>(&self, callback: F)
    where
        F: Fn(&BuildError) + Send + Sync + 'static,
    {
        let mut guard = self.failure_callback.lock();
        *guard = Some(Arc::new(callback));
    }
    pub async fn stop_clock_sync(&self) -> BuildResult<()> {
        let handle = self.resync_handle.lock().take();
        let Some(handle) = handle else { return Ok(()) };
        debug!("stopping auto-resync clock sync");
        let _ = handle.sender.send(ClockSyncCommand::Stop);
        drop(handle.sender); // let the loop drain
        match handle.join.await {
            Ok(()) => Ok(()),
            Err(e) if e.is_panic() => Err(BuildError::Connector(
                ConnectorError::AutoResyncClockPanicked,
            )),
            Err(_) => Ok(()),
        }
    }
    async fn set_clock_sync_frequency<F, Fut>(
        &self,
        frequency: Duration,
        sync_clock_fn: F,
    ) -> BuildResult<()>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = BuildResult<()>> + Send + 'static,
    {
        if frequency < Duration::from_mins(1) {
            return Err(BuildError::Connector(ConnectorError::InvalidSyncFrequency));
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
                    return Err(BuildError::Connector(
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
            Err(_) => Err(BuildError::Connector(
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
    pub async fn sync_clock_http(&self) -> BuildResult<()> {
        self.connector.sync_clock_http().await
    }
    #[allow(unused)]
    pub async fn set_clock_sync_frequency_http(&self, frequency: Duration) -> BuildResult<()>
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
    ) -> BuildResult<Submission<'connector, Request, Request::Response, Request::VerificationRequest>>
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
    Exchange: ETExchange + ETExchangeServerTimeWebsocket,
    Client: WebsocketClient,
{
    pub async fn connect(&self) -> BuildResult<()> {
        self.connector.connect().await
    }
    pub fn is_connected(&self) -> BuildResult<bool> {
        self.connector.is_connected()
    }
    pub async fn disconnect(&self) -> BuildResult<()> {
        self.connector.disconnect().await
    }
    pub async fn sync_clock_websocket(&self) -> BuildResult<()> {
        self.connector.sync_clock_websocket().await
    }
    #[allow(unused)]
    pub async fn set_clock_sync_frequency_websocket(&self, frequency: Duration) -> BuildResult<()>
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
    ) -> BuildResult<Submission<'connector, Request, Request::Response, Request::VerificationRequest>>
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
