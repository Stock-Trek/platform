use crate::panic_guard::PanicUtils;
use parking_lot::Mutex;
use platform_errors::{BpError, BpResult, ConnectorError};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

type Handlers = Arc<Mutex<Vec<ResponseHandler>>>;

#[derive(Clone)]
pub struct WebsocketListener {
    pub(crate) handlers: Handlers,
}

impl WebsocketListener {
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub async fn on_message(&self, message: serde_json::Value) -> BpResult<()> {
        let handlers = self.handlers.lock().clone();
        for handler in handlers {
            if handler.handle(&message)? {
                self.handlers
                    .lock()
                    .retain(|existing| !Arc::ptr_eq(&existing.state, &handler.state));
                break;
            }
        }
        Ok(())
    }
    pub fn waiter_for_filtered_response(
        &self,
        filter: ArcPredicate<serde_json::Value>,
    ) -> BpResult<WaiterForResponse> {
        let state = Arc::new(Mutex::new(WaiterState::default()));
        let handler = ResponseHandler {
            state: state.clone(),
            filter,
        };
        self.handlers.lock().push(handler);
        Ok(WaiterForResponse {
            state,
            handlers: self.handlers.clone(),
        })
    }
}

impl Default for WebsocketListener {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for WebsocketListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let handlers = self.handlers.lock().len();
        f.debug_struct("WebsocketListener")
            .field("handlers", &handlers)
            .finish()
    }
}

pub struct WaiterForResponse {
    state: Arc<Mutex<WaiterState>>,
    handlers: Handlers,
}

impl Future for WaiterForResponse {
    type Output = BpResult<serde_json::Value>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.lock();
        if let Some(msg) = state.filtered_response.take() {
            Poll::Ready(Ok(msg))
        } else if let Some(error) = state.error.take() {
            Poll::Ready(Err(error))
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl Drop for WaiterForResponse {
    fn drop(&mut self) {
        self.handlers
            .lock()
            .retain(|handler| !Arc::ptr_eq(&handler.state, &self.state));
    }
}

type ArcPredicate<T> = Arc<dyn for<'a> Fn(&'a T) -> bool + Send + Sync>;

#[derive(Clone)]
pub(crate) struct ResponseHandler {
    state: Arc<Mutex<WaiterState>>,
    filter: ArcPredicate<serde_json::Value>,
}

impl ResponseHandler {
    fn handle(&self, response: &serde_json::Value) -> BpResult<bool> {
        let is_handled = match PanicUtils::catch_panic(|| (self.filter)(response)) {
            Ok(is_handled) => is_handled,
            Err(panic) => {
                let error = BpError::Connector(ConnectorError::CallbackPanicked(
                    panic.message().to_string(),
                ));
                let mut state = self.state.lock();
                state.error = Some(error);
                if let Some(waker) = state.waker.take() {
                    waker.wake();
                }
                return Ok(true);
            }
        };
        if is_handled {
            let mut state = self.state.lock();
            state.filtered_response = Some(response.clone());
            if let Some(waker) = state.waker.take() {
                waker.wake();
            }
        }
        Ok(is_handled)
    }
}

#[derive(Default)]
struct WaiterState {
    filtered_response: Option<serde_json::Value>,
    error: Option<BpError>,
    waker: Option<Waker>,
}
