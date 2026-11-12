use crate::websocket_listener::{handlers::Handlers, waiter_state::WaiterState};
use parking_lot::Mutex;
use platform_errors::BuildResult;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

pub struct WaiterForResponse {
    pub(super) state: Arc<Mutex<WaiterState>>,
    pub(super) handlers: Handlers,
}

impl Future for WaiterForResponse {
    type Output = BuildResult<serde_json::Value>;

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
