use crate::panic_guard::PanicUtils;
use crate::websocket_listener::{arc_predicate::ArcPredicate, waiter_state::WaiterState};
use parking_lot::Mutex;
use platform_errors::{ConnectorError, PlatformError, PlatformResult};
use std::sync::Arc;

#[derive(Clone)]
pub struct ResponseHandler {
    pub state: Arc<Mutex<WaiterState>>,
    filter: ArcPredicate<serde_json::Value>,
}

impl ResponseHandler {
    pub fn new(state: Arc<Mutex<WaiterState>>, filter: ArcPredicate<serde_json::Value>) -> Self {
        Self { state, filter }
    }

    pub fn handle(&self, response: &serde_json::Value) -> PlatformResult<bool> {
        let is_handled = match PanicUtils::catch_panic(|| (self.filter)(response)) {
            Ok(is_handled) => is_handled,
            Err(panic) => {
                let error = PlatformError::Connector(ConnectorError::CallbackPanicked(
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
