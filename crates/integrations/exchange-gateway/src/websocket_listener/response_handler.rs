use super::{arc_predicate::ArcPredicate, waiter_state::WaiterState};
use crate::panic_guard::PanicUtils;
use parking_lot::Mutex;
use platform_errors::{BpError, BpResult, ConnectorError};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ResponseHandler {
    pub(super) state: Arc<Mutex<WaiterState>>,
    filter: ArcPredicate<serde_json::Value>,
}

impl ResponseHandler {
    pub fn new(state: Arc<Mutex<WaiterState>>, filter: ArcPredicate<serde_json::Value>) -> Self {
        Self { state, filter }
    }

    pub fn handle(&self, response: &serde_json::Value) -> BpResult<bool> {
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
