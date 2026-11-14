use crate::websocket_listener::{
    arc_predicate::ArcPredicate, event_subscribers::EventSubscribers, handlers::Handlers,
    response_handler::ResponseHandler, waiter_for_response::WaiterForResponse,
    waiter_state::WaiterState, websocket_event_stream::WebsocketEventStream,
};
use parking_lot::Mutex;
use platform_errors::BuildResult;
use std::sync::Arc;

#[derive(Clone)]
pub struct WebsocketListener {
    pub handlers: Handlers,
    pub events: EventSubscribers,
}

impl WebsocketListener {
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(Mutex::new(Vec::new())),
            events: EventSubscribers::new(),
        }
    }

    pub async fn on_message(&self, message: serde_json::Value) -> BuildResult<()> {
        self.events.publish(&message);
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

    pub fn subscribe(&self) -> WebsocketEventStream {
        self.events.subscribe()
    }

    pub fn waiter_for_filtered_response(
        &self,
        filter: ArcPredicate<serde_json::Value>,
    ) -> BuildResult<WaiterForResponse> {
        let state = Arc::new(Mutex::new(WaiterState::default()));
        let handler = ResponseHandler::new(Arc::clone(&state), filter);
        self.handlers.lock().push(handler);
        Ok(WaiterForResponse {
            state,
            handlers: Arc::clone(&self.handlers),
        })
    }
}

impl Default for WebsocketListener {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for WebsocketListener {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let handlers = self.handlers.lock().len();
        formatter
            .debug_struct("WebsocketListener")
            .field("handlers", &handlers)
            .field("subscribers", &self.events.subscriber_count())
            .finish()
    }
}
