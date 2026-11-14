use crate::websocket_listener::{senders::Senders, websocket_event_stream::WebsocketEventStream};
use parking_lot::Mutex;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::mpsc::unbounded_channel;

#[derive(Clone)]
pub struct EventSubscribers {
    senders: Senders,
    next_id: Arc<AtomicU64>,
}

impl EventSubscribers {
    pub fn new() -> Self {
        Self {
            senders: Arc::new(Mutex::new(BTreeMap::new())),
            next_id: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn subscribe(&self) -> WebsocketEventStream {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = unbounded_channel();
        self.senders.lock().insert(id, sender);
        WebsocketEventStream {
            id,
            receiver,
            senders: Arc::clone(&self.senders),
        }
    }

    pub fn publish(&self, message: &Value) {
        self.senders
            .lock()
            .retain(|_, sender| sender.send(message.clone()).is_ok());
    }

    pub fn subscriber_count(&self) -> usize {
        self.senders.lock().len()
    }
}

impl Default for EventSubscribers {
    fn default() -> Self {
        Self::new()
    }
}
