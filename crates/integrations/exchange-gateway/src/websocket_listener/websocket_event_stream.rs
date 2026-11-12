use super::senders::Senders;
use serde_json::Value;
use tokio::sync::mpsc::UnboundedReceiver;

pub struct WebsocketEventStream {
    pub(super) id: u64,
    pub(super) receiver: UnboundedReceiver<Value>,
    pub(super) senders: Senders,
}

impl WebsocketEventStream {
    pub async fn next(&mut self) -> Option<Value> {
        self.receiver.recv().await
    }
}

impl Drop for WebsocketEventStream {
    fn drop(&mut self) {
        self.senders.lock().remove(&self.id);
    }
}
