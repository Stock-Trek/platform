//! Rebalance quiescence signalling for shard workers.

use std::sync::Arc;
use tokio::sync::watch;

/// A cloneable handle used by a rebalance coordinator to quiesce a worker.
///
/// The worker stops polling its source once quiesced, checkpoints and
/// acknowledges everything it already accepted, and then releases its lease so
/// the next owner can take over the shard.
#[derive(Debug, Clone)]
pub struct QuiesceSignal {
    sender: watch::Sender<bool>,
}

impl Default for QuiesceSignal {
    fn default() -> Self {
        Self::new()
    }
}

impl QuiesceSignal {
    pub fn new() -> Self {
        let (sender, _receiver) = watch::channel(false);
        Self { sender }
    }

    /// Creates a signal together with a dedicated receiver.
    pub fn channel() -> (Self, QuiesceReceiver) {
        let signal = Self::new();
        let receiver = signal.receiver();
        (signal, receiver)
    }

    /// Requests that the worker stops accepting new inputs and drains.
    pub fn quiesce(&self) {
        self.sender.send_replace(true);
    }

    pub fn is_quiesced(&self) -> bool {
        *self.sender.borrow()
    }

    pub fn receiver(&self) -> QuiesceReceiver {
        QuiesceReceiver {
            receiver: self.sender.subscribe(),
        }
    }

    /// Resolves once quiescence has been requested.
    pub async fn wait(&self) {
        self.receiver().wait().await;
    }
}

/// Receives a quiesce request from a [`QuiesceSignal`].
#[derive(Debug, Clone)]
pub struct QuiesceReceiver {
    receiver: watch::Receiver<bool>,
}

impl QuiesceReceiver {
    pub fn is_quiesced(&self) -> bool {
        *self.receiver.borrow()
    }

    /// Resolves once quiescence has been requested.
    pub async fn wait(&mut self) {
        loop {
            if self.is_quiesced() {
                return;
            }
            if self.receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

impl From<QuiesceSignal> for QuiesceReceiver {
    fn from(signal: QuiesceSignal) -> Self {
        signal.receiver()
    }
}

impl From<&QuiesceSignal> for QuiesceReceiver {
    fn from(signal: &QuiesceSignal) -> Self {
        signal.receiver()
    }
}

impl From<Arc<QuiesceSignal>> for QuiesceReceiver {
    fn from(signal: Arc<QuiesceSignal>) -> Self {
        signal.receiver()
    }
}

impl From<watch::Receiver<bool>> for QuiesceReceiver {
    fn from(receiver: watch::Receiver<bool>) -> Self {
        Self { receiver }
    }
}

impl From<watch::Sender<bool>> for QuiesceReceiver {
    fn from(sender: watch::Sender<bool>) -> Self {
        Self {
            receiver: sender.subscribe(),
        }
    }
}
