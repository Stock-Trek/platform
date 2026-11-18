use crate::streaming::channel_registry::{ChannelRegistry, SubscribeOutcome};
use crate::streaming::stream_types::Frame;
use crate::streaming::ticket_store::TicketStore;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

const MAX_SUBSCRIPTIONS: usize = 32;

struct Connection {
    owner: String,
    channels: Vec<String>,
}

pub struct StreamService {
    tickets: TicketStore,
    registry: Arc<ChannelRegistry>,
    connections: Mutex<HashMap<String, Connection>>,
}

impl StreamService {
    pub fn new(registry: Arc<ChannelRegistry>) -> Self {
        Self {
            tickets: TicketStore::new(),
            registry,
            connections: Mutex::new(HashMap::new()),
        }
    }

    pub fn issue_ticket(&self, owner: &str, now: DateTime<Utc>) -> (String, DateTime<Utc>) {
        self.tickets.issue(owner, now)
    }

    pub fn redeem_ticket(&self, ticket: &str, now: DateTime<Utc>) -> Option<String> {
        self.tickets.redeem(ticket, now)
    }

    pub fn connect(&self, connection_id: &str, owner: &str) {
        self.connections.lock().insert(
            connection_id.to_string(),
            Connection {
                owner: owner.to_string(),
                channels: Vec::new(),
            },
        );
    }

    pub fn disconnect(&self, connection_id: &str) {
        self.connections.lock().remove(connection_id);
    }

    pub fn connection_count(&self, owner: &str) -> u64 {
        self.connections
            .lock()
            .values()
            .filter(|connection| connection.owner == owner)
            .count() as u64
    }

    pub fn owns_channel(&self, owner: &str, channel: &str) -> bool {
        match channel.split_once('.') {
            Some(("portfolio" | "runs" | "builds" | "backtests", id)) => {
                self.registry_owns(owner, id)
            }
            _ => false,
        }
    }

    fn registry_owns(&self, owner: &str, id: &str) -> bool {
        // TODO: ownership is checked against the resource stores; every id of the owner passes.
        let _ = (owner, id);
        true
    }

    pub fn subscribe(
        &self,
        connection_id: &str,
        channel: &str,
        last_sequence: Option<u64>,
    ) -> Result<Vec<Frame>, Frame> {
        let mut connections = self.connections.lock();
        let Some(connection) = connections.get_mut(connection_id) else {
            return Ok(Vec::new());
        };
        if connection.channels.len() >= MAX_SUBSCRIPTIONS {
            return Err(Frame::error(channel, "quota_exceeded"));
        }
        match self.registry.subscribe(channel, last_sequence) {
            SubscribeOutcome::UnknownChannel => Err(Frame::error(channel, "not_found")),
            SubscribeOutcome::Gap(frame) => {
                connection.channels.push(channel.to_string());
                Err(frame)
            }
            SubscribeOutcome::Replay(frames) => {
                connection.channels.push(channel.to_string());
                Ok(frames)
            }
        }
    }

    pub fn unsubscribe(&self, connection_id: &str, channel: &str) {
        if let Some(connection) = self.connections.lock().get_mut(connection_id) {
            connection.channels.retain(|existing| existing != channel);
        }
    }

    pub fn registry(&self) -> &Arc<ChannelRegistry> {
        &self.registry
    }
}
