use crate::accounts::AccountService;
use crate::algorithms::BuildStore;
use crate::backtests::BacktestStore;
use crate::bots::RunStore;
use crate::streaming::channel_registry::{ChannelRegistry, SubscribeOutcome};
use crate::streaming::stream_types::Frame;
use crate::streaming::ticket_store::TicketStore;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

const MAX_SUBSCRIPTIONS: usize = 32;

pub const MAX_SUBSCRIPTIONS_PER_CONNECTION: usize = MAX_SUBSCRIPTIONS;

struct Connection {
    owner: String,
    channels: Vec<String>,
}

pub struct StreamService {
    tickets: TicketStore,
    registry: Arc<ChannelRegistry>,
    connections: Mutex<HashMap<String, Connection>>,
    accounts: Arc<AccountService>,
    builds: Arc<BuildStore>,
    runs: Arc<RunStore>,
    backtests: Arc<BacktestStore>,
}

impl StreamService {
    pub fn new(
        registry: Arc<ChannelRegistry>,
        accounts: Arc<AccountService>,
        builds: Arc<BuildStore>,
        runs: Arc<RunStore>,
        backtests: Arc<BacktestStore>,
    ) -> Self {
        Self {
            tickets: TicketStore::new(),
            registry,
            connections: Mutex::new(HashMap::new()),
            accounts,
            builds,
            runs,
            backtests,
        }
    }

    pub fn issue_ticket(
        &self,
        owner: &str,
        session_id: &str,
        now: DateTime<Utc>,
    ) -> (String, DateTime<Utc>) {
        self.tickets.issue(owner, session_id, now)
    }

    pub fn redeem_ticket(&self, ticket: &str, now: DateTime<Utc>) -> Option<(String, String)> {
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

    pub fn subscription_count(&self, owner: &str) -> u64 {
        self.connections
            .lock()
            .values()
            .filter(|connection| connection.owner == owner)
            .map(|connection| connection.channels.len() as u64)
            .max()
            .unwrap_or(0)
    }

    pub fn owns_channel(&self, owner: &str, channel: &str) -> bool {
        match channel.split_once('.') {
            Some(("portfolio", id)) => self.accounts.get(owner, id).is_ok(),
            Some(("runs", id)) => self.runs.get(owner, id).is_some(),
            Some(("builds", id)) => self.builds.owns_build(owner, id),
            Some(("backtests", id)) => self.backtests.status(owner, id).is_some(),
            _ => false,
        }
    }

    pub fn subscribe(
        &self,
        connection_id: &str,
        channel: &str,
        last_sequence: Option<u64>,
    ) -> SubscribeOutcome {
        let mut connections = self.connections.lock();
        if !connections.contains_key(connection_id) {
            return SubscribeOutcome::UnknownChannel;
        }
        let connection = connections.get_mut(connection_id).expect("checked above");
        if connection.channels.len() >= MAX_SUBSCRIPTIONS {
            return SubscribeOutcome::Gap(Frame::error(channel, "quota_exceeded"));
        }
        match self.registry.subscribe(channel, last_sequence) {
            SubscribeOutcome::Joined { replay, live } => {
                connection.channels.push(channel.to_string());
                SubscribeOutcome::Joined { replay, live }
            }
            outcome => outcome,
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
