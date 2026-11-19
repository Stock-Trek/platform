use crate::accounts::AccountService;
use crate::algorithms::BuildStore;
use crate::backtests::BacktestStore;
use crate::bots::BotStore;
use crate::streaming::channel_registry::{ChannelRegistry, SubscribeOutcome as RegistryOutcome};
use crate::streaming::stream_types::Frame;
use crate::streaming::ticket_store::TicketStore;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

const MAX_SUBSCRIPTIONS: usize = 32;

pub const MAX_SUBSCRIPTIONS_PER_CONNECTION: usize = MAX_SUBSCRIPTIONS;

pub enum Subscription {
    Joined {
        replay: Vec<Frame>,
        live: tokio::sync::broadcast::Receiver<Frame>,
        epoch: u64,
    },
    Gap(Frame),
    UnknownChannel,
}

struct Connection {
    owner: String,
    epochs: HashMap<String, u64>,
}

pub struct StreamService {
    tickets: TicketStore,
    registry: Arc<ChannelRegistry>,
    connections: Mutex<HashMap<String, Connection>>,
    accounts: Arc<AccountService>,
    builds: Arc<BuildStore>,
    bots: Arc<BotStore>,
    backtests: Arc<BacktestStore>,
}

impl StreamService {
    pub fn new(
        registry: Arc<ChannelRegistry>,
        accounts: Arc<AccountService>,
        builds: Arc<BuildStore>,
        bots: Arc<BotStore>,
        backtests: Arc<BacktestStore>,
    ) -> Self {
        Self {
            tickets: TicketStore::new(),
            registry,
            connections: Mutex::new(HashMap::new()),
            accounts,
            builds,
            bots,
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
                epochs: HashMap::new(),
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
            .map(|connection| connection.epochs.len() as u64)
            .max()
            .unwrap_or(0)
    }

    pub fn owns_channel(&self, owner: &str, channel: &str) -> bool {
        match channel.split_once('.') {
            Some(("portfolio", id)) => self.accounts.get(owner, id).is_ok(),
            Some(("runs", id)) => self.bots.get(owner, id).is_some(),
            Some(("builds", id)) => self.builds.owns_algorithm(owner, id),
            Some(("backtests", id)) => self.backtests.status(owner, id).is_some(),
            _ => false,
        }
    }

    pub fn subscribe(
        &self,
        connection_id: &str,
        channel: &str,
        last_sequence: Option<u64>,
    ) -> Subscription {
        let mut connections = self.connections.lock();
        let Some(connection) = connections.get_mut(connection_id) else {
            return Subscription::UnknownChannel;
        };
        if connection.epochs.len() >= MAX_SUBSCRIPTIONS && !connection.epochs.contains_key(channel)
        {
            return Subscription::Gap(Frame::error(channel, "quota_exceeded"));
        }
        match self.registry.subscribe(channel, last_sequence) {
            RegistryOutcome::Joined { replay, live } => {
                let epoch = connection.epochs.get(channel).copied().unwrap_or(0) + 1;
                connection.epochs.insert(channel.to_string(), epoch);
                Subscription::Joined {
                    replay,
                    live,
                    epoch,
                }
            }
            RegistryOutcome::Gap(frame) => Subscription::Gap(frame),
        }
    }

    pub fn unsubscribe(&self, connection_id: &str, channel: &str) {
        if let Some(connection) = self.connections.lock().get_mut(connection_id) {
            connection.epochs.remove(channel);
        }
    }

    pub fn subscription_epoch(&self, connection_id: &str, channel: &str) -> Option<u64> {
        self.connections
            .lock()
            .get(connection_id)?
            .epochs
            .get(channel)
            .copied()
    }

    pub fn registry(&self) -> &Arc<ChannelRegistry> {
        &self.registry
    }
}
