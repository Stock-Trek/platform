use crate::streaming::stream_types::TICKET_TTL_SECONDS;
use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use std::collections::HashMap;

struct Ticket {
    owner: String,
    expires_at: DateTime<Utc>,
}

pub struct TicketStore {
    tickets: Mutex<HashMap<String, Ticket>>,
}

impl TicketStore {
    pub fn new() -> Self {
        Self {
            tickets: Mutex::new(HashMap::new()),
        }
    }

    pub fn issue(&self, owner: &str, now: DateTime<Utc>) -> (String, DateTime<Utc>) {
        // TODO: tickets are signed single-use JWTs so they survive a multi-node deployment.
        let ticket = format!("st_{}", uuid::Uuid::new_v4().simple());
        let expires_at = now + Duration::seconds(TICKET_TTL_SECONDS as i64);
        self.tickets.lock().insert(
            ticket.clone(),
            Ticket {
                owner: owner.to_string(),
                expires_at,
            },
        );
        (ticket, expires_at)
    }

    pub fn redeem(&self, ticket: &str, now: DateTime<Utc>) -> Option<String> {
        self.tickets
            .lock()
            .remove(ticket)
            .and_then(|entry| (now < entry.expires_at).then_some(entry.owner))
    }
}
