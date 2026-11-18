use crate::auth::JwtSigner;
use crate::streaming::stream_types::TICKET_TTL_SECONDS;
use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Serialize)]
struct TicketClaims {
    sub: String,
    jti: String,
    iat: i64,
    exp: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VerifiedTicketClaims {
    sub: String,
    jti: String,
    exp: i64,
}

pub struct TicketStore {
    signer: JwtSigner,
    outstanding: Mutex<HashSet<String>>,
}

impl TicketStore {
    pub fn new() -> Self {
        Self {
            signer: JwtSigner::generate(),
            outstanding: Mutex::new(HashSet::new()),
        }
    }

    pub fn issue(&self, owner: &str, now: DateTime<Utc>) -> (String, DateTime<Utc>) {
        let expires_at = now + Duration::seconds(TICKET_TTL_SECONDS as i64);
        let claims = TicketClaims {
            sub: owner.to_string(),
            jti: uuid::Uuid::new_v4().simple().to_string(),
            iat: now.timestamp(),
            exp: expires_at.timestamp(),
        };
        let ticket = self.signer.mint(&claims);
        self.outstanding.lock().insert(claims.jti);
        (ticket, expires_at)
    }

    pub fn redeem(&self, ticket: &str, now: DateTime<Utc>) -> Option<String> {
        let claims = self.signer.verify::<VerifiedTicketClaims>(ticket)?;
        if claims.exp <= now.timestamp() {
            return None;
        }
        self.outstanding
            .lock()
            .remove(&claims.jti)
            .then_some(claims.sub)
    }
}
