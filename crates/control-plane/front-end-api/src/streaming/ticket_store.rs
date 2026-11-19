use crate::auth::JwtSigner;
use crate::streaming::stream_types::TICKET_TTL_SECONDS;
use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Serialize)]
struct TicketClaims {
    sub: String,
    sid: String,
    jti: String,
    iat: i64,
    exp: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VerifiedTicketClaims {
    sub: String,
    sid: String,
    jti: String,
    iat: i64,
    exp: i64,
}

pub(super) struct TicketStore {
    signer: JwtSigner,
    outstanding: Mutex<HashSet<String>>,
}

impl TicketStore {
    pub(super) fn new() -> Self {
        Self {
            signer: JwtSigner::generate(),
            outstanding: Mutex::new(HashSet::new()),
        }
    }

    pub(super) fn issue(
        &self,
        owner: &str,
        session_id: &str,
        now: DateTime<Utc>,
    ) -> (String, DateTime<Utc>) {
        let expires_at = now + Duration::seconds(TICKET_TTL_SECONDS as i64);
        let claims = TicketClaims {
            sub: owner.to_string(),
            sid: session_id.to_string(),
            jti: uuid::Uuid::new_v4().simple().to_string(),
            iat: now.timestamp(),
            exp: expires_at.timestamp(),
        };
        let ticket = self.signer.mint(&claims);
        self.outstanding.lock().insert(claims.jti);
        (ticket, expires_at)
    }

    pub(super) fn redeem(&self, ticket: &str, now: DateTime<Utc>) -> Option<(String, String)> {
        let claims = self.signer.verify::<VerifiedTicketClaims>(ticket)?;
        if claims.exp <= now.timestamp() || claims.iat > claims.exp {
            return None;
        }
        self.outstanding
            .lock()
            .remove(&claims.jti)
            .then_some((claims.sub, claims.sid))
    }
}
