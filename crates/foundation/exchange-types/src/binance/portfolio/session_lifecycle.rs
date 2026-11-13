use crate::{
    binance::exchange::BinanceExchange,
    portfolio::{RenewalSchedule, SessionLifecycle},
};
use std::time::Duration;

const MAX_SESSION_LIFETIME: Duration = Duration::from_hours(24);
const MAX_RENEWAL_LEAD_TIME: Duration = Duration::from_secs(30);

impl BinanceExchange {
    pub fn portfolio_session_lifecycle(refresh_interval: Duration) -> SessionLifecycle {
        SessionLifecycle::new(
            Some(MAX_SESSION_LIFETIME),
            Some(RenewalSchedule::new(
                refresh_interval,
                refresh_interval.min(MAX_RENEWAL_LEAD_TIME),
            )),
            true,
            true,
        )
    }
}
