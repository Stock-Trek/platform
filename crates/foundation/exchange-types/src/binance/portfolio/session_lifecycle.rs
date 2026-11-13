use crate::portfolio::{RenewalSchedule, SessionLifecyclePolicy};
use std::time::Duration;

const MAX_SESSION_LIFETIME: Duration = Duration::from_hours(24);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinanceSessionLifecyclePolicy {
    max_session_lifetime: Option<Duration>,
    scheduled_renewal: Option<RenewalSchedule>,
    reconnect_reuses_credential: bool,
    revoke_on_close: bool,
}

impl BinanceSessionLifecyclePolicy {
    pub fn new(
        max_session_lifetime: Option<Duration>,
        scheduled_renewal: Option<RenewalSchedule>,
        reconnect_reuses_credential: bool,
        revoke_on_close: bool,
    ) -> Self {
        Self {
            max_session_lifetime,
            scheduled_renewal,
            reconnect_reuses_credential,
            revoke_on_close,
        }
    }

    pub fn for_refresh_interval(refresh_interval: Duration) -> Self {
        Self::new(
            Some(MAX_SESSION_LIFETIME),
            Some(RenewalSchedule::new(
                refresh_interval,
                refresh_interval.min(Duration::from_secs(30)),
            )),
            true,
            true,
        )
    }
}

pub type BinanceSessionLifecycle = BinanceSessionLifecyclePolicy;

impl Default for BinanceSessionLifecyclePolicy {
    fn default() -> Self {
        Self::for_refresh_interval(Duration::from_mins(30))
    }
}

impl SessionLifecyclePolicy for BinanceSessionLifecyclePolicy {
    fn max_session_lifetime(&self) -> Option<Duration> {
        self.max_session_lifetime
    }

    fn scheduled_renewal(&self) -> Option<RenewalSchedule> {
        self.scheduled_renewal
    }

    fn reconnect_reuses_credential(&self) -> bool {
        self.reconnect_reuses_credential
    }

    fn revoke_on_close(&self) -> bool {
        self.revoke_on_close
    }
}
