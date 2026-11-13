use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RenewalSchedule {
    pub interval: Duration,
    pub lead_time: Duration,
}

impl RenewalSchedule {
    pub fn new(interval: Duration, lead_time: Duration) -> Self {
        Self {
            interval,
            lead_time,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionLifecycle {
    max_session_lifetime: Option<Duration>,
    scheduled_renewal: Option<RenewalSchedule>,
    reconnect_reuses_credential: bool,
    revoke_on_close: bool,
}

impl SessionLifecycle {
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

    pub fn max_session_lifetime(&self) -> Option<Duration> {
        self.max_session_lifetime
    }

    pub fn scheduled_renewal(&self) -> Option<RenewalSchedule> {
        self.scheduled_renewal
    }

    pub fn reconnect_reuses_credential(&self) -> bool {
        self.reconnect_reuses_credential
    }

    pub fn revoke_on_close(&self) -> bool {
        self.revoke_on_close
    }
}
