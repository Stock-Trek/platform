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

pub trait SessionLifecyclePolicy: Send + Sync {
    fn max_session_lifetime(&self) -> Option<Duration>;

    fn scheduled_renewal(&self) -> Option<RenewalSchedule>;

    fn reconnect_reuses_credential(&self) -> bool;

    fn revoke_on_close(&self) -> bool;
}
