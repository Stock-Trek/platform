use crate::breaker_permit::BreakerPermit;
use std::time::Duration;

pub enum BreakerDecision<'a> {
    Allowed(BreakerPermit<'a>),
    Rejected(Duration),
}
