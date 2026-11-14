use crate::ApiToken;
use std::sync::Arc;

#[derive(Clone)]
pub enum FlightOutcome {
    Found(Option<Arc<ApiToken>>),
    Failed,
}
