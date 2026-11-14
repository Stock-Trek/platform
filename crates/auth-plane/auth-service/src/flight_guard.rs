use crate::{flight_gates::Gate, flight_outcome::FlightOutcome};
use tokio::sync::MutexGuard as AsyncMutexGuard;

pub struct FlightGuard<'a> {
    gate: &'a Gate,
    _guard: AsyncMutexGuard<'a, ()>,
}

impl<'a> FlightGuard<'a> {
    pub(super) fn new(gate: &'a Gate, guard: AsyncMutexGuard<'a, ()>) -> Self {
        Self {
            gate,
            _guard: guard,
        }
    }

    pub fn outcome(&self) -> Option<FlightOutcome> {
        self.gate.outcome.lock().clone()
    }

    pub fn complete(&self, outcome: FlightOutcome) {
        *self.gate.outcome.lock() = Some(outcome);
    }
}
