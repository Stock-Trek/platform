use crate::{
    flight_gates::{FlightGates, Gate},
    flight_guard::FlightGuard,
};
use std::sync::Arc;

pub struct FlightGate<'a> {
    owner: &'a FlightGates,
    id: String,
    gate: Arc<Gate>,
}

impl<'a> FlightGate<'a> {
    pub(super) fn new(owner: &'a FlightGates, id: &str, gate: Arc<Gate>) -> Self {
        Self {
            owner,
            id: id.to_string(),
            gate,
        }
    }

    pub async fn lock(&self) -> FlightGuard<'_> {
        let guard = self.gate.lock.lock().await;
        FlightGuard::new(&self.gate, guard)
    }
}

impl Drop for FlightGate<'_> {
    fn drop(&mut self) {
        self.owner.remove_gate_if_last(&self.id, &self.gate);
    }
}
