use crate::{flight_gate::FlightGate, flight_outcome::FlightOutcome};
use parking_lot::Mutex;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex as AsyncMutex;

pub struct FlightGates {
    gates: Mutex<HashMap<String, Arc<Gate>>>,
}

impl FlightGates {
    pub fn new() -> Self {
        Self {
            gates: Mutex::new(HashMap::new()),
        }
    }

    pub fn gate(&self, id: &str) -> FlightGate<'_> {
        let mut gates = self.gates.lock();
        if let Some(gate) = gates.get(id) {
            return FlightGate::new(self, id, gate.clone());
        }
        let gate = Arc::new(Gate::new());
        gates.insert(id.to_string(), gate.clone());
        FlightGate::new(self, id, gate)
    }

    pub(super) fn remove_gate_if_last(&self, id: &str, gate: &Arc<Gate>) {
        let mut gates = self.gates.lock();
        let is_last = gates
            .get(id)
            .is_some_and(|current| Arc::ptr_eq(current, gate) && Arc::strong_count(current) == 2);
        if is_last {
            gates.remove(id);
        }
    }
}

pub(super) struct Gate {
    pub(super) lock: AsyncMutex<()>,
    pub(super) outcome: Mutex<Option<FlightOutcome>>,
}

impl Gate {
    fn new() -> Self {
        Self {
            lock: AsyncMutex::new(()),
            outcome: Mutex::new(None),
        }
    }
}
