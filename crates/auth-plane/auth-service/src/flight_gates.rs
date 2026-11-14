use crate::ApiToken;
use parking_lot::Mutex;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};

#[derive(Clone)]
pub enum FlightOutcome {
    Found(Option<Arc<ApiToken>>),
    Failed,
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
