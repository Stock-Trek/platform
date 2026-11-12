use crate::ApiToken;
use parking_lot::Mutex;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};

pub(crate) struct FlightGates {
    gates: Mutex<HashMap<String, Arc<Gate>>>,
}

impl FlightGates {
    pub(crate) fn new() -> Self {
        Self {
            gates: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn gate(&self, id: &str) -> FlightGate<'_> {
        let mut gates = self.gates.lock();
        if let Some(gate) = gates.get(id) {
            return FlightGate::new(self, id, gate.clone());
        }
        let gate = Arc::new(Gate::new());
        gates.insert(id.to_string(), gate.clone());
        FlightGate::new(self, id, gate)
    }
}

struct Gate {
    lock: AsyncMutex<()>,
    outcome: Mutex<Option<FlightOutcome>>,
}

impl Gate {
    fn new() -> Self {
        Self {
            lock: AsyncMutex::new(()),
            outcome: Mutex::new(None),
        }
    }
}

#[derive(Clone)]
pub(crate) enum FlightOutcome {
    Found(Option<Arc<ApiToken>>),
    Failed,
}

pub(crate) struct FlightGate<'a> {
    owner: &'a FlightGates,
    id: String,
    gate: Arc<Gate>,
}

impl<'a> FlightGate<'a> {
    fn new(owner: &'a FlightGates, id: &str, gate: Arc<Gate>) -> Self {
        Self {
            owner,
            id: id.to_string(),
            gate,
        }
    }

    pub(crate) async fn lock(&self) -> FlightGuard<'_> {
        let guard = self.gate.lock.lock().await;
        FlightGuard {
            gate: &self.gate,
            _guard: guard,
        }
    }
}

impl Drop for FlightGate<'_> {
    fn drop(&mut self) {
        let mut gates = self.owner.gates.lock();
        let is_last = gates
            .get(&self.id)
            .is_some_and(|gate| Arc::ptr_eq(gate, &self.gate) && Arc::strong_count(gate) == 2);
        if is_last {
            gates.remove(&self.id);
        }
    }
}

pub(crate) struct FlightGuard<'a> {
    gate: &'a Gate,
    _guard: AsyncMutexGuard<'a, ()>,
}

impl FlightGuard<'_> {
    pub(crate) fn outcome(&self) -> Option<FlightOutcome> {
        self.gate.outcome.lock().clone()
    }

    pub(crate) fn complete(&self, outcome: FlightOutcome) {
        *self.gate.outcome.lock() = Some(outcome);
    }
}
