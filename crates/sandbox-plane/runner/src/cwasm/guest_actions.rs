use serde_json::Value;
use std::sync::{Arc, Mutex};

use crate::cwasm::Emit;
use crate::cwasm::guest_state::GuestState;

#[derive(Clone, Default)]
pub struct GuestActions {
    actions: Arc<Mutex<Vec<String>>>,
}

impl GuestActions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn emit(&self) -> Emit {
        let actions = self.clone();
        Arc::new(move |kind, payload| {
            Self::capture(&actions, kind, &payload);
            GuestState::emit_stdout(kind, payload);
        })
    }

    pub fn take(&self) -> Vec<String> {
        std::mem::take(
            &mut *self
                .actions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    fn capture(actions: &GuestActions, kind: &str, payload: &Value) {
        if kind != "action" {
            return;
        }
        if let Some(action) = payload.get("action").and_then(Value::as_str) {
            actions
                .actions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(action.to_string());
        }
    }
}
