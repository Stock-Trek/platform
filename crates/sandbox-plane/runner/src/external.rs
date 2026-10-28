use serde::Serialize;
use stock_trek::{
    Command, Preferences,
    actions::{RecoveryPolicy, ResolvedAction},
    errors::{StockTrekError, StockTrekResult, ValueError},
    signals::Signals,
};

#[link(wasm_import_module = "env")]
unsafe extern "C" {
    fn queue_metadata(
        name_ptr: *const u8,
        name_len: usize,
        description_ptr: *const u8,
        description_len: usize,
    );
    fn queue_preferences(preferences_ptr: *const u8, preferences_len: usize);
    fn queue_signals(signals_ptr: *const u8, signals_len: usize);
    fn queue_strategy(strategy_ptr: *const u8, strategy_len: usize);
    fn queue_action(
        action_ptr: *const u8,
        action_len: usize,
        recovery_policy_ptr: *const u8,
        recovery_policy_len: usize,
    );
}

pub struct Enqueue;

impl Enqueue {
    pub fn metadata(name: &str, description: &str) -> StockTrekResult<()> {
        unsafe {
            queue_metadata(
                name.as_ptr(),
                name.len(),
                description.as_ptr(),
                description.len(),
            );
        }
        Ok(())
    }

    pub fn preferences(preferences: Preferences) -> StockTrekResult<()> {
        let preferences_json = Self::to_json(&preferences)?;
        unsafe {
            queue_preferences(preferences_json.as_ptr(), preferences_json.len());
        }
        Ok(())
    }

    pub fn signals(signals: Signals) -> StockTrekResult<()> {
        let signals_json = Self::to_json(&signals)?;
        unsafe {
            queue_signals(signals_json.as_ptr(), signals_json.len());
        }
        Ok(())
    }

    pub fn strategy(command: Command) -> StockTrekResult<()> {
        let strategy_json = Self::to_json(&command)?;
        unsafe {
            queue_strategy(strategy_json.as_ptr(), strategy_json.len());
        }
        Ok(())
    }

    pub fn action(
        action: &ResolvedAction,
        recovery_policy: &RecoveryPolicy,
    ) -> StockTrekResult<()> {
        let action_json = Self::to_json(action)?;
        let recovery_policy_json = Self::to_json(recovery_policy)?;
        unsafe {
            queue_action(
                action_json.as_ptr(),
                action_json.len(),
                recovery_policy_json.as_ptr(),
                recovery_policy_json.len(),
            );
        }
        Ok(())
    }

    fn to_json<T: Serialize>(body: &T) -> StockTrekResult<String> {
        serde_json::to_string(body).map_err(|_| {
            StockTrekError::Value(ValueError::IncorrectType {
                expected: "serializable".to_string(),
                found: "JSON serialization error".to_string(),
            })
        })
    }
}
