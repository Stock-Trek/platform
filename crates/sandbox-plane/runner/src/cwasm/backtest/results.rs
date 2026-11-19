use serde_json::Value;

pub(super) struct BacktestResults;

impl BacktestResults {
    pub(super) fn collate(simulations: Vec<Value>) -> Value {
        Value::Array(simulations)
    }
}
