use serde_json::Value;

pub struct BacktestResults;

impl BacktestResults {
    pub fn collate(simulations: Vec<Value>) -> Value {
        Value::Array(simulations)
    }
}
