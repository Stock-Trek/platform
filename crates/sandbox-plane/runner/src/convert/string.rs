use serde::de::DeserializeOwned;
use stock_trek::errors::{StockTrekError, ValueError};

pub struct JsonString;

impl JsonString {
    pub fn deserialize<T: DeserializeOwned>(value: &str) -> Result<T, StockTrekError> {
        serde_json::from_value(serde_json::Value::String(value.to_string())).map_err(|_| {
            StockTrekError::Value(ValueError::IncorrectType {
                expected: std::any::type_name::<T>().to_string(),
                found: value.to_string(),
            })
        })
    }
}

pub fn from_string<T: DeserializeOwned>(value: &str) -> Result<T, StockTrekError> {
    JsonString::deserialize(value)
}
