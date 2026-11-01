use serde::de::DeserializeOwned;
use stock_trek::errors::{StockTrekError, ValueError};

pub struct JsonString;

impl JsonString {
    pub fn deserialize<T: DeserializeOwned>(value: &str) -> Result<T, StockTrekError> {
        let parse = |candidate: &str| {
            serde_json::from_value::<T>(serde_json::Value::String(candidate.to_string()))
        };
        parse(value)
            .or_else(|_| parse(&to_snake_case(value)))
            .map_err(|_| {
                StockTrekError::Value(ValueError::IncorrectType {
                    expected: std::any::type_name::<T>().to_string(),
                    found: value.to_string(),
                })
            })
    }
}

/// Converts PascalCase enum variants to the snake_case representation used by
/// `stock-trek-types`, while preserving already-snake_case input.
fn to_snake_case(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    for (index, character) in value.chars().enumerate() {
        if index > 0 && character.is_ascii_uppercase() {
            result.push('_');
        }
        result.extend(character.to_lowercase());
    }
    result
}
