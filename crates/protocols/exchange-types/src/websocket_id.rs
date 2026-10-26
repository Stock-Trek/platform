use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(untagged)]
#[non_exhaustive]
pub enum ETWebsocketId {
    Int(i64),
    Str(String),
}

impl From<i64> for ETWebsocketId {
    fn from(id: i64) -> Self {
        Self::Int(id)
    }
}

impl From<String> for ETWebsocketId {
    fn from(id: String) -> Self {
        Self::Str(id)
    }
}

impl From<&str> for ETWebsocketId {
    fn from(id: &str) -> Self {
        Self::Str(id.into())
    }
}
