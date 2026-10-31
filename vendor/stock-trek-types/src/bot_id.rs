#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BotId(pub String);

impl BotId {
    pub fn new(bot_id: &str) -> Self {
        BotId(bot_id.to_string())
    }
}
