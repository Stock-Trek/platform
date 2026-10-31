#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Tag(pub String);

impl Tag {
    pub fn new(tag: &str) -> Self {
        Tag(tag.to_string())
    }
}
