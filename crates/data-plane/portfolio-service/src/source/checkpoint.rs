use platform_errors::PlatformResult;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Checkpoint {
    sequence: u64,
}

impl Checkpoint {
    pub fn new(sequence: u64) -> Self {
        Self { sequence }
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn to_bytes(&self) -> PlatformResult<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn from_bytes(payload: &[u8]) -> PlatformResult<Self> {
        Ok(serde_json::from_slice(payload)?)
    }
}
