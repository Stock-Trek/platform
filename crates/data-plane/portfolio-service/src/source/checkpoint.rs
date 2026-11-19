use platform_errors::PlatformResult;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(crate) struct Checkpoint {
    sequence: u64,
}

impl Checkpoint {
    pub(crate) fn new(sequence: u64) -> Self {
        Self { sequence }
    }

    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }

    pub(crate) fn to_bytes(&self) -> PlatformResult<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub(crate) fn from_bytes(payload: &[u8]) -> PlatformResult<Self> {
        Ok(serde_json::from_slice(payload)?)
    }
}
