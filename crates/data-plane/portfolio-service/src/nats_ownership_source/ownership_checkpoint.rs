use platform_errors::{BuildError, BuildResult};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct OwnershipCheckpoint {
    sequence: u64,
}

impl OwnershipCheckpoint {
    pub fn new(sequence: u64) -> Self {
        Self { sequence }
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn to_bytes(&self) -> BuildResult<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn from_bytes(payload: &[u8]) -> BuildResult<Self> {
        serde_json::from_slice(payload).map_err(BuildError::external)
    }
}
