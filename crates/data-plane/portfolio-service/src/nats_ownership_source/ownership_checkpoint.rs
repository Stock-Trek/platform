use platform_errors::{BpError, BpResult};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(super) struct OwnershipCheckpoint {
    sequence: u64,
}

impl OwnershipCheckpoint {
    pub(super) fn new(sequence: u64) -> Self {
        Self { sequence }
    }

    pub(super) fn sequence(&self) -> u64 {
        self.sequence
    }

    pub(super) fn to_bytes(&self) -> BpResult<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub(super) fn from_bytes(payload: &[u8]) -> BpResult<Self> {
        serde_json::from_slice(payload).map_err(BpError::external)
    }
}
