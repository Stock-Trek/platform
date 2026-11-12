use platform_errors::{BuildError, BuildResult};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(super) struct UpdateCheckpoint {
    sequence: u64,
}

impl UpdateCheckpoint {
    pub(super) fn new(sequence: u64) -> Self {
        Self { sequence }
    }

    pub(super) fn sequence(&self) -> u64 {
        self.sequence
    }

    pub(super) fn to_bytes(&self) -> BuildResult<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub(super) fn from_bytes(payload: &[u8]) -> BuildResult<Self> {
        serde_json::from_slice(payload).map_err(BuildError::external)
    }
}
