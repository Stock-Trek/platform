use platform_errors::{BpError, BpResult, SystemError};
use serde::{Deserialize, Serialize};
use std::{
    hash::{DefaultHasher, Hash, Hasher},
    time::UNIX_EPOCH,
};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimeOrderedId(pub String);

impl TimeOrderedId {
    pub fn try_new<T>(hashable: &T) -> BpResult<Self>
    where
        T: Hash,
    {
        let mut state = DefaultHasher::new();
        hashable.hash(&mut state);
        let high_bits = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| BpError::System(SystemError::SystemTimeBeforeEpoch))?
            .as_millis() as u64;
        let low_bits = state.finish();
        let id = Uuid::from_u64_pair(high_bits, low_bits).to_string();
        Ok(Self(id))
    }
}
