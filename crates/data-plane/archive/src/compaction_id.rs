use crate::ArchiveKind;
use platform_errors::{PlatformError, PlatformResult};
use platform_types::TimestampMilliseconds;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fmt, str::FromStr};
use uuid::{Builder, Uuid};

pub const COMPACTION_ID_PROPERTY: &str = "archive-compaction-id";
const ID_NAMESPACE: &[u8] = b"platform-archive-compaction-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CompactionId(Uuid);

impl CompactionId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn for_batch(
        kind: ArchiveKind,
        bucket_start: TimestampMilliseconds,
        digest: &str,
    ) -> PlatformResult<Self> {
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "compaction batch digest must be a full sha256 hex digest",
            )));
        }

        let mut hasher = Sha256::new();
        hasher.update(ID_NAMESPACE);
        hasher.update([0_u8]);
        hasher.update(kind.label().as_bytes());
        hasher.update([0_u8]);
        hasher.update(bucket_start.0.to_be_bytes());
        hasher.update([0_u8]);
        hasher.update(digest.as_bytes());
        let hash = hasher.finalize();
        let mut counter = [0_u8; 10];
        counter.copy_from_slice(&hash[..10]);
        let millis = u64::try_from(bucket_start.0.max(0)).unwrap_or_default();
        Ok(Self(
            Builder::from_unix_timestamp_millis(millis, &counter).into_uuid(),
        ))
    }

    pub fn as_uuid(self) -> Uuid {
        self.0
    }

    pub fn as_str(self) -> String {
        self.0.to_string()
    }
}

impl Default for CompactionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for CompactionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for CompactionId {
    type Err = PlatformError;

    fn from_str(value: &str) -> PlatformResult<Self> {
        Uuid::parse_str(value).map(Self).map_err(|error| {
            PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid compaction id {value}: {error}"),
            ))
        })
    }
}

impl TryFrom<String> for CompactionId {
    type Error = PlatformError;

    fn try_from(value: String) -> PlatformResult<Self> {
        value.parse()
    }
}

impl TryFrom<&str> for CompactionId {
    type Error = PlatformError;

    fn try_from(value: &str) -> PlatformResult<Self> {
        value.parse()
    }
}
