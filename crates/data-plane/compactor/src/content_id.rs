use platform_archive::ArchiveRecord;
use platform_errors::{BpError, BpResult};

const CONTENT_ID_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const CONTENT_ID_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentId {
    hash: u64,
}

impl ContentId {
    pub(crate) fn new() -> Self {
        Self {
            hash: CONTENT_ID_OFFSET_BASIS,
        }
    }

    pub(crate) fn update(&mut self, record: &ArchiveRecord) -> BpResult<()> {
        let bytes = serde_json::to_vec(record).map_err(BpError::external)?;
        for byte in bytes {
            self.hash ^= u64::from(byte);
            self.hash = self.hash.wrapping_mul(CONTENT_ID_PRIME);
        }
        Ok(())
    }

    pub(crate) fn value(self) -> u64 {
        self.hash
    }
}

#[cfg(test)]
mod tests;
