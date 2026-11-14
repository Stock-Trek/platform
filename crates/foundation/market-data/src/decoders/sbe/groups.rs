use crate::decoders::binary_cursor::BinaryCursor;
use platform_errors::IngesterError;

#[derive(Debug, Clone, Copy)]
pub struct SbeGroup {
    pub entries_start: usize,
    pub block_length: usize,
    pub count: usize,
}

impl SbeGroup {
    pub fn entry(&self, index: usize) -> Result<usize, IngesterError> {
        if index >= self.count {
            return Err(IngesterError::malformed_sbe(format!(
                "group entry {index} is out of bounds for {} entries",
                self.count
            )));
        }
        index
            .checked_mul(self.block_length)
            .and_then(|offset| self.entries_start.checked_add(offset))
            .ok_or_else(|| IngesterError::malformed_sbe("group entry offset overflow"))
    }

    pub fn end(&self) -> Result<usize, IngesterError> {
        self.count
            .checked_mul(self.block_length)
            .and_then(|length| self.entries_start.checked_add(length))
            .ok_or_else(|| IngesterError::malformed_sbe("group size overflow"))
    }
}

pub trait SbeGroupExt {
    fn read_group32(&mut self) -> Result<SbeGroup, IngesterError>;
    fn read_group16(&mut self) -> Result<SbeGroup, IngesterError>;
}

impl SbeGroupExt for BinaryCursor<'_> {
    fn read_group32(&mut self) -> Result<SbeGroup, IngesterError> {
        let block_length = self.read_u16()? as usize;
        let count = self.read_u32()? as usize;
        Ok(SbeGroup {
            entries_start: self.position(),
            block_length,
            count,
        })
    }

    fn read_group16(&mut self) -> Result<SbeGroup, IngesterError> {
        let block_length = self.read_u16()? as usize;
        let count = self.read_u16()? as usize;
        Ok(SbeGroup {
            entries_start: self.position(),
            block_length,
            count,
        })
    }
}
