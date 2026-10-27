use platform_errors::IngesterError;

/// Cursor over a raw binary exchange frame.
///
/// Offsets are absolute within the frame and numeric reads are little-endian,
/// matching the binary formats exposed by supported exchanges.
#[derive(Debug, Clone, Copy)]
pub struct BinaryCursor<'a> {
    frame: &'a [u8],
}

impl<'a> BinaryCursor<'a> {
    pub fn new(frame: &'a [u8]) -> Self {
        Self { frame }
    }

    pub fn get(&self, offset: usize, length: usize) -> Result<&'a [u8], IngesterError> {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| IngesterError::malformed_binary("frame offset overflow"))?;
        self.frame.get(offset..end).ok_or_else(|| {
            IngesterError::malformed_binary(format!(
                "frame is too short for {length} bytes at offset {offset}"
            ))
        })
    }

    pub fn read_u8(&self, offset: usize) -> Result<u8, IngesterError> {
        Ok(self.get(offset, 1)?[0])
    }

    pub fn read_u16(&self, offset: usize) -> Result<u16, IngesterError> {
        let bytes: [u8; 2] = self.get(offset, 2)?.try_into().map_err(|_| {
            IngesterError::malformed_binary(format!("invalid u16 at offset {offset}"))
        })?;
        Ok(u16::from_le_bytes(bytes))
    }

    pub fn read_u32(&self, offset: usize) -> Result<u32, IngesterError> {
        let bytes: [u8; 4] = self.get(offset, 4)?.try_into().map_err(|_| {
            IngesterError::malformed_binary(format!("invalid u32 at offset {offset}"))
        })?;
        Ok(u32::from_le_bytes(bytes))
    }

    pub fn read_i64(&self, offset: usize) -> Result<i64, IngesterError> {
        let bytes: [u8; 8] = self.get(offset, 8)?.try_into().map_err(|_| {
            IngesterError::malformed_binary(format!("invalid i64 at offset {offset}"))
        })?;
        Ok(i64::from_le_bytes(bytes))
    }

    pub fn read_sequence(&self, offset: usize) -> Result<u64, IngesterError> {
        let value = self.read_i64(offset)?;
        u64::try_from(value).map_err(|_| {
            IngesterError::malformed_binary(format!(
                "negative sequence {value} at offset {offset}"
            ))
        })
    }

    pub fn read_var_string(&self, offset: usize) -> Result<String, IngesterError> {
        let length = self.read_u8(offset)? as usize;
        let start = offset
            .checked_add(1)
            .ok_or_else(|| IngesterError::malformed_binary("string offset overflow"))?;
        let bytes = self.get(start, length)?;
        let value = std::str::from_utf8(bytes).map_err(IngesterError::utf8)?;
        Ok(value.to_string())
    }

    pub fn advance(&self, base: usize, count: usize, stride: usize) -> Result<usize, IngesterError> {
        let length = count
            .checked_mul(stride)
            .and_then(|length| base.checked_add(length))
            .ok_or_else(|| IngesterError::malformed_binary("group size overflow"))?;
        if length > self.frame.len() {
            return Err(IngesterError::malformed_binary(format!(
                "group ends at {length} beyond frame length {}",
                self.frame.len()
            )));
        }
        Ok(length)
    }
}
