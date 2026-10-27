use platform_errors::IngesterError;

/// Cursor over a raw binary exchange frame.
///
/// The cursor owns the read position, so consumers read fields in order rather
/// than tracking offsets themselves. Numeric reads are little-endian, matching
/// the binary formats exposed by supported exchanges.
#[derive(Debug, Clone, Copy)]
pub struct BinaryCursor<'a> {
    frame: &'a [u8],
    position: usize,
}

impl<'a> BinaryCursor<'a> {
    pub fn new(frame: &'a [u8]) -> Self {
        Self { frame, position: 0 }
    }

    pub fn position(&self) -> usize {
        self.position
    }

    pub fn remaining(&self) -> usize {
        self.frame.len().saturating_sub(self.position)
    }

    pub fn seek(&mut self, position: usize) -> Result<(), IngesterError> {
        if position > self.frame.len() {
            return Err(IngesterError::malformed_binary(format!(
                "cannot seek to {position} beyond frame length {}",
                self.frame.len()
            )));
        }
        self.position = position;
        Ok(())
    }

    pub fn skip(&mut self, length: usize) -> Result<(), IngesterError> {
        let position = self
            .position
            .checked_add(length)
            .ok_or_else(|| IngesterError::malformed_binary("frame offset overflow"))?;
        self.seek(position)
    }

    pub fn read_bytes(&mut self, length: usize) -> Result<&'a [u8], IngesterError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| IngesterError::malformed_binary("frame offset overflow"))?;
        let bytes = self.frame.get(self.position..end).ok_or_else(|| {
            IngesterError::malformed_binary(format!(
                "frame is too short for {length} bytes at offset {}",
                self.position
            ))
        })?;
        self.position = end;
        Ok(bytes)
    }

    pub fn read_u8(&mut self) -> Result<u8, IngesterError> {
        Ok(self.read_bytes(1)?[0])
    }

    pub fn read_u16(&mut self) -> Result<u16, IngesterError> {
        let bytes = self.read_bytes(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub fn read_u32(&mut self) -> Result<u32, IngesterError> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
        ]))
    }

    pub fn read_i64(&mut self) -> Result<i64, IngesterError> {
        let bytes = self.read_bytes(8)?;
        Ok(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub fn read_sequence(&mut self) -> Result<u64, IngesterError> {
        let value = self.read_i64()?;
        u64::try_from(value).map_err(|_| {
            IngesterError::malformed_binary(format!("negative sequence {value}"))
        })
    }

    pub fn read_var_string(&mut self) -> Result<String, IngesterError> {
        let length = self.read_u8()? as usize;
        let bytes = self.read_bytes(length)?;
        let value = std::str::from_utf8(bytes).map_err(IngesterError::utf8)?;
        Ok(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_advance_the_cursor() {
        let frame = [1u8, 2, 0, 3, 0, 0, 0];
        let mut cursor = BinaryCursor::new(&frame);

        assert_eq!(cursor.read_u8().unwrap(), 1);
        assert_eq!(cursor.position(), 1);
        assert_eq!(cursor.read_u16().unwrap(), 2);
        assert_eq!(cursor.read_u32().unwrap(), 3);
        assert_eq!(cursor.position(), frame.len());
        assert_eq!(cursor.remaining(), 0);
    }

    #[test]
    fn seeking_and_skipping_move_the_cursor() {
        let frame = [0u8; 16];
        let mut cursor = BinaryCursor::new(&frame);

        cursor.skip(4).unwrap();
        assert_eq!(cursor.position(), 4);
        cursor.seek(8).unwrap();
        assert_eq!(cursor.position(), 8);
        assert!(cursor.seek(frame.len() + 1).is_err());
    }

    #[test]
    fn reads_past_the_end_fail_without_advancing() {
        let frame = [1u8];
        let mut cursor = BinaryCursor::new(&frame);

        assert!(cursor.read_u16().is_err());
        assert_eq!(cursor.position(), 0);
    }

    #[test]
    fn reads_length_prefixed_strings() {
        let frame = b"\x03abc";
        let mut cursor = BinaryCursor::new(frame);

        assert_eq!(cursor.read_var_string().unwrap(), "abc");
        assert_eq!(cursor.remaining(), 0);
    }
}
