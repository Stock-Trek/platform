use strum::Display;

#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ByteEncoder {
    Base16,
    Base32,
    Base58,
    Base64,
    HexLower,
    HexUpper,
    Percent,
}

impl ByteEncoder {
    const HEX_DIGITS: [u8; 16] = *b"0123456789ABCDEF";

    pub fn encode(&self, bytes: &[u8]) -> String {
        match self {
            ByteEncoder::Base16 => data_encoding::HEXUPPER.encode(bytes),
            ByteEncoder::Base32 => data_encoding::BASE32.encode(bytes),
            ByteEncoder::Base58 => bs58::encode(bytes).into_string(),
            ByteEncoder::Base64 => data_encoding::BASE64.encode(bytes),
            ByteEncoder::HexLower => data_encoding::HEXLOWER.encode(bytes),
            ByteEncoder::HexUpper => data_encoding::HEXUPPER.encode(bytes),
            ByteEncoder::Percent => {
                // Compute capacity
                // Every unreserved byte contributes 1 byte; every other contributes 3.
                let mut cap = bytes.len();
                for &b in bytes {
                    if !Self::is_unreserved(b) {
                        cap += 2;
                    }
                }
                let mut out = String::with_capacity(cap);
                // Second pass: encode.
                for &b in bytes {
                    if Self::is_unreserved(b) {
                        // Unreserved bytes are ASCII, so this conversion is lossless.
                        out.push(b as char);
                    } else {
                        out.push('%');
                        out.push(Self::HEX_DIGITS[(b >> 4) as usize] as char);
                        out.push(Self::HEX_DIGITS[(b & 0x0f) as usize] as char);
                    }
                }
                out
            }
        }
    }
    #[inline(always)]
    fn is_unreserved(b: u8) -> bool {
        matches!(b, b'0'..=b'9' | b'A'..=b'Z' | b'a'..=b'z' | b'-' | b'.' | b'_' | b'~')
    }
}
