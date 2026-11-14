use sha2::{Digest, Sha256};
use std::{
    fmt,
    fs::File,
    io::{self, Read},
    path::Path,
};

const CONTENT_HASH_BYTES: usize = 16;
const FILE_READ_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentHash([u8; CONTENT_HASH_BYTES]);

impl ContentHash {
    pub fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let mut hasher = ContentHasher::new();
        hasher.update(bytes);
        hasher.finish()
    }

    pub fn from_file(path: &Path) -> io::Result<(Self, u64)> {
        let mut file = File::open(path)?;
        let mut hasher = ContentHasher::new();
        let mut length = 0_u64;
        let mut buffer = [0_u8; FILE_READ_BUFFER_BYTES];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            length += u64::try_from(read).unwrap_or(0);
        }
        Ok((hasher.finish(), length))
    }
}

impl fmt::Display for ContentHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct ContentHasher {
    hasher: Sha256,
}

impl ContentHasher {
    pub fn new() -> Self {
        Self {
            hasher: Sha256::new(),
        }
    }

    pub fn update(&mut self, bytes: impl AsRef<[u8]>) {
        self.hasher.update(bytes.as_ref());
    }

    pub fn finish(self) -> ContentHash {
        let digest = self.hasher.finalize();
        let mut hash = [0_u8; CONTENT_HASH_BYTES];
        hash.copy_from_slice(&digest[..CONTENT_HASH_BYTES]);
        ContentHash(hash)
    }
}
