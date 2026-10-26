/// The default maximum depth of a collected directory tree.
pub const DEFAULT_MAX_DIRECTORY_DEPTH: usize = 8;
/// The default maximum number of files collected from a directory.
pub const DEFAULT_MAX_DIRECTORY_FILES: usize = 100;
/// The default maximum number of bytes collected from a directory.
pub const DEFAULT_MAX_DIRECTORY_BYTES: u64 = 1024 * 1024;

/// Limits applied while collecting a directory tree.
///
/// Directory entries are loaded into memory before being written to the source
/// archive, so these limits keep a pathological repository from exhausting the
/// Lambda's memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectoryLimits {
    pub max_depth: usize,
    pub max_files: usize,
    pub max_total_bytes: u64,
}

impl Default for DirectoryLimits {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_DIRECTORY_DEPTH,
            max_files: DEFAULT_MAX_DIRECTORY_FILES,
            max_total_bytes: DEFAULT_MAX_DIRECTORY_BYTES,
        }
    }
}
