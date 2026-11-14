use platform_errors::{PlatformError, PlatformResult};
use platform_git::GitFile;
use std::io::{Cursor, Write};
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

const USER_GENERATED_PREFIX: &str = "crates/sandbox-plane/runner/src/user_generated/";

pub struct SourceArchive;

impl SourceArchive {
    pub fn create(files: &[GitFile]) -> PlatformResult<Vec<u8>> {
        let mut archive = ZipWriter::new(Cursor::new(Vec::new()));
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
        for file in files {
            archive
                .start_file(format!("{USER_GENERATED_PREFIX}{}", file.path), options)
                .map_err(PlatformError::from)?;
            archive
                .write_all(&file.contents)
                .map_err(PlatformError::from)?;
        }
        Ok(archive.finish().map_err(PlatformError::from)?.into_inner())
    }
}
