//! Download and unpack the user's algorithm sources.
//!
//! The orchestrator uploads a zip archive of the repository's
//! `src/algorithms` directory and passes the pre-signed download URL in the
//! build request as `signed_get_url`. Archive paths are rooted at
//! `crates/compute/runner/src/user_generated/` so extracting the archive over the
//! working directory replaces the checked in algorithm stub with the user's
//! source.

use platform_errors::{BpError, BpResult, IoError, StorageError};
use std::{
    io::{Cursor, Read, Write},
    num::NonZeroU64,
    path::{Component, Path, PathBuf},
    time::Duration,
};
use tracing::info;
use zip::ZipArchive;

const USER_GENERATED_DIR: &str = "crates/compute/runner/src/user_generated";
const USER_GENERATED_PREFIX: &str = "crates/compute/runner/src/user_generated/";

pub const DEFAULT_SOURCE_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(60);
pub const DEFAULT_MAX_SOURCE_ARCHIVE_BYTES: NonZeroU64 =
    NonZeroU64::new(16 * 1024 * 1024).expect("default source archive limit is non-zero");
pub const DEFAULT_MAX_EXTRACTED_SOURCE_BYTES: NonZeroU64 =
    NonZeroU64::new(64 * 1024 * 1024).expect("default extracted source limit is non-zero");

/// Resource limits applied while downloading and extracting user sources.
///
/// Every limit is non-zero and therefore always enforced, so an archive cannot
/// exhaust the agent's memory or ephemeral disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceLimits {
    pub download_timeout: Duration,
    pub max_archive_bytes: NonZeroU64,
    pub max_extracted_bytes: NonZeroU64,
}

impl SourceLimits {
    pub const fn new(
        download_timeout: Duration,
        max_archive_bytes: NonZeroU64,
        max_extracted_bytes: NonZeroU64,
    ) -> Self {
        Self {
            download_timeout,
            max_archive_bytes,
            max_extracted_bytes,
        }
    }
}

impl Default for SourceLimits {
    fn default() -> Self {
        Self::new(
            DEFAULT_SOURCE_DOWNLOAD_TIMEOUT,
            DEFAULT_MAX_SOURCE_ARCHIVE_BYTES,
            DEFAULT_MAX_EXTRACTED_SOURCE_BYTES,
        )
    }
}

pub struct Source {
    bytes: Vec<u8>,
}

impl Source {
    pub async fn download(url: &str, limits: SourceLimits) -> BpResult<Self> {
        let client = reqwest::Client::builder()
            .timeout(limits.download_timeout)
            .build()
            .map_err(|source| {
                BpError::http_request("create user source download client", url, source)
            })?;
        let response =
            client.get(url).send().await.map_err(|source| {
                BpError::http_request("download user source archive", url, source)
            })?;
        let status = response.status();
        if !status.is_success() {
            let body = Self::read_body(response, url, limits.max_archive_bytes)
                .await
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
                .unwrap_or_default();
            return Err(BpError::http_rejected(
                "download user source archive",
                url,
                status.as_u16(),
                body,
            ));
        }
        let bytes = Self::read_body(response, url, limits.max_archive_bytes).await?;
        info!(bytes = bytes.len(), "downloaded user source archive");
        Ok(Self { bytes })
    }

    async fn read_body(
        mut response: reqwest::Response,
        url: &str,
        max_bytes: NonZeroU64,
    ) -> BpResult<Vec<u8>> {
        if let Some(length) = response.content_length() {
            Self::ensure_archive_within_limit(length, max_bytes)?;
        }

        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|source| {
            BpError::http_request("read user source archive response", url, source)
        })? {
            let size = Self::size_after(bytes.len(), chunk.len());
            Self::ensure_archive_within_limit(size, max_bytes)?;
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }

    fn ensure_archive_within_limit(size: u64, max_bytes: NonZeroU64) -> BpResult<()> {
        if size > max_bytes.get() {
            return Err(BpError::Storage(StorageError::SourceArchiveTooLarge {
                size,
                max_bytes: max_bytes.get(),
            }));
        }
        Ok(())
    }

    fn size_after(current: usize, additional: usize) -> u64 {
        u64::try_from(current)
            .unwrap_or(u64::MAX)
            .saturating_add(u64::try_from(additional).unwrap_or(u64::MAX))
    }

    pub fn extract(
        &self,
        destination: &Path,
        max_extracted_bytes: NonZeroU64,
    ) -> BpResult<Vec<PathBuf>> {
        let mut archive = ZipArchive::new(Cursor::new(&self.bytes))
            .map_err(|error| BpError::Io(IoError::Zip(error)))?;

        // Validate every path and the declared uncompressed size before writing
        // anything, so a malformed archive cannot leave a partially extracted
        // source tree behind.
        let entry_count = archive.len();
        let mut entries = Vec::with_capacity(entry_count);
        let mut declared_bytes = 0;
        for index in 0..entry_count {
            let entry = archive
                .by_index(index)
                .map_err(|error| BpError::Io(IoError::Zip(error)))?;
            let name = entry
                .name()
                .map_err(|error| BpError::Io(IoError::Zip(error)))?;
            entries.push(Self::entry_path(&name)?);
            if !entry.is_dir() {
                declared_bytes =
                    Self::add_extracted_bytes(declared_bytes, entry.size(), max_extracted_bytes)?;
            }
        }

        let mut files = Vec::new();
        let mut extracted_bytes = 0;
        for (index, relative) in entries.into_iter().enumerate() {
            let mut entry = archive
                .by_index(index)
                .map_err(|error| BpError::Io(IoError::Zip(error)))?;
            let path = destination.join(relative);
            if entry.is_dir() {
                std::fs::create_dir_all(&path).map_err(|source| {
                    BpError::io(
                        "create source directory",
                        path.display().to_string(),
                        source,
                    )
                })?;
                continue;
            }
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|source| {
                    BpError::io(
                        "create source directory",
                        parent.display().to_string(),
                        source,
                    )
                })?;
            }
            let mut output = std::fs::File::create(&path).map_err(|source| {
                BpError::io("write source file", path.display().to_string(), source)
            })?;
            let allowance = max_extracted_bytes.get().saturating_sub(extracted_bytes);
            let mut limited = (&mut entry).take(allowance.saturating_add(1));
            let written = std::io::copy(&mut limited, &mut output).map_err(|source| {
                BpError::io("write source file", path.display().to_string(), source)
            })?;
            output.flush().map_err(|source| {
                BpError::io("write source file", path.display().to_string(), source)
            })?;
            if written > allowance {
                drop(output);
                let _ = std::fs::remove_file(&path);
                return Err(BpError::Storage(StorageError::ExtractedSourceTooLarge {
                    size: extracted_bytes.saturating_add(written),
                    max_bytes: max_extracted_bytes.get(),
                }));
            }
            extracted_bytes = extracted_bytes.saturating_add(written);
            files.push(path);
        }

        info!(
            destination = %destination.display(),
            entries = entry_count,
            bytes = extracted_bytes,
            "unpacked user source archive"
        );
        Ok(files)
    }

    fn add_extracted_bytes(current: u64, additional: u64, max_bytes: NonZeroU64) -> BpResult<u64> {
        let size = current.saturating_add(additional);
        if size > max_bytes.get() {
            return Err(BpError::Storage(StorageError::ExtractedSourceTooLarge {
                size,
                max_bytes: max_bytes.get(),
            }));
        }
        Ok(size)
    }

    fn entry_path(name: &str) -> BpResult<PathBuf> {
        if name.contains('\\') {
            return Err(BpError::invalid_path(name));
        }
        let path = Path::new(name);
        if path.as_os_str().is_empty()
            || path
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(BpError::invalid_path(name));
        }
        if path != Path::new(USER_GENERATED_DIR) && !path.starts_with(USER_GENERATED_PREFIX) {
            return Err(BpError::invalid_path(name));
        }
        Ok(path.to_path_buf())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use platform_errors::ErrorType;
    use std::io::Cursor;
    use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

    fn archive(files: &[(&str, &[u8])]) -> Vec<u8> {
        let mut archive = ZipWriter::new(Cursor::new(Vec::new()));
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
        for (name, contents) in files {
            archive.start_file(*name, options).unwrap();
            archive.write_all(contents).unwrap();
        }
        archive.finish().unwrap().into_inner()
    }

    #[test]
    fn extracts_sources_within_the_limit() {
        let directory = tempfile::tempdir().unwrap();
        let source = Source {
            bytes: archive(&[(
                "crates/compute/runner/src/user_generated/algorithm.rs",
                b"pub struct Example;",
            )]),
        };

        let files = source
            .extract(directory.path(), NonZeroU64::new(1024).unwrap())
            .unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(std::fs::read(&files[0]).unwrap(), b"pub struct Example;");
    }

    #[test]
    fn rejects_archives_whose_declared_extracted_size_exceeds_the_limit() {
        let directory = tempfile::tempdir().unwrap();
        let source = Source {
            bytes: archive(&[(
                "crates/compute/runner/src/user_generated/algorithm.rs",
                &[0u8; 256],
            )]),
        };

        let error = source
            .extract(directory.path(), NonZeroU64::new(128).unwrap())
            .unwrap_err();

        assert!(matches!(
            error,
            BpError::Storage(StorageError::ExtractedSourceTooLarge { .. })
        ));
        assert_eq!(error.error_type(), "extracted_source_too_large");
        assert!(!error.retryability().is_retryable());
        assert!(
            !directory
                .path()
                .join("crates/compute/runner/src/user_generated/algorithm.rs")
                .exists()
        );
    }

    #[tokio::test]
    async fn rejects_downloads_larger_than_the_limit() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            let _ = stream.read(&mut request).await;
            let _ = stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\nhello world",
                )
                .await;
        });
        let limits = SourceLimits::new(
            Duration::from_secs(5),
            NonZeroU64::new(4).unwrap(),
            NonZeroU64::new(1024).unwrap(),
        );

        let error = match Source::download(&format!("http://{address}/sources.zip"), limits).await {
            Ok(_) => panic!("expected oversized download to be rejected"),
            Err(error) => error,
        };

        server.await.unwrap();
        assert!(matches!(
            error,
            BpError::Storage(StorageError::SourceArchiveTooLarge { .. })
        ));
        assert_eq!(error.error_type(), "source_archive_too_large");
        assert!(!error.retryability().is_retryable());
    }
}
