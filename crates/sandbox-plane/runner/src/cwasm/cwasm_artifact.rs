use platform_errors::{BuildError, BuildResult, StorageError};
use sha2::{Digest, Sha256};
use std::{
    num::NonZeroU64,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::io::AsyncWriteExt;
use tracing::info;

pub const CWASM_FILE_NAME: &str = "runner.cwasm";
pub const DEFAULT_CWASM_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(60);
pub const DEFAULT_MAX_CWASM_BYTES: NonZeroU64 =
    NonZeroU64::new(64 * 1024 * 1024).expect("default cwasm limit is non-zero");

/// The Wasmtime version embedded by the native runner.
///
/// Keep this in sync with the pinned `wasmtime` version in the workspace
/// `Cargo.toml`; the version check turns build-image/runner drift into a clear
/// diagnostic instead of a runtime deserialization failure.
pub const WASMTIME_VERSION: &str = "43.0.1";

/// Resource limits applied while downloading a compiled runner artifact.
///
/// Every limit is non-zero and therefore always enforced, so a cwasm artifact
/// cannot exhaust the runner's memory or ephemeral disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CwasmLimits {
    pub download_timeout: Duration,
    pub max_bytes: NonZeroU64,
}

impl CwasmLimits {
    pub const fn new(download_timeout: Duration, max_bytes: NonZeroU64) -> Self {
        Self {
            download_timeout,
            max_bytes,
        }
    }
}

impl Default for CwasmLimits {
    fn default() -> Self {
        Self::new(DEFAULT_CWASM_DOWNLOAD_TIMEOUT, DEFAULT_MAX_CWASM_BYTES)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CwasmIntegrity {
    pub sha256: String,
    pub size: u64,
    pub wasmtime_version: String,
}

impl CwasmIntegrity {
    pub fn new(sha256: impl Into<String>, size: u64, wasmtime_version: impl Into<String>) -> Self {
        Self {
            sha256: sha256.into(),
            size,
            wasmtime_version: wasmtime_version.into(),
        }
    }
}

/// A compiled runner artifact downloaded from a pre-signed URL.
///
/// The artifact is held in a private temporary directory for as long as this
/// value is alive, and is removed when it is dropped.
#[derive(Debug)]
pub struct Cwasm {
    _directory: tempfile::TempDir,
    path: PathBuf,
}

impl Cwasm {
    pub async fn download(
        url: &str,
        integrity: &CwasmIntegrity,
        limits: CwasmLimits,
    ) -> BuildResult<Self> {
        let expected_hash = Self::parse_hash(&integrity.sha256)?;
        Self::ensure_wasmtime_version(&integrity.wasmtime_version)?;
        Self::ensure_within_limit(integrity.size, limits.max_bytes)?;

        let client = reqwest::Client::builder()
            .timeout(limits.download_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|source| {
                BuildError::http_request("create cwasm download client", url, source)
            })?;
        let response =
            client.get(url).send().await.map_err(|source| {
                BuildError::http_request("download cwasm artifact", url, source)
            })?;
        let status = response.status();
        if !status.is_success() {
            let body = Self::read_body(response, url, limits.max_bytes)
                .await
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
                .unwrap_or_default();
            return Err(BuildError::http_rejected(
                "download cwasm artifact",
                url,
                status.as_u16(),
                body,
            ));
        }

        if let Some(length) = response.content_length() {
            Self::ensure_within_limit(length, limits.max_bytes)?;
            if length != integrity.size {
                return Err(Self::size_mismatch(integrity.size, length));
            }
        }

        let directory = tempfile::Builder::new()
            .prefix("platform-cwasm-")
            .tempdir()
            .map_err(|source| {
                BuildError::io("create cwasm directory", "temporary directory", source)
            })?;
        let path = directory.path().join(CWASM_FILE_NAME);
        let mut output = tokio::fs::File::create(&path).await.map_err(|source| {
            BuildError::io("create cwasm artifact", path.display().to_string(), source)
        })?;
        let (size, actual_hash) =
            Self::write_body(response, url, limits.max_bytes, &mut output).await?;
        output.flush().await.map_err(|source| {
            BuildError::io("write cwasm artifact", path.display().to_string(), source)
        })?;

        if size != integrity.size {
            return Err(Self::size_mismatch(integrity.size, size));
        }
        if actual_hash != expected_hash {
            return Err(BuildError::Storage(StorageError::CwasmHashMismatch {
                expected: hex::encode(expected_hash),
                actual: hex::encode(actual_hash),
            }));
        }

        let cwasm = Self {
            _directory: directory,
            path,
        };
        info!(
            bytes = size,
            path = %cwasm.path().display(),
            "downloaded cwasm artifact"
        );
        Ok(cwasm)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn parse_hash(value: &str) -> BuildResult<[u8; 32]> {
        let trimmed = value.trim();
        let encoded = trimmed
            .strip_prefix("sha256:")
            .or_else(|| trimmed.strip_prefix("sha256="))
            .unwrap_or(trimmed);
        let invalid = || {
            BuildError::Storage(StorageError::CwasmHashInvalid {
                value: value.to_string(),
            })
        };
        let decoded = hex::decode(encoded).map_err(|_| invalid())?;
        decoded.try_into().map_err(|_| invalid())
    }

    fn ensure_wasmtime_version(expected: &str) -> BuildResult<()> {
        let expected = expected.trim();
        let expected = expected
            .strip_prefix("wasmtime ")
            .or_else(|| expected.strip_prefix("wasmtime@"))
            .unwrap_or(expected);
        let expected = expected.strip_prefix('v').unwrap_or(expected);
        if expected == WASMTIME_VERSION {
            Ok(())
        } else {
            Err(BuildError::Storage(
                StorageError::CwasmWasmtimeVersionMismatch {
                    expected: expected.to_string(),
                    actual: WASMTIME_VERSION.to_string(),
                },
            ))
        }
    }

    fn size_mismatch(expected: u64, actual: u64) -> BuildError {
        BuildError::Storage(StorageError::CwasmSizeMismatch { expected, actual })
    }

    async fn read_body(
        mut response: reqwest::Response,
        url: &str,
        max_bytes: NonZeroU64,
    ) -> BuildResult<Vec<u8>> {
        if let Some(length) = response.content_length() {
            Self::ensure_within_limit(length, max_bytes)?;
        }

        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|source| BuildError::http_request("read cwasm response", url, source))?
        {
            let size = Self::size_after(bytes.len(), chunk.len());
            Self::ensure_within_limit(size, max_bytes)?;
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }

    async fn write_body(
        mut response: reqwest::Response,
        url: &str,
        max_bytes: NonZeroU64,
        output: &mut tokio::fs::File,
    ) -> BuildResult<(u64, [u8; 32])> {
        if let Some(length) = response.content_length() {
            Self::ensure_within_limit(length, max_bytes)?;
        }

        let mut hasher = Sha256::new();
        let mut size = 0;
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|source| BuildError::http_request("read cwasm response", url, source))?
        {
            let next_size =
                Self::size_after_u64(size, u64::try_from(chunk.len()).unwrap_or(u64::MAX));
            Self::ensure_within_limit(next_size, max_bytes)?;
            output.write_all(&chunk).await.map_err(|source| {
                BuildError::io("write cwasm artifact", url.to_string(), source)
            })?;
            hasher.update(&chunk);
            size = next_size;
        }
        Ok((size, hasher.finalize().into()))
    }

    fn ensure_within_limit(size: u64, max_bytes: NonZeroU64) -> BuildResult<()> {
        if size > max_bytes.get() {
            return Err(BuildError::Storage(StorageError::CwasmTooLarge {
                size,
                max_bytes: max_bytes.get(),
            }));
        }
        Ok(())
    }

    fn size_after(current: usize, additional: usize) -> u64 {
        Self::size_after_u64(
            u64::try_from(current).unwrap_or(u64::MAX),
            u64::try_from(additional).unwrap_or(u64::MAX),
        )
    }

    fn size_after_u64(current: u64, additional: u64) -> u64 {
        current.saturating_add(additional)
    }
}
