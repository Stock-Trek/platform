use platform_errors::{BpError, BpResult, StorageError};
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

/// A compiled runner artifact downloaded from a pre-signed URL.
///
/// The artifact is held in a private temporary directory for as long as this
/// value is alive, and is removed when it is dropped.
pub struct Cwasm {
    _directory: tempfile::TempDir,
    path: PathBuf,
}

impl Cwasm {
    pub async fn download(url: &str, limits: CwasmLimits) -> BpResult<Self> {
        let client = reqwest::Client::builder()
            .timeout(limits.download_timeout)
            .build()
            .map_err(|source| BpError::http_request("create cwasm download client", url, source))?;
        let response = client
            .get(url)
            .send()
            .await
            .map_err(|source| BpError::http_request("download cwasm artifact", url, source))?;
        let status = response.status();
        if !status.is_success() {
            let body = Self::read_body(response, url, limits.max_bytes)
                .await
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
                .unwrap_or_default();
            return Err(BpError::http_rejected(
                "download cwasm artifact",
                url,
                status.as_u16(),
                body,
            ));
        }

        let directory = tempfile::Builder::new()
            .prefix("platform-cwasm-")
            .tempdir()
            .map_err(|source| {
                BpError::io("create cwasm directory", "temporary directory", source)
            })?;
        let path = directory.path().join(CWASM_FILE_NAME);
        let mut output = tokio::fs::File::create(&path).await.map_err(|source| {
            BpError::io("create cwasm artifact", path.display().to_string(), source)
        })?;
        let size = Self::write_body(response, url, limits.max_bytes, &mut output).await?;
        output.flush().await.map_err(|source| {
            BpError::io("write cwasm artifact", path.display().to_string(), source)
        })?;

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

    async fn read_body(
        mut response: reqwest::Response,
        url: &str,
        max_bytes: NonZeroU64,
    ) -> BpResult<Vec<u8>> {
        if let Some(length) = response.content_length() {
            Self::ensure_within_limit(length, max_bytes)?;
        }

        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|source| BpError::http_request("read cwasm response", url, source))?
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
    ) -> BpResult<u64> {
        if let Some(length) = response.content_length() {
            Self::ensure_within_limit(length, max_bytes)?;
        }

        let mut size = 0;
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|source| BpError::http_request("read cwasm response", url, source))?
        {
            let next_size =
                Self::size_after_u64(size, u64::try_from(chunk.len()).unwrap_or(u64::MAX));
            Self::ensure_within_limit(next_size, max_bytes)?;
            output
                .write_all(&chunk)
                .await
                .map_err(|source| BpError::io("write cwasm artifact", url.to_string(), source))?;
            size = next_size;
        }
        Ok(size)
    }

    fn ensure_within_limit(size: u64, max_bytes: NonZeroU64) -> BpResult<()> {
        if size > max_bytes.get() {
            return Err(BpError::Storage(StorageError::CwasmTooLarge {
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
