use platform_errors::{BpError, BpResult};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

pub struct Runner {
    bin: PathBuf,
}

impl Runner {
    pub fn new(bin: PathBuf) -> Self {
        Self { bin }
    }

    pub async fn invoke(&self, request: &str) -> BpResult<()> {
        let mut child = Command::new(&self.bin)
            .stdin(std::process::Stdio::piped())
            .spawn()
            .map_err(|source| {
                BpError::io("start runner", self.bin.display().to_string(), source)
            })?;
        let mut stdin = child.stdin.take().ok_or_else(|| {
            BpError::io(
                "start runner",
                self.bin.display().to_string(),
                "missing stdin pipe",
            )
        })?;
        stdin
            .write_all(request.as_bytes())
            .await
            .map_err(|source| {
                BpError::io("write run request", self.bin.display().to_string(), source)
            })?;
        drop(stdin);
        drop(child);
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests;
