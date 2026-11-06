use platform_errors::{BpError, BpResult};
use std::path::Path;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

pub async fn invoke_runner(runner_bin: &Path, request: &str) -> BpResult<()> {
    let mut child = Command::new(runner_bin)
        .stdin(std::process::Stdio::piped())
        .spawn()
        .map_err(|source| BpError::io("start runner", runner_bin.display().to_string(), source))?;
    let mut stdin = child.stdin.take().ok_or_else(|| {
        BpError::io(
            "start runner",
            runner_bin.display().to_string(),
            "missing stdin pipe",
        )
    })?;
    stdin
        .write_all(request.as_bytes())
        .await
        .map_err(|source| {
            BpError::io(
                "write run request",
                runner_bin.display().to_string(),
                source,
            )
        })?;
    drop(stdin);
    drop(child);
    Ok(())
}

#[cfg(all(test, unix))]
mod tests;
