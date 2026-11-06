use super::Runner;
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::{NamedTempFile, tempdir};

fn runner_script(lines: &[String]) -> PathBuf {
    let script = NamedTempFile::new().unwrap();
    let mut file = fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(script.path())
        .unwrap();
    writeln!(file, "#!/bin/sh").unwrap();
    for line in lines {
        writeln!(file, "{line}").unwrap();
    }
    file.sync_all().unwrap();
    drop(file);
    let path = script.into_temp_path().keep().unwrap();
    let mut permissions = fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).unwrap();
    path
}

#[tokio::test]
async fn writes_the_request_and_returns_without_waiting() {
    let directory = tempdir().unwrap();
    let output = directory.path().join("request");
    let script = runner_script(&[format!("cat > '{}'", output.display())]);

    Runner::new(script).invoke("request").await.unwrap();

    let mut delivered = false;
    for _ in 0..100 {
        if fs::read(&output).is_ok_and(|body| body == b"request") {
            delivered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(delivered);
}

#[tokio::test]
async fn fails_when_the_runner_binary_is_missing() {
    let result = Runner::new(PathBuf::from("/nonexistent/runner"))
        .invoke("{}")
        .await;
    assert!(result.is_err());
}
