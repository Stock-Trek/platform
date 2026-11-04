use super::*;
use platform_errors::{BpError, ErrorType, StorageError};
use sha2::{Digest, Sha256};
use std::num::NonZeroU64;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn server(response: &'static [u8]) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0; 1024];
        let _ = stream.read(&mut request).await;
        let _ = stream.write_all(response).await;
    });
    (format!("http://{address}/runner.cwasm"), handle)
}

fn limits(max_bytes: u64) -> CwasmLimits {
    CwasmLimits::new(Duration::from_secs(5), NonZeroU64::new(max_bytes).unwrap())
}

fn integrity(content: &[u8]) -> CwasmIntegrity {
    CwasmIntegrity::new(
        format!("sha256:{}", hex::encode(Sha256::digest(content))),
        content.len() as u64,
        WASMTIME_VERSION,
    )
}

#[test]
fn exposes_the_lockfile_wasmtime_version() {
    assert!(!WASMTIME_VERSION.is_empty());
    assert!(WASMTIME_VERSION.starts_with(char::is_numeric));
}

#[tokio::test]
async fn downloads_the_artifact_to_a_temporary_file() {
    let (url, server) =
        server(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\ncompiled").await;

    let cwasm = Cwasm::download(&url, limits(64)).await.unwrap();

    server.await.unwrap();
    assert_eq!(cwasm.path().file_name().unwrap(), CWASM_FILE_NAME);
    assert_eq!(std::fs::read(cwasm.path()).unwrap(), b"compiled");
}

#[tokio::test]
async fn removes_the_temporary_file_when_dropped() {
    let (url, server) =
        server(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\ncompiled").await;

    let cwasm = Cwasm::download(&url, limits(64)).await.unwrap();
    let path = cwasm.path().to_path_buf();
    drop(cwasm);

    server.await.unwrap();
    assert!(!path.exists());
}

#[tokio::test]
async fn downloads_an_artifact_with_matching_integrity_metadata() {
    let (url, server) =
        server(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\ncompiled").await;

    let cwasm = Cwasm::download_verified(&url, &integrity(b"compiled"), limits(64))
        .await
        .unwrap();

    server.await.unwrap();
    assert_eq!(std::fs::read(cwasm.path()).unwrap(), b"compiled");
}

#[tokio::test]
async fn rejects_an_artifact_with_the_wrong_hash() {
    let (url, server) =
        server(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\ncompiled").await;
    let expected = integrity(b"wrong!!!");

    let error = match Cwasm::download_verified(&url, &expected, limits(64)).await {
        Ok(_) => panic!("expected mismatched artifact to be rejected"),
        Err(error) => error,
    };

    server.await.unwrap();
    assert!(matches!(
        error,
        BpError::Storage(StorageError::CwasmHashMismatch { .. })
    ));
    assert_eq!(error.error_type(), "cwasm_hash_mismatch");
}

#[tokio::test]
async fn rejects_an_artifact_with_the_wrong_size() {
    let (url, server) =
        server(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\ncompiled").await;
    let mut expected = integrity(b"compiled");
    expected.size = 7;

    let error = match Cwasm::download_verified(&url, &expected, limits(64)).await {
        Ok(_) => panic!("expected mismatched artifact to be rejected"),
        Err(error) => error,
    };

    server.await.unwrap();
    assert!(matches!(
        error,
        BpError::Storage(StorageError::CwasmSizeMismatch { .. })
    ));
    assert_eq!(error.error_type(), "cwasm_size_mismatch");
}

#[tokio::test]
async fn rejects_an_artifact_from_a_different_wasmtime_version() {
    let mut expected = integrity(b"compiled");
    expected.wasmtime_version = "0.0.0".to_string();

    let error = Cwasm::download_verified("http://127.0.0.1:1/runner.cwasm", &expected, limits(64))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BpError::Storage(StorageError::CwasmWasmtimeVersionMismatch { .. })
    ));
    assert_eq!(error.error_type(), "cwasm_wasmtime_version_mismatch");
}

#[tokio::test]
async fn rejects_an_invalid_artifact_hash_before_downloading() {
    let expected = CwasmIntegrity::new("not-a-hash", 8, WASMTIME_VERSION);

    let error = Cwasm::download_verified("http://127.0.0.1:1/runner.cwasm", &expected, limits(64))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BpError::Storage(StorageError::CwasmHashInvalid { .. })
    ));
    assert_eq!(error.error_type(), "cwasm_hash_invalid");
}

#[tokio::test]
async fn does_not_follow_download_redirects() {
    let (url, server) = server(
        b"HTTP/1.1 302 Found\r\nLocation: https://example.com/runner.cwasm\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    )
    .await;

    let error = Cwasm::download_verified(&url, &integrity(b"compiled"), limits(64))
        .await
        .unwrap_err();

    server.await.unwrap();
    assert!(matches!(error, BpError::Http(_)));
}

#[tokio::test]
async fn rejects_an_expected_size_above_the_limit_before_downloading() {
    let mut expected = integrity(b"compiled");
    expected.size = 1024;

    let error = Cwasm::download_verified("http://127.0.0.1:1/runner.cwasm", &expected, limits(64))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BpError::Storage(StorageError::CwasmTooLarge { .. })
    ));
}

#[tokio::test]
async fn rejects_artifacts_larger_than_the_content_length_limit() {
    let (url, server) =
        server(b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\nhello world")
            .await;

    let error = match Cwasm::download(&url, limits(4)).await {
        Ok(_) => panic!("expected oversized artifact to be rejected"),
        Err(error) => error,
    };

    server.await.unwrap();
    assert!(matches!(
        error,
        BpError::Storage(StorageError::CwasmTooLarge { .. })
    ));
    assert_eq!(error.error_type(), "cwasm_too_large");
    assert!(!error.retryability().is_retryable());
}

#[tokio::test]
async fn rejects_streamed_artifacts_that_exceed_the_limit() {
    let (url, server) = server(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n",
    )
    .await;

    let error = match Cwasm::download(&url, limits(4)).await {
        Ok(_) => panic!("expected oversized artifact to be rejected"),
        Err(error) => error,
    };

    server.await.unwrap();
    assert!(matches!(
        error,
        BpError::Storage(StorageError::CwasmTooLarge { .. })
    ));
}

#[tokio::test]
async fn rejects_a_failed_download_response() {
    let (url, server) =
        server(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 6\r\nConnection: close\r\n\r\ndenied")
            .await;

    let error = match Cwasm::download(&url, limits(64)).await {
        Ok(_) => panic!("expected rejected download to fail"),
        Err(error) => error,
    };

    server.await.unwrap();
    assert!(matches!(error, BpError::Http(_)));
    assert_eq!(error.error_type(), "rejected");
}
