use super::*;
use platform_errors::{BpError, ErrorType, StorageError};
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
