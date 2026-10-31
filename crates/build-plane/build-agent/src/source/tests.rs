use super::*;
use platform_errors::ErrorType;
use platform_errors::{BpError, StorageError};
use std::io::{Cursor, Write};
use std::num::NonZeroU64;
use std::time::Duration;
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
            "crates/sandbox-plane/runner/src/user_generated/algorithm.rs",
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
            "crates/sandbox-plane/runner/src/user_generated/algorithm.rs",
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
            .join("crates/sandbox-plane/runner/src/user_generated/algorithm.rs")
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
