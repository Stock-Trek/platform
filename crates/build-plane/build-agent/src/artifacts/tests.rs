use super::artifact::*;
use crate::s3::SignedPost;
use platform_errors::{BpError, ConfigError};
use std::{collections::BTreeMap, time::Duration};
use std::{
    fs as std_fs,
    io::Cursor,
    path::{Path, PathBuf},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

impl Artifacts {
    fn new(
        signed_post: SignedPost,
        source_get_url: String,
        source_dir: PathBuf,
        build_cache_dir: PathBuf,
    ) -> platform_errors::BpResult<Self> {
        Self::with_source_limits(
            signed_post,
            source_get_url,
            source_dir,
            build_cache_dir,
            crate::source::SourceLimits::default(),
        )
    }
}

async fn upload_server(expected_uploads: usize) -> (String, tokio::task::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/upload", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        let mut bodies = Vec::new();
        for _ in 0..expected_uploads {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut chunk = [0_u8; 4096];
                let read = socket.read(&mut chunk).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                if let Some(header_end) = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|position| position + 4)
                {
                    let headers = String::from_utf8_lossy(&request[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap();
                    if request.len() >= header_end + content_length {
                        break;
                    }
                }
            }
            bodies.push(String::from_utf8_lossy(&request).into_owned());
            socket
                .write_all(
                    b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        }
        bodies
    });
    (url, handle)
}

fn upload_artifacts(url: String) -> Artifacts {
    let fields = BTreeMap::from([("key".to_string(), "artifacts/${filename}".to_string())]);
    let signed_post = SignedPost::new(url, fields, Duration::from_secs(5)).unwrap();
    Artifacts::new(
        signed_post,
        "https://example.com/source.zip".to_string(),
        PathBuf::from("/unused-source"),
        PathBuf::from("/unused-cache"),
    )
    .unwrap()
}

fn create_user_generated_source(artifacts: &Artifacts) {
    let root = artifacts.build_dir.path().join(USER_GENERATED_DIR);
    std_fs::create_dir_all(&root).unwrap();
    std_fs::write(root.join("algorithm.rs"), b"pub struct Example;\n").unwrap();
}

fn artifacts(source_dir: &Path, build_cache_dir: &Path) -> Artifacts {
    let fields = BTreeMap::from([("key".to_string(), "artifacts/${filename}".to_string())]);
    let signed_post = SignedPost::new(
        "https://example.com/upload".to_string(),
        fields,
        Duration::from_secs(1),
    )
    .unwrap();

    Artifacts::new(
        signed_post,
        "https://example.com/source.zip".to_string(),
        source_dir.to_path_buf(),
        build_cache_dir.to_path_buf(),
    )
    .unwrap()
}

#[tokio::test]
async fn publishes_source_before_compile_output_on_failure() {
    let (url, server) = upload_server(2).await;
    let artifacts = upload_artifacts(url);
    create_user_generated_source(&artifacts);
    artifacts.set_compile_output(b"error[E0425]: cannot find value `foo`\n".to_vec());
    let error = BpError::Config(ConfigError::Build("compile failed".to_string()));

    artifacts.publish_failure(&error).await.unwrap();

    let bodies = server.await.unwrap();
    assert_eq!(bodies.len(), 2);
    assert!(bodies[0].contains(USER_GENERATED_ARCHIVE));
    assert!(bodies[1].contains(COMPILE_OUTPUT_FILE));
    assert!(bodies[1].contains("cannot find value `foo`"));
    assert!(bodies[1].contains("compile failed"));
}

#[tokio::test]
async fn publishes_source_before_cwasm_on_success() {
    let (url, server) = upload_server(2).await;
    let artifacts = upload_artifacts(url);
    create_user_generated_source(&artifacts);
    let cwasm = artifacts.build_dir.path().join(CWASM_ARTIFACT_FILE);
    std_fs::write(&cwasm, b"compiled").unwrap();

    artifacts.publish(&cwasm).await.unwrap();

    let bodies = server.await.unwrap();
    assert_eq!(bodies.len(), 2);
    assert!(bodies[0].contains(USER_GENERATED_ARCHIVE));
    assert!(bodies[1].contains(CWASM_ARTIFACT_FILE));
    assert!(!bodies.iter().any(|body| body.contains(COMPILE_OUTPUT_FILE)));
}

#[tokio::test]
async fn target_dir_starts_from_the_prebuilt_cache_for_each_invocation() {
    let source = tempfile::tempdir().unwrap();
    let build_cache = tempfile::tempdir().unwrap();
    let prebuilt = source
        .path()
        .join(SOURCE_TARGET_DIR)
        .join(WASM_TARGET)
        .join("release");
    std_fs::create_dir_all(&prebuilt).unwrap();
    std_fs::write(prebuilt.join(WASM_ARTIFACT_FILE), b"prebuilt").unwrap();

    let artifacts = artifacts(source.path(), build_cache.path());
    let first = artifacts.prepare_target_dir().await.unwrap();
    let artifact = first
        .path()
        .join(WASM_TARGET)
        .join("release")
        .join(WASM_ARTIFACT_FILE);
    assert_eq!(std_fs::read(&artifact).unwrap(), b"prebuilt");

    // Simulate cargo writing output derived from a user's sources.
    std_fs::write(first.path().join("user-output"), b"previous run").unwrap();

    let second = artifacts.prepare_target_dir().await.unwrap();
    assert_ne!(first.path(), second.path());
    let artifact = second
        .path()
        .join(WASM_TARGET)
        .join("release")
        .join(WASM_ARTIFACT_FILE);
    assert_eq!(std_fs::read(artifact).unwrap(), b"prebuilt");
    assert!(!second.path().join("user-output").exists());
}

#[test]
fn source_archive_contains_user_generated_files() {
    let root = tempfile::tempdir().unwrap();
    let algorithm = root.path().join("algorithms/algorithm.rs");
    std_fs::create_dir_all(algorithm.parent().unwrap()).unwrap();
    std_fs::write(&algorithm, b"algorithm").unwrap();
    let manifest = root.path().join("mod.rs");
    std_fs::write(&manifest, b"modules").unwrap();

    let bytes =
        Artifacts::source_archive(root.path(), &[algorithm.clone(), manifest.clone()]).unwrap();
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes)).unwrap();

    assert_eq!(archive.len(), 2);
    assert_eq!(
        archive.by_index(0).unwrap().name().unwrap(),
        "algorithms/algorithm.rs"
    );
    assert_eq!(archive.by_index(1).unwrap().name().unwrap(), "mod.rs");
}

#[test]
fn validates_user_generated_algorithm_sources() {
    let root = tempfile::tempdir().unwrap();
    let algorithm = root.path().join("algorithm.rs");
    std_fs::write(
        &algorithm,
        b"use stock_trek::prelude::Algorithm;\n\npub struct Example;\n",
    )
    .unwrap();
    let modules = root.path().join("mod.rs");
    std_fs::write(&modules, b"// module declarations\n").unwrap();

    Artifacts::validate_user_generated(&[modules, algorithm]).unwrap();
}

#[test]
fn checks_mod_rs_alongside_algorithm_sources() {
    let root = tempfile::tempdir().unwrap();
    let algorithm = root.path().join("algorithm.rs");
    std_fs::write(
        &algorithm,
        b"use stock_trek::prelude::Algorithm;\n\npub struct Example;\n",
    )
    .unwrap();
    let modules = root.path().join("mod.rs");
    std_fs::write(&modules, b"pub mod algorithm;\n").unwrap();

    let error = Artifacts::validate_user_generated(&[algorithm, modules]).unwrap_err();

    assert!(matches!(error, BpError::Validation(_)));
}

#[test]
fn checks_sources_without_rust_extension() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("algorithm.txt");
    std_fs::write(&source, b"use std::fs;\n\npub struct Example;\n").unwrap();

    let error = Artifacts::validate_user_generated(&[source]).unwrap_err();

    assert!(matches!(error, BpError::Validation(_)));
}

#[test]
fn rejects_user_generated_sources_with_blocked_syntax() {
    let root = tempfile::tempdir().unwrap();
    let algorithm = root.path().join("algorithm.rs");
    std_fs::write(&algorithm, b"use std::fs;\n\npub struct Example;\n").unwrap();

    let error = Artifacts::validate_user_generated(&[algorithm]).unwrap_err();

    assert!(matches!(error, BpError::Validation(_)));
    assert!(error.to_string().contains("user generated code"));
}
