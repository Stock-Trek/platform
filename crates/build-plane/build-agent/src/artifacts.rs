use crate::{
    s3::SignedPost,
    source::{Source, SourceLimits},
};
use platform_errors::{BpError, BpResult, ConfigError, IoError, ValidationError};
use std::{
    fs as std_fs,
    io::{self, Cursor, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tempfile::TempDir;
use tokio::{fs, process::Command};
use tracing::info;
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

const WASM_MANIFEST: &str = "crates/compute/runner/Cargo.toml";
const WASM_TARGET: &str = "wasm32-wasip1";
const WASM_ARTIFACT_FILE: &str = "runner.wasm";
const CWASM_ARTIFACT_FILE: &str = "runner.cwasm";
const SOURCE_TARGET_DIR: &str = "target";
const USER_GENERATED_DIR: &str = "crates/compute/runner/src/user_generated";
const USER_GENERATED_ARCHIVE: &str = "sources.zip";
pub const COMPILE_OUTPUT_FILE: &str = "compile-output.txt";
const WORKSPACE_ENTRIES: &[&str] = &[
    "Cargo.toml",
    "Cargo.lock",
    "crates",
    ".cargo",
    "rust-toolchain.toml",
];
const CARGO: &str = "cargo";
const WASMTIME: &str = "wasmtime";

#[derive(Clone)]
pub struct Artifacts {
    signed_post: SignedPost,
    source_get_url: String,
    source_dir: PathBuf,
    build_cache_dir: PathBuf,
    build_dir: Arc<TempDir>,
    source_limits: SourceLimits,
    compile_output: Arc<Mutex<Option<Vec<u8>>>>,
}

impl Artifacts {
    #[cfg(test)]
    pub fn new(
        signed_post: SignedPost,
        source_get_url: String,
        source_dir: PathBuf,
        build_cache_dir: PathBuf,
    ) -> BpResult<Self> {
        Self::with_source_limits(
            signed_post,
            source_get_url,
            source_dir,
            build_cache_dir,
            SourceLimits::default(),
        )
    }

    pub fn with_source_limits(
        signed_post: SignedPost,
        source_get_url: String,
        source_dir: PathBuf,
        build_cache_dir: PathBuf,
        source_limits: SourceLimits,
    ) -> BpResult<Self> {
        let build_dir = tempfile::tempdir().map_err(|source| {
            BpError::io(
                "create build directory",
                "temporary build directory",
                source,
            )
        })?;
        Ok(Self {
            signed_post,
            source_get_url,
            source_dir,
            build_cache_dir,
            build_dir: Arc::new(build_dir),
            source_limits,
            compile_output: Arc::new(Mutex::new(None)),
        })
    }

    pub async fn compile(&self) -> BpResult<PathBuf> {
        self.prepare_workspace()?;
        let sources = self.unpack_source().await?;
        Self::validate_user_generated(&sources)?;
        let target_dir = self.prepare_target_dir().await?;
        let wasm = self.build_wasm(target_dir.path()).await?;
        self.compile_wasm(&wasm).await
    }

    fn prepare_workspace(&self) -> BpResult<()> {
        let workspace = self.build_dir.path();
        if !self.source_dir.join("Cargo.toml").is_file() {
            return Err(BpError::Config(ConfigError::Build(format!(
                "build workspace source {} does not contain Cargo.toml",
                self.source_dir.display()
            ))));
        }

        std_fs::create_dir_all(workspace).map_err(|source| {
            BpError::io(
                "prepare build workspace",
                workspace.display().to_string(),
                source,
            )
        })?;
        for entry in WORKSPACE_ENTRIES {
            let from = self.source_dir.join(entry);
            if !from.exists() {
                continue;
            }
            let to = workspace.join(entry);
            copy_path(&from, &to).map_err(|source| {
                BpError::io(
                    "copy build workspace entry",
                    from.display().to_string(),
                    source,
                )
            })?;
        }
        Ok(())
    }

    async fn unpack_source(&self) -> BpResult<Vec<PathBuf>> {
        let source = Source::download(&self.source_get_url, self.source_limits).await?;
        source.extract(
            self.build_dir.path(),
            self.source_limits.max_extracted_bytes,
        )
    }

    fn validate_user_generated(sources: &[PathBuf]) -> BpResult<()> {
        for path in sources {
            stock_trek_check::verify::verify(path.display().to_string()).map_err(|error| {
                BpError::Validation(ValidationError::InvalidValue {
                    name: format!("user generated code {}", path.display()),
                    value: error.to_string(),
                })
            })?;
        }
        info!(files = sources.len(), "validated user generated code");
        Ok(())
    }

    async fn build_wasm(&self, target_dir: &Path) -> BpResult<PathBuf> {
        let workspace = self.build_dir.path();
        let manifest = workspace.join(WASM_MANIFEST);
        let module = target_dir
            .join(WASM_TARGET)
            .join("release")
            .join(WASM_ARTIFACT_FILE);
        let output = Command::new(CARGO)
            .arg("build")
            .arg("--manifest-path")
            .arg(&manifest)
            .arg("--target")
            .arg(WASM_TARGET)
            .arg("--target-dir")
            .arg(target_dir)
            .arg("--release")
            .arg("--lib")
            .arg("--locked")
            .arg("--offline")
            .current_dir(workspace)
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|source| {
                BpError::io("compile artifact", module.display().to_string(), source)
            })?;
        self.set_compile_output(Self::command_output("cargo build", &output));

        if !output.status.success() {
            return Err(BpError::command_failed(
                "compile artifact",
                module,
                output.status,
            ));
        }
        info!(
            manifest = %manifest.display(),
            target = WASM_TARGET,
            target_dir = %target_dir.display(),
            "built wasm artifact"
        );
        Ok(module)
    }

    /// Create a pristine target directory for this invocation.
    ///
    /// The prebuilt target directory baked into the image is copied into a
    /// fresh temporary directory every time, so cargo never sees artifacts
    /// produced by user code from an earlier invocation.
    async fn prepare_target_dir(&self) -> BpResult<TempDir> {
        let cache_root = self.build_cache_dir.clone();
        std_fs::create_dir_all(&cache_root).map_err(|source| {
            BpError::io(
                "prepare build cache",
                cache_root.display().to_string(),
                source,
            )
        })?;
        let target_dir = tempfile::Builder::new()
            .prefix("platform-target-")
            .tempdir_in(&cache_root)
            .map_err(|source| {
                BpError::io(
                    "prepare build cache",
                    cache_root.display().to_string(),
                    source,
                )
            })?;

        let source = self.source_dir.join(SOURCE_TARGET_DIR);
        if source.is_dir() {
            let from = source;
            let to = target_dir.path().to_path_buf();
            tokio::task::spawn_blocking(move || copy_path(&from, &to))
                .await
                .map_err(|source| {
                    BpError::Config(ConfigError::Build(format!(
                        "failed to prepare build cache: {source}"
                    )))
                })?
                .map_err(|source| {
                    BpError::io(
                        "copy build cache",
                        target_dir.path().display().to_string(),
                        source,
                    )
                })?;
        }

        Ok(target_dir)
    }

    async fn compile_wasm(&self, module: &Path) -> BpResult<PathBuf> {
        let output = self.build_dir.path().join(CWASM_ARTIFACT_FILE);
        let command_output = Command::new(WASMTIME)
            .arg("compile")
            .arg("-C")
            .arg("cache=no")
            .arg(module)
            .arg("-o")
            .arg(&output)
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|source| {
                BpError::io("compile artifact", module.display().to_string(), source)
            })?;
        self.set_compile_output(Self::command_output("wasmtime compile", &command_output));

        if !command_output.status.success() {
            return Err(BpError::command_failed(
                "compile artifact",
                module.to_path_buf(),
                command_output.status,
            ));
        }
        info!(module = %module.display(), output = %output.display(), "compiled wasm artifact");
        Ok(output)
    }

    pub async fn publish(&self, cwasm: &Path) -> BpResult<()> {
        self.publish_source().await?;
        self.publish_cwasm(cwasm).await?;
        info!("published build artifacts");
        Ok(())
    }

    pub async fn publish_failure(&self, error: &BpError) -> BpResult<()> {
        let source_result = self.publish_source().await;
        if let Err(source_error) = &source_result {
            tracing::error!(
                error = %source_error,
                "failed to publish source archive for failed build"
            );
        }
        let output_result = self.publish_compile_output(error).await;
        if let Err(output_error) = &output_result {
            tracing::error!(
                error = %output_error,
                "failed to publish compile output for failed build"
            );
        }
        output_result?;
        source_result
    }

    async fn publish_source(&self) -> BpResult<()> {
        let root = self.build_dir.path().join(USER_GENERATED_DIR);
        let files = Self::user_generated_files(&root).await?;
        let archive = Self::source_archive(&root, &files)?;
        let archive_path = self.build_dir.path().join(USER_GENERATED_ARCHIVE);
        fs::write(&archive_path, archive).await.map_err(|source| {
            BpError::io(
                "write source archive",
                archive_path.display().to_string(),
                source,
            )
        })?;
        self.signed_post
            .upload_named(&archive_path, Path::new(USER_GENERATED_ARCHIVE))
            .await?;
        info!(files = files.len(), "published source archive");
        Ok(())
    }

    async fn publish_cwasm(&self, cwasm: &Path) -> BpResult<()> {
        self.signed_post.upload(cwasm).await?;
        info!(artifact = %cwasm.display(), "published compiled artifact");
        Ok(())
    }

    async fn publish_compile_output(&self, error: &BpError) -> BpResult<()> {
        let mut output = self.take_compile_output().unwrap_or_default();
        if output.is_empty() {
            output = format!("build failed: {error}\n").into_bytes();
        } else {
            if !output.ends_with(b"\n") {
                output.push(b'\n');
            }
            output.extend_from_slice(format!("build failed: {error}\n").as_bytes());
        }
        let output_path = self.build_dir.path().join(COMPILE_OUTPUT_FILE);
        fs::write(&output_path, &output).await.map_err(|source| {
            BpError::io(
                "write compile output",
                output_path.display().to_string(),
                source,
            )
        })?;
        self.signed_post
            .upload_named(&output_path, Path::new(COMPILE_OUTPUT_FILE))
            .await?;
        info!(bytes = output.len(), "published compile output");
        Ok(())
    }

    fn command_output(command: &str, output: &std::process::Output) -> Vec<u8> {
        let mut captured = Vec::new();
        let _ = write!(
            captured,
            "command: {command}\nstatus: {}\n\nstdout:\n",
            output.status
        );
        captured.extend_from_slice(&output.stdout);
        if !output.stdout.ends_with(b"\n") {
            captured.push(b'\n');
        }
        captured.extend_from_slice(b"\nstderr:\n");
        captured.extend_from_slice(&output.stderr);
        if !output.stderr.ends_with(b"\n") {
            captured.push(b'\n');
        }
        captured
    }

    fn set_compile_output(&self, output: Vec<u8>) {
        if let Ok(mut current) = self.compile_output.lock() {
            *current = Some(output);
        }
    }

    fn take_compile_output(&self) -> Option<Vec<u8>> {
        self.compile_output
            .lock()
            .ok()
            .and_then(|mut output| output.take())
    }

    fn source_archive(root: &Path, files: &[PathBuf]) -> BpResult<Vec<u8>> {
        let mut archive = ZipWriter::new(Cursor::new(Vec::new()));
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
        for file in files {
            let relative = file.strip_prefix(root).map_err(|_| {
                BpError::Config(ConfigError::Build(format!(
                    "artifact path {} is outside {USER_GENERATED_DIR}",
                    file.display()
                )))
            })?;
            let name = relative
                .to_str()
                .ok_or_else(|| {
                    BpError::Config(ConfigError::Build(format!(
                        "artifact path {} is not valid UTF-8",
                        file.display()
                    )))
                })?
                .replace('\\', "/");
            archive
                .start_file(name, options)
                .map_err(|error| BpError::Io(IoError::Zip(error)))?;
            let contents = std_fs::read(file).map_err(|source| {
                BpError::io("read artifact", file.display().to_string(), source)
            })?;
            archive.write_all(&contents).map_err(|source| {
                BpError::io("write source archive", file.display().to_string(), source)
            })?;
        }
        Ok(archive
            .finish()
            .map_err(|error| BpError::Io(IoError::Zip(error)))?
            .into_inner())
    }

    async fn user_generated_files(root: &Path) -> BpResult<Vec<PathBuf>> {
        let root = fs::canonicalize(root).await.map_err(|source| {
            BpError::io(
                "read artifact directory",
                root.display().to_string(),
                source,
            )
        })?;
        let mut files = Vec::new();
        let mut pending = vec![root.clone()];
        while let Some(directory) = pending.pop() {
            let mut entries = fs::read_dir(&directory).await.map_err(|source| {
                BpError::io(
                    "read artifact directory",
                    directory.display().to_string(),
                    source,
                )
            })?;
            while let Some(entry) = entries.next_entry().await.map_err(|source| {
                BpError::io(
                    "read artifact directory",
                    directory.display().to_string(),
                    source,
                )
            })? {
                let path = entry.path();
                let file_type = entry.file_type().await.map_err(|source| {
                    BpError::io(
                        "read artifact directory",
                        directory.display().to_string(),
                        source,
                    )
                })?;
                let resolved = fs::canonicalize(&path).await.map_err(|source| {
                    BpError::io("read artifact", path.display().to_string(), source)
                })?;
                if !resolved.starts_with(&root) {
                    return Err(BpError::Config(ConfigError::Build(format!(
                        "artifact path {} resolves outside {USER_GENERATED_DIR}",
                        path.display()
                    ))));
                }

                let target = fs::metadata(&resolved).await.map_err(|source| {
                    BpError::io("read artifact", path.display().to_string(), source)
                })?;
                if target.is_dir() {
                    if file_type.is_symlink() {
                        return Err(BpError::Config(ConfigError::Build(format!(
                            "artifact path {} is a symlinked directory",
                            path.display()
                        ))));
                    }
                    pending.push(path);
                } else if target.is_file() {
                    files.push(path);
                } else {
                    return Err(BpError::Config(ConfigError::Build(format!(
                        "artifact path {} is not a regular file or directory",
                        path.display()
                    ))));
                }
            }
        }
        files.sort();
        Ok(files)
    }
}

/// Recursively copy `from` to `to`, preserving regular files, directories and
/// symlinks. The source tree is the trusted, read-only workspace baked into the
/// image; user archives are extracted into the writable copy instead.
fn copy_path(from: &Path, to: &Path) -> io::Result<()> {
    let metadata = std_fs::symlink_metadata(from)?;
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        let target = std_fs::read_link(from)?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(target, to)?;
        #[cfg(not(unix))]
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("cannot copy symlink {}", from.display()),
        ));
    } else if file_type.is_dir() {
        std_fs::create_dir_all(to)?;
        for entry in std_fs::read_dir(from)? {
            let entry = entry?;
            copy_path(&entry.path(), &to.join(entry.file_name()))?;
        }
    } else if file_type.is_file() {
        if let Some(parent) = to.parent() {
            std_fs::create_dir_all(parent)?;
        }
        std_fs::copy(from, to)?;
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported workspace entry {}", from.display()),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    async fn upload_server(
        expected_uploads: usize,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
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
}
