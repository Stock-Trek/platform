use crate::{s3::SignedPost, source::SourceLimits};
use platform_errors::{BpError, BpResult};
use std::{
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tempfile::TempDir;

pub(super) const WASM_MANIFEST: &str = "crates/compute/runner/Cargo.toml";
pub(super) const WASM_TARGET: &str = "wasm32-wasip1";
pub(super) const WASM_ARTIFACT_FILE: &str = "runner.wasm";
pub(super) const CWASM_ARTIFACT_FILE: &str = "runner.cwasm";
pub(super) const SOURCE_TARGET_DIR: &str = "target";
pub(super) const USER_GENERATED_DIR: &str = "crates/compute/runner/src/user_generated";
pub(super) const USER_GENERATED_ARCHIVE: &str = "sources.zip";
pub(super) const COMPILE_OUTPUT_FILE: &str = "compile-output.txt";
pub(super) const WORKSPACE_ENTRIES: &[&str] = &[
    "Cargo.toml",
    "Cargo.lock",
    "crates",
    ".cargo",
    "rust-toolchain.toml",
];
pub(super) const CARGO: &str = "cargo";
pub(super) const WASMTIME: &str = "wasmtime";

#[derive(Clone)]
pub struct Artifacts {
    pub(super) signed_post: SignedPost,
    pub(super) source_get_url: String,
    pub(super) source_dir: PathBuf,
    pub(super) build_cache_dir: PathBuf,
    pub(super) build_dir: Arc<TempDir>,
    pub(super) source_limits: SourceLimits,
    pub(super) compile_output: Arc<Mutex<Option<Vec<u8>>>>,
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

    pub(super) fn command_output(command: &str, output: &std::process::Output) -> Vec<u8> {
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

    pub(super) fn set_compile_output(&self, output: Vec<u8>) {
        if let Ok(mut current) = self.compile_output.lock() {
            *current = Some(output);
        }
    }

    pub(super) fn take_compile_output(&self) -> Option<Vec<u8>> {
        self.compile_output
            .lock()
            .ok()
            .and_then(|mut output| output.take())
    }
}
