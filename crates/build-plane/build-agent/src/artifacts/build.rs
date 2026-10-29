use super::{
    Artifacts, CARGO, CWASM_ARTIFACT_FILE, SOURCE_TARGET_DIR, WASM_ARTIFACT_FILE, WASM_MANIFEST,
    WASM_TARGET, WASMTIME, WORKSPACE_ENTRIES,
};
use crate::source::Source;
use platform_errors::{BpError, BpResult, ConfigError, ValidationError};
use std::{
    fs as std_fs, io,
    path::{Path, PathBuf},
};
use tempfile::TempDir;
use tokio::process::Command;
use tracing::info;

impl Artifacts {
    pub(super) fn prepare_workspace(&self) -> BpResult<()> {
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
            Self::copy_path(&from, &to).map_err(|source| {
                BpError::io(
                    "copy build workspace entry",
                    from.display().to_string(),
                    source,
                )
            })?;
        }
        Ok(())
    }

    pub(super) async fn unpack_source(&self) -> BpResult<Vec<PathBuf>> {
        let source = Source::download(&self.source_get_url, self.source_limits).await?;
        source.extract(
            self.build_dir.path(),
            self.source_limits.max_extracted_bytes,
        )
    }

    pub(super) fn validate_user_generated(sources: &[PathBuf]) -> BpResult<()> {
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

    pub(super) async fn build_wasm(&self, target_dir: &Path) -> BpResult<PathBuf> {
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
    pub(super) async fn prepare_target_dir(&self) -> BpResult<TempDir> {
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
            tokio::task::spawn_blocking(move || Self::copy_path(&from, &to))
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

    pub(super) async fn compile_wasm(&self, module: &Path) -> BpResult<PathBuf> {
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

    /// Recursively copy `from` to `to`, preserving regular files, directories
    /// and symlinks. The source tree is the trusted, read-only workspace baked
    /// into the image; user archives are extracted into the writable copy
    /// instead.
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
                Self::copy_path(&entry.path(), &to.join(entry.file_name()))?;
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
}
