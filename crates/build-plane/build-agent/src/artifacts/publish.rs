use super::{Artifacts, COMPILE_OUTPUT_FILE, USER_GENERATED_ARCHIVE, USER_GENERATED_DIR};
use platform_errors::{BpError, BpResult, ConfigError, IoError};
use std::{
    fs as std_fs,
    io::{Cursor, Write},
    path::{Path, PathBuf},
};
use tokio::fs;
use tracing::info;
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

impl Artifacts {
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

    pub(crate) fn source_archive(root: &Path, files: &[PathBuf]) -> BpResult<Vec<u8>> {
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
