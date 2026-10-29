use super::{
    GitHubInstallation,
    types::{GitHubBlob, GitHubTree, TreeParams},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use platform_errors::{BpError, BpResult, GitHubError};
use platform_git::git_host::GitFile;
use std::path::Path;

impl GitHubInstallation {
    pub(super) async fn collect_directory_files(
        &self,
        repo_id: u64,
        ref_name: &str,
        directory: &str,
    ) -> BpResult<Vec<GitFile>> {
        let directory = directory.trim_matches('/');
        if !directory.is_empty() {
            Self::validate_relative_path(repo_id, directory, "")?;
        }

        // The Contents API silently caps directory listings at 1,000 entries.
        // The Git Trees API returns the complete tree (and reports when GitHub
        // had to truncate it), so use that for directory discovery instead.
        let tree = self.tree(repo_id, ref_name).await?;
        if tree.truncated {
            return Err(BpError::GitHub(GitHubError::TreeTruncated {
                repo_id,
                ref_name: ref_name.to_owned(),
            }));
        }

        if !directory.is_empty()
            && !tree
                .tree
                .iter()
                .any(|entry| entry.path == directory && entry.kind == "tree")
        {
            return Err(BpError::GitHub(GitHubError::DirectoryNotFound {
                repo_id,
                path: directory.to_owned(),
                ref_name: ref_name.to_owned(),
            }));
        }

        let prefix = if directory.is_empty() {
            String::new()
        } else {
            format!("{directory}/")
        };
        let mut files = Vec::new();
        let mut total_bytes = 0_u64;
        for entry in tree.tree {
            let Some(relative) = entry.path.strip_prefix(&prefix) else {
                continue;
            };
            if relative.is_empty() {
                continue;
            }

            let depth = relative.matches('/').count();
            if depth > self.directory_limits.max_depth {
                return Err(BpError::GitHub(GitHubError::DirectoryDepthExceeded {
                    repo_id,
                    path: entry.path,
                    max_depth: self.directory_limits.max_depth,
                }));
            }
            Self::validate_relative_path(repo_id, relative, directory)?;

            match entry.kind.as_str() {
                "tree" => {}
                "blob" => {
                    // Symlinks are blobs with mode 120000; like submodules they
                    // are not part of the source archive.
                    if entry.mode == "120000" {
                        continue;
                    }
                    if files.len() >= self.directory_limits.max_files {
                        return Err(BpError::GitHub(GitHubError::TooManyFiles {
                            repo_id,
                            directory: directory.to_owned(),
                            max_files: self.directory_limits.max_files,
                        }));
                    }
                    if let Some(size) = entry.size
                        && total_bytes.saturating_add(size) > self.directory_limits.max_total_bytes
                    {
                        return Err(BpError::GitHub(GitHubError::DirectoryTooLarge {
                            repo_id,
                            directory: directory.to_owned(),
                            max_bytes: self.directory_limits.max_total_bytes,
                        }));
                    }
                    let contents = self.blob(repo_id, &entry.sha).await?;
                    total_bytes = total_bytes.saturating_add(contents.len() as u64);
                    if total_bytes > self.directory_limits.max_total_bytes {
                        return Err(BpError::GitHub(GitHubError::DirectoryTooLarge {
                            repo_id,
                            directory: directory.to_owned(),
                            max_bytes: self.directory_limits.max_total_bytes,
                        }));
                    }
                    files.push(GitFile {
                        path: relative.to_owned(),
                        contents,
                    });
                }
                "commit" => {}
                kind => {
                    return Err(BpError::GitHub(GitHubError::UnsupportedContentType {
                        repo_id,
                        path: entry.path,
                        content_type: kind.to_owned(),
                    }));
                }
            }
        }
        files.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(files)
    }

    async fn tree(&self, repo_id: u64, ref_name: &str) -> BpResult<GitHubTree> {
        let params = TreeParams { recursive: 1 };
        self.client
            .get::<GitHubTree, _, _>(
                format!(
                    "/repositories/{repo_id}/git/trees/{}",
                    Self::encode_path(ref_name)
                ),
                Some(&params),
            )
            .await
            .map_err(|source| {
                BpError::GitHub(GitHubError::GetTree {
                    repo_id,
                    ref_name: ref_name.to_owned(),
                    source,
                })
            })
    }

    async fn blob(&self, repo_id: u64, sha: &str) -> BpResult<Vec<u8>> {
        let blob = self
            .client
            .get::<GitHubBlob, _, _>(
                format!("/repositories/{repo_id}/git/blobs/{sha}"),
                None::<&()>,
            )
            .await
            .map_err(|source| {
                BpError::GitHub(GitHubError::GetBlob {
                    repo_id,
                    sha: sha.to_owned(),
                    source,
                })
            })?;
        Self::decode_content(
            repo_id,
            sha,
            blob.content.as_deref(),
            blob.encoding.as_deref(),
        )?
        .ok_or_else(|| {
            BpError::GitHub(GitHubError::MissingContent {
                repo_id,
                path: sha.to_owned(),
            })
        })
    }

    fn decode_content(
        repo_id: u64,
        path: &str,
        content: Option<&str>,
        encoding: Option<&str>,
    ) -> BpResult<Option<Vec<u8>>> {
        let Some(content) = content else {
            return Ok(None);
        };
        match encoding {
            Some("base64") => {
                let compact = content
                    .chars()
                    .filter(|character| !character.is_ascii_whitespace())
                    .collect::<String>();
                BASE64.decode(compact).map(Some).map_err(|source| {
                    BpError::GitHub(GitHubError::DecodeContent {
                        repo_id,
                        path: path.to_owned(),
                        encoding: "base64",
                        source,
                    })
                })
            }
            Some("utf8") => Ok(Some(content.as_bytes().to_vec())),
            Some("none") | None if content.is_empty() => Ok(None),
            Some(encoding) => Err(BpError::GitHub(GitHubError::UnsupportedContentEncoding {
                repo_id,
                path: path.to_owned(),
                encoding: encoding.to_owned(),
            })),
            None => Ok(Some(content.as_bytes().to_vec())),
        }
    }

    fn encode_path(path: &str) -> String {
        path.split('/')
            .map(|segment| {
                let mut encoded = String::new();
                for byte in segment.bytes() {
                    match byte {
                        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                            encoded.push(char::from(byte))
                        }
                        byte => encoded.push_str(&format!("%{byte:02X}")),
                    }
                }
                encoded
            })
            .collect::<Vec<_>>()
            .join("/")
    }

    fn validate_relative_path(repo_id: u64, path: &str, directory: &str) -> BpResult<()> {
        let path = Path::new(path);
        if path.as_os_str().is_empty()
            || path.to_string_lossy().contains('\\')
            || path
                .components()
                .any(|component| !matches!(component, std::path::Component::Normal(_)))
        {
            return Err(BpError::GitHub(GitHubError::InvalidContentPath {
                repo_id,
                path: path.display().to_string(),
                directory: directory.to_owned(),
            }));
        }
        Ok(())
    }
}
