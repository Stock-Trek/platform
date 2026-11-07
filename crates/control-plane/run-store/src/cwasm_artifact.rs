/// A compiled runner artifact and the metadata needed to verify it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CwasmArtifact {
    pub key: String,
    pub sha256: String,
    pub size: u64,
    pub wasmtime_version: String,
}

impl CwasmArtifact {
    pub fn new(
        key: impl Into<String>,
        sha256: impl Into<String>,
        size: u64,
        wasmtime_version: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            sha256: sha256.into(),
            size,
            wasmtime_version: wasmtime_version.into(),
        }
    }
}
