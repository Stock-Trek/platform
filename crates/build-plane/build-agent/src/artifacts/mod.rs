mod artifact;
mod build;
mod publish;
#[cfg(test)]
mod tests;

pub use artifact::Artifacts;
use artifact::{
    CARGO, COMPILE_OUTPUT_FILE, CWASM_ARTIFACT_FILE, SOURCE_TARGET_DIR, USER_GENERATED_ARCHIVE,
    USER_GENERATED_DIR, WASM_ARTIFACT_FILE, WASM_MANIFEST, WASM_TARGET, WASMTIME,
    WORKSPACE_ENTRIES,
};
