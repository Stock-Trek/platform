use crate::{GENERATED_DIR, ProtoGen};
use std::path::Path;

pub struct ProtoGenApp;

impl ProtoGenApp {
    pub fn run() -> Result<(), Box<dyn std::error::Error>> {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(3)
            .ok_or("proto-gen lives under the workspace root")?;
        ProtoGen::generate(&root.join("proto"), &root.join(GENERATED_DIR))?;
        Ok(())
    }
}
