use super::*;
use std::path::Path;

#[test]
fn checked_in_sources_match_proto_files() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("proto-gen lives under the workspace root");
    let out_dir = std::env::temp_dir().join(format!("proto-gen-test-{}", std::process::id()));
    ProtoGen::generate(&root.join("proto"), &out_dir).expect("failed to generate DTOs");

    for name in ["dto.rs", "mod.rs"] {
        let expected = std::fs::read_to_string(root.join(GENERATED_DIR).join(name))
            .unwrap_or_else(|error| panic!("failed to read checked-in {name}: {error}"));
        let actual = std::fs::read_to_string(out_dir.join(name))
            .unwrap_or_else(|error| panic!("failed to read generated {name}: {error}"));
        assert_eq!(
            expected, actual,
            "crates/sandbox-plane/runner/src/generated/{name} is out of date; run `cargo run -p platform-proto-gen`"
        );
    }

    std::fs::remove_dir_all(&out_dir).ok();
}
