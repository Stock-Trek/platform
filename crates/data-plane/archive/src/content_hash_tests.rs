use crate::{ContentHash, ContentHasher};

#[test]
fn content_hash_is_truncated_sha256_hex() {
    let hash = ContentHash::from_bytes(b"hello");

    assert_eq!(hash.to_string(), "2cf24dba5fb0a30e26e83b2ac5b9e29e");
    assert_eq!(hash.to_string().len(), 32);
}

#[test]
fn content_hasher_accepts_incremental_input() {
    let mut hasher = ContentHasher::new();
    hasher.update(b"hel");
    hasher.update(b"lo");

    assert_eq!(hasher.finish(), ContentHash::from_bytes(b"hello"));
}

#[test]
fn content_hash_from_file_matches_bytes_and_length() {
    let path =
        std::env::temp_dir().join(format!("platform-content-hash-test-{}", std::process::id()));
    std::fs::write(&path, b"hello").unwrap();

    let (hash, length) = ContentHash::from_file(&path).unwrap();
    std::fs::remove_file(&path).unwrap();

    assert_eq!(hash, ContentHash::from_bytes(b"hello"));
    assert_eq!(length, 5);
}
