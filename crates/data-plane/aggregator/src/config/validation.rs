use platform_errors::{BpError, BpResult};

pub(super) fn validate_kv_bucket(name: &str, bucket: &str) -> BpResult<()> {
    let valid = !bucket.is_empty()
        && bucket
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'));
    if valid {
        Ok(())
    } else {
        Err(BpError::config_invalid(
            name,
            "must only contain alphanumeric characters, dashes, and underscores",
        ))
    }
}
