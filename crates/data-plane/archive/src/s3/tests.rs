use super::*;

#[test]
fn parses_s3_uris_and_rejects_incomplete_ones() {
    assert_eq!(
        S3::parse_uri("s3://archive/market-data/raw/events/a.parquet").unwrap(),
        (
            Bucket::new("archive"),
            ObjectKey::new("market-data/raw/events/a.parquet")
        )
    );
    assert!(S3::parse_uri("https://archive/key").is_err());
    assert!(S3::parse_uri("s3://archive").is_err());
    assert!(S3::parse_uri("s3:///key").is_err());
    assert!(S3::parse_uri("s3://archive/").is_err());
}
