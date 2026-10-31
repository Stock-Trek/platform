use super::*;

#[test]
fn batch_fingerprints_are_stable_and_order_sensitive() {
    let first = IcebergDataFile::new("s3://archive/a.parquet", 1, 2);
    let second = IcebergDataFile::new("s3://archive/b.parquet", 3, 4);

    assert_eq!(
        IcebergCatalog::batch_fingerprint(&[first.clone(), second.clone()]),
        IcebergCatalog::batch_fingerprint(&[first.clone(), second.clone()])
    );
    assert_ne!(
        IcebergCatalog::batch_fingerprint(&[first.clone(), second.clone()]),
        IcebergCatalog::batch_fingerprint(&[second, first])
    );
}
