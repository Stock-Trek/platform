use crate::iceberg::{ArchiveRegistration, IcebergDataFile};

fn file(path: &str) -> IcebergDataFile {
    IcebergDataFile::new(path.to_string(), 10, 2)
}

#[test]
fn registration_sorts_and_deduplicates_outputs() {
    let registration = ArchiveRegistration::new(
        vec![
            file("s3://bucket/b.parquet"),
            file("s3://bucket/a.parquet"),
            file("s3://bucket/a.parquet"),
        ],
        None::<Vec<String>>,
    )
    .unwrap();

    assert_eq!(
        registration
            .files()
            .iter()
            .map(|file| file.file_path.as_str())
            .collect::<Vec<_>>(),
        vec!["s3://bucket/a.parquet", "s3://bucket/b.parquet"]
    );
    assert_eq!(
        registration.expected_outputs(),
        ["s3://bucket/a.parquet", "s3://bucket/b.parquet"]
            .into_iter()
            .map(str::to_string)
            .collect()
    );
}

#[test]
fn registration_sorts_and_deduplicates_sources() {
    let registration = ArchiveRegistration::new(
        vec![file("s3://bucket/out.parquet")],
        Some(vec![
            "s3://bucket/raw-2.parquet".to_string(),
            "s3://bucket/raw-1.parquet".to_string(),
            "s3://bucket/raw-1.parquet".to_string(),
        ]),
    )
    .unwrap();

    assert_eq!(
        registration.source_files().unwrap(),
        ["s3://bucket/raw-1.parquet", "s3://bucket/raw-2.parquet"]
    );
}

#[test]
fn registration_rejects_an_empty_source_batch() {
    let error = ArchiveRegistration::new(
        vec![file("s3://bucket/out.parquet")],
        Some(Vec::<String>::new()),
    )
    .unwrap_err();

    assert!(error.to_string().contains("at least one source object"));
}

#[test]
fn registration_rejects_an_empty_registration() {
    let error =
        ArchiveRegistration::new(Vec::<IcebergDataFile>::new(), None::<Vec<String>>).unwrap_err();

    assert!(error.to_string().contains("must contain data files"));
}
