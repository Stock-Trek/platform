#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergDataFile {
    pub file_path: String,
    pub file_size_in_bytes: i64,
    pub record_count: i64,
}

impl IcebergDataFile {
    pub fn new(file_path: impl Into<String>, file_size_in_bytes: i64, record_count: i64) -> Self {
        Self {
            file_path: file_path.into(),
            file_size_in_bytes,
            record_count,
        }
    }
}
