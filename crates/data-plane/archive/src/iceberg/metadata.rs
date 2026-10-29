use super::avro::{self, AvroSchema, AvroValue};
use platform_errors::{BpError, BpResult};
use serde_json::{Value, json};
use std::fmt::Display;

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

pub(crate) fn manifest(
    snapshot_id: i64,
    sequence_number: i64,
    files: &[IcebergDataFile],
) -> BpResult<Vec<u8>> {
    let records = files
        .iter()
        .map(|file| {
            AvroValue::Record(vec![
                AvroValue::Int(1),
                AvroValue::Long(snapshot_id),
                AvroValue::Long(sequence_number),
                AvroValue::Long(sequence_number),
                AvroValue::Record(vec![
                    AvroValue::Int(0),
                    AvroValue::String(file.file_path.clone()),
                    AvroValue::String("PARQUET".to_string()),
                    AvroValue::Record(Vec::new()),
                    AvroValue::Long(file.record_count),
                    AvroValue::Long(file.file_size_in_bytes),
                    AvroValue::Null,
                    AvroValue::Null,
                    AvroValue::Null,
                    AvroValue::Null,
                    AvroValue::Null,
                    AvroValue::Null,
                    AvroValue::Null,
                    AvroValue::Null,
                    AvroValue::Null,
                    AvroValue::Null,
                ]),
            ])
        })
        .collect::<Vec<_>>();

    avro::encode(&manifest_schema().to_string(), &records)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManifestListEntry {
    pub(crate) manifest_path: String,
    pub(crate) manifest_length: i64,
    pub(crate) partition_spec_id: i32,
    pub(crate) content: i32,
    pub(crate) sequence_number: i64,
    pub(crate) min_sequence_number: i64,
    pub(crate) added_snapshot_id: i64,
    pub(crate) added_files_count: i32,
    pub(crate) existing_files_count: i32,
    pub(crate) deleted_files_count: i32,
    pub(crate) added_rows_count: i64,
    pub(crate) existing_rows_count: i64,
    pub(crate) deleted_rows_count: i64,
}

impl ManifestListEntry {
    pub(crate) fn added(
        manifest_path: String,
        manifest_length: i64,
        snapshot_id: i64,
        sequence_number: i64,
        files: &[IcebergDataFile],
    ) -> Self {
        Self {
            manifest_path,
            manifest_length,
            partition_spec_id: 0,
            content: 0,
            sequence_number,
            min_sequence_number: sequence_number,
            added_snapshot_id: snapshot_id,
            added_files_count: i32::try_from(files.len()).unwrap_or(i32::MAX),
            existing_files_count: 0,
            deleted_files_count: 0,
            added_rows_count: files
                .iter()
                .fold(0_i64, |total, file| total.saturating_add(file.record_count)),
            existing_rows_count: 0,
            deleted_rows_count: 0,
        }
    }
}

pub(crate) fn manifest_list(entries: &[ManifestListEntry]) -> BpResult<Vec<u8>> {
    let records = entries
        .iter()
        .map(|entry| {
            AvroValue::Record(vec![
                AvroValue::String(entry.manifest_path.clone()),
                AvroValue::Long(entry.manifest_length),
                AvroValue::Int(entry.partition_spec_id),
                AvroValue::Int(entry.content),
                AvroValue::Long(entry.sequence_number),
                AvroValue::Long(entry.min_sequence_number),
                AvroValue::Long(entry.added_snapshot_id),
                AvroValue::Int(entry.added_files_count),
                AvroValue::Int(entry.existing_files_count),
                AvroValue::Int(entry.deleted_files_count),
                AvroValue::Long(entry.added_rows_count),
                AvroValue::Long(entry.existing_rows_count),
                AvroValue::Long(entry.deleted_rows_count),
                AvroValue::Null,
                AvroValue::Null,
            ])
        })
        .collect::<Vec<_>>();

    avro::encode(&manifest_list_schema().to_string(), &records)
}

pub(crate) fn read_manifest_list(bytes: &[u8]) -> BpResult<Vec<ManifestListEntry>> {
    let (schema, records) = avro::decode(bytes)?;
    records
        .iter()
        .map(|record| -> BpResult<ManifestListEntry> {
            Ok(ManifestListEntry {
                manifest_path: string_field(&schema, record, "manifest_path")?.to_string(),
                manifest_length: long_field(&schema, record, "manifest_length")?,
                partition_spec_id: integer_field(&schema, record, "partition_spec_id")?,
                content: integer_field(&schema, record, "content")?,
                sequence_number: long_field(&schema, record, "sequence_number")?,
                min_sequence_number: long_field(&schema, record, "min_sequence_number")?,
                added_snapshot_id: long_field(&schema, record, "added_snapshot_id")?,
                added_files_count: integer_field(&schema, record, "added_files_count")?,
                existing_files_count: integer_field(&schema, record, "existing_files_count")?,
                deleted_files_count: integer_field(&schema, record, "deleted_files_count")?,
                added_rows_count: long_field(&schema, record, "added_rows_count")?,
                existing_rows_count: long_field(&schema, record, "existing_rows_count")?,
                deleted_rows_count: long_field(&schema, record, "deleted_rows_count")?,
            })
        })
        .map(|entry| entry.map_err(|error| metadata_error("read manifest list", error)))
        .collect()
}

pub(crate) fn read_data_file_paths(bytes: &[u8]) -> BpResult<Vec<String>> {
    let (schema, records) = avro::decode(bytes)?;
    let mut paths = Vec::new();
    for record in &records {
        let status = integer_field(&schema, record, "status")
            .map_err(|error| metadata_error("read manifest entry", error))?;
        if status == 2 {
            continue;
        }
        let data_file = record_field(&schema, record, "data_file")
            .map_err(|error| metadata_error("read manifest entry", error))?;
        let data_file_schema = field_schema(&schema, "data_file")
            .map_err(|error| metadata_error("read manifest entry", error))?;
        let path = string_field(data_file_schema, data_file, "file_path")
            .map_err(|error| metadata_error("read manifest entry", error))?;
        paths.push(path.to_string());
    }
    Ok(paths)
}

fn manifest_schema() -> Value {
    json!({
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {"name": "status", "type": "int", "field-id": 0},
            {"name": "snapshot_id", "type": ["null", "long"], "default": null, "field-id": 1},
            {"name": "sequence_number", "type": ["null", "long"], "default": null, "field-id": 3},
            {"name": "file_sequence_number", "type": ["null", "long"], "default": null, "field-id": 4},
            {
                "name": "data_file",
                "field-id": 2,
                "type": {
                    "type": "record",
                    "name": "r2",
                    "fields": [
                        {"name": "content", "type": "int", "field-id": 134},
                        {"name": "file_path", "type": "string", "field-id": 100},
                        {"name": "file_format", "type": "string", "field-id": 101},
                        {"name": "partition", "type": {"type": "record", "name": "r102", "fields": []}, "field-id": 102},
                        {"name": "record_count", "type": "long", "field-id": 103},
                        {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
                        {"name": "column_sizes", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k117_v118", "fields": [{"name": "key", "type": "int", "field-id": 117}, {"name": "value", "type": "long", "field-id": 118}]}, "logicalType": "map"}], "default": null, "field-id": 108},
                        {"name": "value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k119_v120", "fields": [{"name": "key", "type": "int", "field-id": 119}, {"name": "value", "type": "long", "field-id": 120}]}, "logicalType": "map"}], "default": null, "field-id": 109},
                        {"name": "null_value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k121_v122", "fields": [{"name": "key", "type": "int", "field-id": 121}, {"name": "value", "type": "long", "field-id": 122}]}, "logicalType": "map"}], "default": null, "field-id": 110},
                        {"name": "nan_value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k138_v139", "fields": [{"name": "key", "type": "int", "field-id": 138}, {"name": "value", "type": "long", "field-id": 139}]}, "logicalType": "map"}], "default": null, "field-id": 137},
                        {"name": "lower_bounds", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k126_v127", "fields": [{"name": "key", "type": "int", "field-id": 126}, {"name": "value", "type": "bytes", "field-id": 127}]}, "logicalType": "map"}], "default": null, "field-id": 125},
                        {"name": "upper_bounds", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k129_v130", "fields": [{"name": "key", "type": "int", "field-id": 129}, {"name": "value", "type": "bytes", "field-id": 130}]}, "logicalType": "map"}], "default": null, "field-id": 128},
                        {"name": "key_metadata", "type": ["null", "bytes"], "default": null, "field-id": 131},
                        {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long", "element-id": 133}], "default": null, "field-id": 132},
                        {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int", "element-id": 136}], "default": null, "field-id": 135},
                        {"name": "sort_order_id", "type": ["null", "int"], "default": null, "field-id": 140}
                    ]
                }
            }
        ]
    })
}

fn manifest_list_schema() -> Value {
    json!({
        "type": "record",
        "name": "manifest_file",
        "fields": [
            {"name": "manifest_path", "type": "string", "field-id": 500},
            {"name": "manifest_length", "type": "long", "field-id": 501},
            {"name": "partition_spec_id", "type": "int", "field-id": 502},
            {"name": "content", "type": "int", "default": 0, "field-id": 517},
            {"name": "sequence_number", "type": "long", "default": 0, "field-id": 515},
            {"name": "min_sequence_number", "type": "long", "default": 0, "field-id": 516},
            {"name": "added_snapshot_id", "type": "long", "field-id": 503},
            {"name": "added_files_count", "type": "int", "field-id": 504},
            {"name": "existing_files_count", "type": "int", "field-id": 505},
            {"name": "deleted_files_count", "type": "int", "field-id": 506},
            {"name": "added_rows_count", "type": "long", "field-id": 512},
            {"name": "existing_rows_count", "type": "long", "field-id": 513},
            {"name": "deleted_rows_count", "type": "long", "field-id": 514},
            {
                "name": "partitions",
                "type": ["null", {
                    "type": "array",
                    "element-id": 508,
                    "items": {
                        "type": "record",
                        "name": "field_summary",
                        "fields": [
                            {"name": "contains_null", "type": "boolean", "field-id": 509},
                            {"name": "contains_nan", "type": ["null", "boolean"], "default": null, "field-id": 518},
                            {"name": "lower_bound", "type": ["null", "bytes"], "default": null, "field-id": 510},
                            {"name": "upper_bound", "type": ["null", "bytes"], "default": null, "field-id": 511}
                        ]
                    }
                }],
                "default": null,
                "field-id": 507
            },
            {"name": "key_metadata", "type": ["null", "bytes"], "default": null, "field-id": 519}
        ]
    })
}

fn record_field<'a>(
    schema: &AvroSchema,
    value: &'a AvroValue,
    name: &str,
) -> BpResult<&'a AvroValue> {
    let (AvroSchema::Record { fields, .. }, AvroValue::Record(values)) = (schema, value) else {
        return Err(metadata_error(
            "read Avro record",
            "expected a record value",
        ));
    };
    let index = fields
        .iter()
        .position(|field| field.name == name)
        .ok_or_else(|| metadata_error("read Avro record", format!("missing field {name}")))?;
    values
        .get(index)
        .ok_or_else(|| metadata_error("read Avro record", format!("missing value for {name}")))
}

fn field_schema<'a>(schema: &'a AvroSchema, name: &str) -> BpResult<&'a AvroSchema> {
    let AvroSchema::Record { fields, .. } = schema else {
        return Err(metadata_error(
            "read Avro schema",
            "expected a record schema",
        ));
    };
    fields
        .iter()
        .find(|field| field.name == name)
        .map(|field| &field.schema)
        .ok_or_else(|| metadata_error("read Avro schema", format!("missing field {name}")))
}

fn string_field<'a>(schema: &AvroSchema, value: &'a AvroValue, name: &str) -> BpResult<&'a str> {
    match record_field(schema, value, name)? {
        AvroValue::String(value) => Ok(value),
        _ => Err(metadata_error(
            "read Avro field",
            format!("field {name} is not a string"),
        )),
    }
}

fn integer_field(schema: &AvroSchema, value: &AvroValue, name: &str) -> BpResult<i32> {
    match record_field(schema, value, name)? {
        AvroValue::Int(value) => Ok(*value),
        _ => Err(metadata_error(
            "read Avro field",
            format!("field {name} is not an int"),
        )),
    }
}

fn long_field(schema: &AvroSchema, value: &AvroValue, name: &str) -> BpResult<i64> {
    match record_field(schema, value, name)? {
        AvroValue::Long(value) => Ok(*value),
        _ => Err(metadata_error(
            "read Avro field",
            format!("field {name} is not a long"),
        )),
    }
}

fn metadata_error(operation: &'static str, message: impl Display) -> BpError {
    BpError::io(
        operation,
        "Iceberg metadata",
        std::io::Error::new(std::io::ErrorKind::InvalidData, message.to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn files() -> Vec<IcebergDataFile> {
        vec![
            IcebergDataFile::new("s3://archive/raw/events/one.parquet", 100, 10),
            IcebergDataFile::new("s3://archive/raw/events/two.parquet", 200, 20),
        ]
    }

    #[test]
    fn manifests_round_trip_data_file_paths() {
        let encoded = manifest(7, 3, &files()).unwrap();

        assert_eq!(
            read_data_file_paths(&encoded).unwrap(),
            vec![
                "s3://archive/raw/events/one.parquet",
                "s3://archive/raw/events/two.parquet"
            ]
        );
    }

    #[test]
    fn manifest_lists_round_trip_manifest_paths() {
        let encoded = manifest_list(&[ManifestListEntry::added(
            "s3://archive/warehouse/market_data/raw/events/metadata/manifest.avro".to_string(),
            123,
            7,
            3,
            &files(),
        )])
        .unwrap();

        let entries = read_manifest_list(&encoded).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].manifest_path,
            "s3://archive/warehouse/market_data/raw/events/metadata/manifest.avro"
        );
        assert_eq!(entries[0].added_files_count, 2);
        assert_eq!(entries[0].added_rows_count, 30);
    }

    #[test]
    fn empty_manifests_and_manifest_lists_are_valid() {
        let encoded = manifest(7, 3, &[]).unwrap();
        assert!(read_data_file_paths(&encoded).unwrap().is_empty());

        let encoded = manifest_list(&[ManifestListEntry::added(
            "s3://archive/manifest.avro".to_string(),
            10,
            7,
            3,
            &[],
        )])
        .unwrap();
        let entries = read_manifest_list(&encoded).unwrap();
        assert_eq!(entries[0].manifest_path, "s3://archive/manifest.avro");
    }
}
