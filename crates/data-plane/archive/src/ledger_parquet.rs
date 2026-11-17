use crate::{ArchiveKind, CompactionId, LedgerEntry};
use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
};
use platform_errors::{PlatformError, PlatformResult};
use platform_types::TimestampMilliseconds;
use std::{collections::HashMap, sync::Arc};

pub struct LedgerParquet;

impl LedgerParquet {
    pub fn encode(entries: &[LedgerEntry]) -> PlatformResult<Bytes> {
        if entries.is_empty() {
            return Err(Self::error("cannot encode an empty ledger batch"));
        }
        for entry in entries {
            entry.validate()?;
        }
        let schema = Self::schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt32Array::from_iter_values(
                entries.iter().map(|entry| entry.format_version),
            )),
            Arc::new(StringArray::from_iter_values(
                entries.iter().map(|entry| entry.compaction_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                entries.iter().map(|entry| entry.kind.label()),
            )),
            Arc::new(Int64Array::from_iter_values(
                entries.iter().map(|entry| entry.partition_hour_millis),
            )),
            Arc::new(StringArray::from_iter_values(
                entries
                    .iter()
                    .map(|entry| entry.source_manifest_uri.as_str()),
            )),
            Arc::new(Int64Array::from_iter_values(
                entries.iter().map(|entry| entry.source_count),
            )),
            Arc::new(Int64Array::from_iter_values(
                entries.iter().map(|entry| entry.processed_at.0),
            )),
            Arc::new(Int32Array::from_iter_values(
                entries.iter().map(|entry| entry.partition_spec_id),
            )),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(
            &mut buffer,
            schema,
            Some(WriterProperties::builder().build()),
        )?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(Bytes::from(buffer))
    }

    pub fn encode_one(entry: &LedgerEntry) -> PlatformResult<Bytes> {
        Self::encode(std::slice::from_ref(entry))
    }

    pub fn decode(bytes: Bytes) -> PlatformResult<Vec<LedgerEntry>> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
        let mut entries = Vec::new();
        for batch in reader {
            let batch = batch?;
            let versions = Self::u32s(&batch, "format_version")?;
            let ids = Self::strings(&batch, "compaction_id")?;
            let kinds = Self::strings(&batch, "archive_kind")?;
            let hours = Self::i64s(&batch, "partition_hour_millis")?;
            let manifests = Self::strings(&batch, "source_manifest_uri")?;
            let counts = Self::i64s(&batch, "source_count")?;
            let processed = Self::i64s(&batch, "processed_at_millis")?;
            let spec_ids = Self::i32s(&batch, "partition_spec_id")?;
            for index in 0..batch.num_rows() {
                let kind = ArchiveKind::from_segment(kinds.value(index))
                    .ok_or_else(|| Self::error("ledger row has an unknown archive kind"))?;
                let entry = LedgerEntry {
                    format_version: versions.value(index),
                    compaction_id: CompactionId::try_from(ids.value(index))?,
                    kind,
                    partition_hour_millis: hours.value(index),
                    source_manifest_uri: manifests.value(index).to_string(),
                    source_count: counts.value(index),
                    processed_at: TimestampMilliseconds(processed.value(index)),
                    partition_spec_id: spec_ids.value(index),
                };
                entry.validate()?;
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("format_version", DataType::UInt32, false),
                Field::new("compaction_id", DataType::Utf8, false),
                Field::new("archive_kind", DataType::Utf8, false),
                Field::new("partition_hour_millis", DataType::Int64, false),
                Field::new("source_manifest_uri", DataType::Utf8, false),
                Field::new("source_count", DataType::Int64, false),
                Field::new("processed_at_millis", DataType::Int64, false),
                Field::new("partition_spec_id", DataType::Int32, false),
            ],
            HashMap::from([(
                "ledger_format_version".to_string(),
                crate::LEDGER_FORMAT_VERSION.to_string(),
            )]),
        ))
    }

    fn strings<'a>(batch: &'a RecordBatch, name: &str) -> PlatformResult<&'a StringArray> {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| Self::error(&format!("ledger column {name} is missing")))
    }

    fn u32s<'a>(batch: &'a RecordBatch, name: &str) -> PlatformResult<&'a UInt32Array> {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| Self::error(&format!("ledger column {name} is missing")))
    }

    fn i64s<'a>(batch: &'a RecordBatch, name: &str) -> PlatformResult<&'a Int64Array> {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| Self::error(&format!("ledger column {name} is missing")))
    }

    fn i32s<'a>(batch: &'a RecordBatch, name: &str) -> PlatformResult<&'a Int32Array> {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
            .ok_or_else(|| Self::error(&format!("ledger column {name} is missing")))
    }

    fn error(message: &str) -> PlatformError {
        PlatformError::external(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message.to_string(),
        ))
    }
}
