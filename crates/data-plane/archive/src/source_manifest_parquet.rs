use crate::{CompactionId, SourceManifest};
use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
};
use platform_errors::{PlatformError, PlatformResult};
use std::{collections::HashMap, sync::Arc};

pub struct SourceManifestParquet;

impl SourceManifestParquet {
    pub fn encode(manifest: &SourceManifest) -> PlatformResult<Bytes> {
        manifest.validate()?;
        let schema = Self::schema(
            manifest.compaction_id,
            manifest.format_version,
            manifest.partition_spec_id,
        );
        let uris = Arc::new(StringArray::from_iter_values(
            manifest.sources.iter().map(String::as_str),
        )) as ArrayRef;
        let ids = Arc::new(StringArray::from_iter_values(
            manifest
                .sources
                .iter()
                .map(|_| manifest.compaction_id.as_str()),
        )) as ArrayRef;
        let versions = Arc::new(UInt32Array::from_iter_values(
            manifest.sources.iter().map(|_| manifest.format_version),
        )) as ArrayRef;
        let spec_ids = Arc::new(Int32Array::from_iter_values(
            manifest.sources.iter().map(|_| manifest.partition_spec_id),
        )) as ArrayRef;
        let batch = RecordBatch::try_new(schema.clone(), vec![uris, ids, versions, spec_ids])?;
        let properties = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![
                parquet::file::metadata::KeyValue::new(
                    "compaction_id".to_string(),
                    manifest.compaction_id.as_str(),
                ),
                parquet::file::metadata::KeyValue::new(
                    "format_version".to_string(),
                    manifest.format_version.to_string(),
                ),
                parquet::file::metadata::KeyValue::new(
                    "partition_spec_id".to_string(),
                    manifest.partition_spec_id.to_string(),
                ),
            ]))
            .build();
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(properties))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(Bytes::from(buffer))
    }

    pub fn decode(bytes: Bytes) -> PlatformResult<SourceManifest> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
        let metadata_id = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .and_then(|entries| {
                entries
                    .iter()
                    .find(|entry| entry.key == "compaction_id")
                    .and_then(|entry| entry.value.clone())
            });
        let reader = builder.build()?;
        let mut sources = Vec::new();
        let mut compaction_id = None;
        let mut format_version = None;
        let mut partition_spec_id = None;
        for batch in reader {
            let batch = batch?;
            let uris = Self::strings(&batch, "uri")?;
            let ids = Self::strings(&batch, "compaction_id")?;
            let versions = Self::u32s(&batch, "format_version")?;
            let spec_ids = Self::i32s(&batch, "partition_spec_id")?;
            for index in 0..batch.num_rows() {
                let id = CompactionId::try_from(ids.value(index))?;
                if compaction_id
                    .replace(id)
                    .is_some_and(|existing| existing != id)
                {
                    return Err(Self::error(
                        "source manifest contains more than one compaction id",
                    ));
                }
                let version = versions.value(index);
                if format_version
                    .replace(version)
                    .is_some_and(|existing| existing != version)
                {
                    return Err(Self::error(
                        "source manifest contains more than one format version",
                    ));
                }
                let spec_id = spec_ids.value(index);
                if partition_spec_id
                    .replace(spec_id)
                    .is_some_and(|existing| existing != spec_id)
                {
                    return Err(Self::error(
                        "source manifest contains more than one partition spec id",
                    ));
                }
                sources.push(uris.value(index).to_string());
            }
        }
        let compaction_id = compaction_id.ok_or_else(|| Self::error("source manifest is empty"))?;
        if metadata_id.is_some_and(|value| value != compaction_id.as_str()) {
            return Err(Self::error(
                "source manifest metadata does not match its rows",
            ));
        }
        let partition_spec_id =
            partition_spec_id.ok_or_else(|| Self::error("source manifest is empty"))?;
        SourceManifest::new(compaction_id, partition_spec_id, sources).and_then(|manifest| {
            let expected = format_version.unwrap_or(manifest.format_version);
            if expected != manifest.format_version {
                return Err(Self::error("source manifest row format is not supported"));
            }
            Ok(manifest)
        })
    }

    fn schema(
        compaction_id: CompactionId,
        format_version: u32,
        partition_spec_id: i32,
    ) -> Arc<Schema> {
        let metadata = HashMap::from([
            (
                "compaction_id".to_string(),
                compaction_id.as_str().to_string(),
            ),
            ("format_version".to_string(), format_version.to_string()),
            (
                "partition_spec_id".to_string(),
                partition_spec_id.to_string(),
            ),
        ]);
        Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("uri", DataType::Utf8, false),
                Field::new("compaction_id", DataType::Utf8, false),
                Field::new("format_version", DataType::UInt32, false),
                Field::new("partition_spec_id", DataType::Int32, false),
            ],
            metadata,
        ))
    }

    fn strings<'a>(batch: &'a RecordBatch, name: &str) -> PlatformResult<&'a StringArray> {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| Self::error(&format!("source manifest column {name} is missing")))
    }

    fn u32s<'a>(batch: &'a RecordBatch, name: &str) -> PlatformResult<&'a UInt32Array> {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| Self::error(&format!("source manifest column {name} is missing")))
    }

    fn i32s<'a>(batch: &'a RecordBatch, name: &str) -> PlatformResult<&'a Int32Array> {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
            .ok_or_else(|| Self::error(&format!("source manifest column {name} is missing")))
    }

    fn error(message: &str) -> PlatformError {
        PlatformError::external(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message.to_string(),
        ))
    }
}

impl SourceManifestParquet {
    pub fn row_count(bytes: &Bytes) -> PlatformResult<usize> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?;
        Ok(usize::try_from(builder.metadata().file_metadata().num_rows()).unwrap_or_default())
    }
}
