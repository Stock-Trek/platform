use crate::{ArchiveBatch, ArchiveKind};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec};
use platform_errors::{PlatformError, PlatformResult};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArchiveTableLayer {
    Raw,
    Compacted,
    Ledger,
}

impl ArchiveTableLayer {
    pub fn label(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Compacted => "compacted",
            Self::Ledger => "ledger",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArchiveIcebergTable {
    RawEvents,
    RawCandles,
    CompactedEvents,
    CompactedCandles,
    LedgerEvents,
    LedgerCandles,
}

impl ArchiveIcebergTable {
    pub fn new(layer: ArchiveTableLayer, kind: ArchiveKind) -> Self {
        match (layer, kind) {
            (ArchiveTableLayer::Raw, ArchiveKind::Events) => Self::RawEvents,
            (ArchiveTableLayer::Raw, ArchiveKind::Candles) => Self::RawCandles,
            (ArchiveTableLayer::Compacted, ArchiveKind::Events) => Self::CompactedEvents,
            (ArchiveTableLayer::Compacted, ArchiveKind::Candles) => Self::CompactedCandles,
            (ArchiveTableLayer::Ledger, ArchiveKind::Events) => Self::LedgerEvents,
            (ArchiveTableLayer::Ledger, ArchiveKind::Candles) => Self::LedgerCandles,
        }
    }

    pub fn raw(kind: ArchiveKind) -> Self {
        Self::new(ArchiveTableLayer::Raw, kind)
    }

    pub fn compacted(kind: ArchiveKind) -> Self {
        Self::new(ArchiveTableLayer::Compacted, kind)
    }

    pub fn ledger(kind: ArchiveKind) -> Self {
        Self::new(ArchiveTableLayer::Ledger, kind)
    }

    pub fn layer(self) -> ArchiveTableLayer {
        match self {
            Self::RawEvents | Self::RawCandles => ArchiveTableLayer::Raw,
            Self::CompactedEvents | Self::CompactedCandles => ArchiveTableLayer::Compacted,
            Self::LedgerEvents | Self::LedgerCandles => ArchiveTableLayer::Ledger,
        }
    }

    pub fn kind(self) -> ArchiveKind {
        match self {
            Self::RawEvents | Self::CompactedEvents | Self::LedgerEvents => ArchiveKind::Events,
            Self::RawCandles | Self::CompactedCandles | Self::LedgerCandles => ArchiveKind::Candles,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::RawEvents => "raw/events",
            Self::RawCandles => "raw/candles",
            Self::CompactedEvents => "compacted/events",
            Self::CompactedCandles => "compacted/candles",
            Self::LedgerEvents => "ledger/events",
            Self::LedgerCandles => "ledger/candles",
        }
    }

    pub fn all() -> [Self; 6] {
        [
            Self::RawEvents,
            Self::RawCandles,
            Self::CompactedEvents,
            Self::CompactedCandles,
            Self::LedgerEvents,
            Self::LedgerCandles,
        ]
    }

    pub fn is_ledger(self) -> bool {
        self.layer() == ArchiveTableLayer::Ledger
    }

    pub fn table_schema(self) -> PlatformResult<Schema> {
        if self.is_ledger() {
            Self::ledger_schema()
        } else {
            Self::schema(self.kind())
        }
    }

    pub fn partition_spec(self) -> PlatformResult<Option<UnboundPartitionSpec>> {
        if !self.is_ledger() {
            return Ok(None);
        }
        let builder = UnboundPartitionSpec::builder()
            .add_partition_field(3, "archive_kind", Transform::Identity)
            .and_then(|builder| {
                builder.add_partition_field(4, "partition_hour_millis", Transform::Identity)
            })
            .map_err(|error| {
                PlatformError::external(std::io::Error::other(format!(
                    "build ledger partition spec: {error}"
                )))
            })?;
        let spec = builder
            .build()
            .bind(Self::ledger_schema()?)
            .map_err(|error| {
                PlatformError::external(std::io::Error::other(format!(
                    "bind ledger partition spec: {error}"
                )))
            })?;
        Ok(Some(spec.into_unbound()))
    }

    pub fn ledger_schema() -> PlatformResult<Schema> {
        Self::build_schema(vec![
            Self::field(1, "format_version", PrimitiveType::Int, false),
            Self::field(2, "compaction_id", PrimitiveType::String, false),
            Self::field(3, "archive_kind", PrimitiveType::String, false),
            Self::field(4, "partition_hour_millis", PrimitiveType::Long, false),
            Self::field(5, "source_manifest_uri", PrimitiveType::String, false),
            Self::field(6, "source_count", PrimitiveType::Long, false),
            Self::field(7, "processed_at_millis", PrimitiveType::Long, false),
            Self::field(8, "partition_spec_id", PrimitiveType::Int, false),
        ])
    }

    fn field(id: i32, name: &str, primitive: PrimitiveType, required: bool) -> NestedField {
        NestedField::new(id, name, Type::Primitive(primitive), required)
    }

    fn build_schema(fields: Vec<NestedField>) -> PlatformResult<Schema> {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(fields.into_iter().map(Arc::new))
            .build()
            .map_err(PlatformError::external)
    }

    pub fn schema(kind: ArchiveKind) -> PlatformResult<Schema> {
        let arrow_schema = ArchiveBatch::schema(kind);
        let fields = arrow_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let id = i32::try_from(index + 1).map_err(|error| {
                    PlatformError::external(std::io::Error::other(format!(
                        "archive schema has too many fields: {error}"
                    )))
                })?;
                Ok(Arc::new(NestedField::new(
                    id,
                    field.name(),
                    Self::iceberg_type(field.name(), field.data_type())?,
                    !field.is_nullable(),
                )))
            })
            .collect::<PlatformResult<Vec<_>>>()?;

        Self::build_schema(
            fields
                .into_iter()
                .map(|field| field.as_ref().clone())
                .collect(),
        )
    }

    fn iceberg_type(name: &str, data_type: &arrow_schema::DataType) -> PlatformResult<Type> {
        use arrow_schema::DataType;

        match data_type {
            DataType::Utf8 | DataType::LargeUtf8 => Ok(Type::Primitive(PrimitiveType::String)),
            DataType::Int64 | DataType::UInt64 => Ok(Type::Primitive(PrimitiveType::Long)),
            DataType::Int32 | DataType::UInt32 => Ok(Type::Primitive(PrimitiveType::Int)),
            DataType::Boolean => Ok(Type::Primitive(PrimitiveType::Boolean)),
            DataType::Float64 => Ok(Type::Primitive(PrimitiveType::Double)),
            other => Err(PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("archive column {name} has unsupported Iceberg type {other:?}"),
            ))),
        }
    }
}
