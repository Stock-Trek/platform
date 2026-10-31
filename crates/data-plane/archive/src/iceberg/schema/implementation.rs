use crate::{ArchiveBatch, ArchiveKind};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use platform_errors::{BpError, BpResult};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArchiveTableLayer {
    Raw,
    Compacted,
}

impl ArchiveTableLayer {
    pub fn label(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Compacted => "compacted",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArchiveIcebergTable {
    RawEvents,
    RawCandles,
    CompactedEvents,
    CompactedCandles,
}

impl ArchiveIcebergTable {
    pub fn new(layer: ArchiveTableLayer, kind: ArchiveKind) -> Self {
        match (layer, kind) {
            (ArchiveTableLayer::Raw, ArchiveKind::Events) => Self::RawEvents,
            (ArchiveTableLayer::Raw, ArchiveKind::Candles) => Self::RawCandles,
            (ArchiveTableLayer::Compacted, ArchiveKind::Events) => Self::CompactedEvents,
            (ArchiveTableLayer::Compacted, ArchiveKind::Candles) => Self::CompactedCandles,
        }
    }

    pub fn raw(kind: ArchiveKind) -> Self {
        Self::new(ArchiveTableLayer::Raw, kind)
    }

    pub fn compacted(kind: ArchiveKind) -> Self {
        Self::new(ArchiveTableLayer::Compacted, kind)
    }

    pub fn layer(self) -> ArchiveTableLayer {
        match self {
            Self::RawEvents | Self::RawCandles => ArchiveTableLayer::Raw,
            Self::CompactedEvents | Self::CompactedCandles => ArchiveTableLayer::Compacted,
        }
    }

    pub fn kind(self) -> ArchiveKind {
        match self {
            Self::RawEvents | Self::CompactedEvents => ArchiveKind::Events,
            Self::RawCandles | Self::CompactedCandles => ArchiveKind::Candles,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::RawEvents => "raw/events",
            Self::RawCandles => "raw/candles",
            Self::CompactedEvents => "compacted/events",
            Self::CompactedCandles => "compacted/candles",
        }
    }

    pub fn all() -> [Self; 4] {
        [
            Self::RawEvents,
            Self::RawCandles,
            Self::CompactedEvents,
            Self::CompactedCandles,
        ]
    }

    pub(crate) fn schema(kind: ArchiveKind) -> BpResult<Schema> {
        let arrow_schema = ArchiveBatch::schema(kind);
        let fields = arrow_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let id = i32::try_from(index + 1).map_err(|error| {
                    BpError::external(std::io::Error::other(format!(
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
            .collect::<BpResult<Vec<_>>>()?;

        Schema::builder()
            .with_schema_id(0)
            .with_fields(fields)
            .build()
            .map_err(BpError::external)
    }

    fn iceberg_type(name: &str, data_type: &arrow_schema::DataType) -> BpResult<Type> {
        use arrow_schema::DataType;

        match data_type {
            DataType::Utf8 | DataType::LargeUtf8 => Ok(Type::Primitive(PrimitiveType::String)),
            DataType::Int64 | DataType::UInt64 => Ok(Type::Primitive(PrimitiveType::Long)),
            DataType::Boolean => Ok(Type::Primitive(PrimitiveType::Boolean)),
            DataType::Float64 => Ok(Type::Primitive(PrimitiveType::Double)),
            other => Err(BpError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("archive column {name} has unsupported Iceberg type {other:?}"),
            ))),
        }
    }
}
