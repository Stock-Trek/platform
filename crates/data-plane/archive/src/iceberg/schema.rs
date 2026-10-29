use crate::{ArchiveBatch, ArchiveKind};
use platform_errors::{BpError, BpResult};
use serde_json::{Value, json};

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
}

pub(crate) fn schema(kind: ArchiveKind) -> BpResult<Value> {
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
            Ok(json!({
                "id": id,
                "name": field.name(),
                "required": !field.is_nullable(),
                "type": iceberg_type(field.name(), field.data_type())?,
            }))
        })
        .collect::<BpResult<Vec<_>>>()?;

    Ok(json!({
        "type": "struct",
        "schema-id": 0,
        "fields": fields,
    }))
}

fn iceberg_type(name: &str, data_type: &arrow_schema::DataType) -> BpResult<&'static str> {
    use arrow_schema::DataType;

    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => Ok("string"),
        DataType::Int64 | DataType::UInt64 => Ok("long"),
        DataType::Boolean => Ok("boolean"),
        DataType::Float64 => Ok("double"),
        other => Err(BpError::external(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("archive column {name} has unsupported Iceberg type {other:?}"),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tables_are_named_per_layer_and_kind() {
        assert_eq!(
            ArchiveIcebergTable::raw(ArchiveKind::Events).name(),
            "raw/events"
        );
        assert_eq!(
            ArchiveIcebergTable::raw(ArchiveKind::Candles).name(),
            "raw/candles"
        );
        assert_eq!(
            ArchiveIcebergTable::compacted(ArchiveKind::Events).name(),
            "compacted/events"
        );
        assert_eq!(
            ArchiveIcebergTable::compacted(ArchiveKind::Candles).name(),
            "compacted/candles"
        );
        assert_eq!(
            ArchiveIcebergTable::new(ArchiveTableLayer::Compacted, ArchiveKind::Candles).kind(),
            ArchiveKind::Candles
        );
    }

    #[test]
    fn event_schema_matches_the_archive_columns() {
        let schema = schema(ArchiveKind::Events).unwrap();
        let fields = schema["fields"].as_array().unwrap();

        assert_eq!(fields.len(), 6);
        assert_eq!(fields[0]["name"], "exchange");
        assert_eq!(fields[0]["type"], "string");
        assert_eq!(fields[2]["name"], "sequence");
        assert_eq!(fields[2]["type"], "long");
        assert_eq!(fields[2]["required"], false);
    }

    #[test]
    fn candle_schema_matches_the_archive_columns() {
        let schema = schema(ArchiveKind::Candles).unwrap();
        let fields = schema["fields"].as_array().unwrap();

        assert_eq!(fields.len(), 17);
        assert_eq!(fields[8]["name"], "open");
        assert_eq!(fields[8]["type"], "double");
        assert_eq!(fields[16]["name"], "revision");
    }
}
