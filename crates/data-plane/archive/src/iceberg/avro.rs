use platform_errors::{BpError, BpResult};
use serde_json::Value;
use std::{collections::HashMap, fmt::Display};
use uuid::Uuid;

const MAGIC: &[u8; 4] = b"Obj\x01";
const SYNC_MARKER_LEN: usize = 16;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AvroSchema {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Record {
        name: String,
        fields: Vec<AvroField>,
    },
    Array(Box<AvroSchema>),
    Map(Box<AvroSchema>),
    Union(Vec<AvroSchema>),
    Fixed {
        name: String,
        size: usize,
    },
    Enum {
        name: String,
        symbols: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AvroField {
    pub(crate) name: String,
    pub(crate) schema: AvroSchema,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AvroValue {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
    String(String),
    Array(Vec<AvroValue>),
    Map(Vec<(String, AvroValue)>),
    Record(Vec<AvroValue>),
    Fixed(Vec<u8>),
    Enum(String),
}

impl AvroSchema {
    fn parse(value: &Value) -> BpResult<Self> {
        Self::parse_named(value, &mut HashMap::new())
    }

    fn parse_named(value: &Value, names: &mut HashMap<String, Self>) -> BpResult<Self> {
        match value {
            Value::String(name) => Self::parse_primitive(name)
                .or_else(|| names.get(name).cloned())
                .ok_or_else(|| Self::invalid(format!("unknown Avro type {name}"))),
            Value::Array(branches) => Ok(Self::Union(
                branches
                    .iter()
                    .map(|branch| Self::parse_named(branch, names))
                    .collect::<BpResult<Vec<_>>>()?,
            )),
            Value::Object(object) => {
                let type_name = object
                    .get("type")
                    .ok_or_else(|| Self::invalid("Avro schema object has no type"))?;
                match type_name {
                    Value::Array(_) => Self::parse_named(type_name, names),
                    Value::String(name) => match name.as_str() {
                        "record" => {
                            let name = object
                                .get("name")
                                .and_then(Value::as_str)
                                .ok_or_else(|| Self::invalid("Avro record has no name"))?
                                .to_string();
                            let fields_value = object
                                .get("fields")
                                .and_then(Value::as_array)
                                .ok_or_else(|| Self::invalid("Avro record has no fields"))?;
                            let fields = fields_value
                                .iter()
                                .map(|field| {
                                    let name = field
                                        .get("name")
                                        .and_then(Value::as_str)
                                        .ok_or_else(|| Self::invalid("Avro field has no name"))?
                                        .to_string();
                                    let schema = field
                                        .get("type")
                                        .ok_or_else(|| Self::invalid("Avro field has no type"))?;
                                    Ok(AvroField {
                                        name,
                                        schema: Self::parse_named(schema, names)?,
                                    })
                                })
                                .collect::<BpResult<Vec<_>>>()?;
                            let schema = Self::Record {
                                name: name.clone(),
                                fields,
                            };
                            names.insert(name, schema.clone());
                            Ok(schema)
                        }
                        "array" => Ok(Self::Array(Box::new(Self::parse_named(
                            object
                                .get("items")
                                .ok_or_else(|| Self::invalid("Avro array has no items"))?,
                            names,
                        )?))),
                        "map" => Ok(Self::Map(Box::new(Self::parse_named(
                            object
                                .get("values")
                                .ok_or_else(|| Self::invalid("Avro map has no values"))?,
                            names,
                        )?))),
                        "fixed" => {
                            let name = object
                                .get("name")
                                .and_then(Value::as_str)
                                .ok_or_else(|| Self::invalid("Avro fixed type has no name"))?
                                .to_string();
                            let size = object
                                .get("size")
                                .and_then(Value::as_u64)
                                .ok_or_else(|| Self::invalid("Avro fixed type has no size"))?
                                as usize;
                            Ok(Self::Fixed { name, size })
                        }
                        "enum" => {
                            let name = object
                                .get("name")
                                .and_then(Value::as_str)
                                .ok_or_else(|| Self::invalid("Avro enum has no name"))?
                                .to_string();
                            let symbols = object
                                .get("symbols")
                                .and_then(Value::as_array)
                                .ok_or_else(|| Self::invalid("Avro enum has no symbols"))?
                                .iter()
                                .map(|symbol| {
                                    symbol.as_str().map(str::to_string).ok_or_else(|| {
                                        Self::invalid("Avro enum symbol is not a string")
                                    })
                                })
                                .collect::<BpResult<Vec<_>>>()?;
                            Ok(Self::Enum { name, symbols })
                        }
                        primitive => Self::parse_primitive(primitive)
                            .ok_or_else(|| Self::invalid(format!("unknown Avro type {primitive}"))),
                    },
                    _ => Err(Self::invalid("Avro schema type is not a string or array")),
                }
            }
            _ => Err(Self::invalid(
                "Avro schema is not a string, array, or object",
            )),
        }
    }

    fn parse_primitive(name: &str) -> Option<Self> {
        Some(match name {
            "null" => Self::Null,
            "boolean" => Self::Boolean,
            "int" => Self::Int,
            "long" => Self::Long,
            "float" => Self::Float,
            "double" => Self::Double,
            "bytes" => Self::Bytes,
            "string" => Self::String,
            _ => return None,
        })
    }

    fn accepts(&self, value: &AvroValue) -> bool {
        matches!(
            (self, value),
            (Self::Null, AvroValue::Null)
                | (Self::Boolean, AvroValue::Boolean(_))
                | (Self::Int, AvroValue::Int(_))
                | (Self::Long, AvroValue::Long(_))
                | (Self::Float, AvroValue::Float(_))
                | (Self::Double, AvroValue::Double(_))
                | (Self::Bytes, AvroValue::Bytes(_))
                | (Self::String, AvroValue::String(_))
                | (Self::Array(_), AvroValue::Array(_))
                | (Self::Map(_), AvroValue::Map(_))
                | (Self::Record { .. }, AvroValue::Record(_))
                | (Self::Fixed { .. }, AvroValue::Fixed(_))
                | (Self::Enum { .. }, AvroValue::Enum(_))
        )
    }

    fn invalid(message: impl Display) -> BpError {
        invalid("parse Avro schema", message)
    }
}

pub(crate) fn encode(schema_json: &str, records: &[AvroValue]) -> BpResult<Vec<u8>> {
    let schema_value: Value = serde_json::from_str(schema_json)
        .map_err(|error| invalid("parse Avro schema JSON", error))?;
    let schema = AvroSchema::parse(&schema_value)?;

    let mut output = Vec::new();
    output.extend_from_slice(MAGIC);

    let mut metadata = Vec::new();
    write_long(&mut metadata, 2);
    write_string(&mut metadata, "avro.schema")?;
    write_bytes(&mut metadata, schema_json.as_bytes())?;
    write_string(&mut metadata, "avro.codec")?;
    write_bytes(&mut metadata, b"null")?;
    write_long(&mut metadata, 0);
    output.extend_from_slice(&metadata);

    let sync_marker = *Uuid::new_v4().as_bytes();
    output.extend_from_slice(&sync_marker);

    if !records.is_empty() {
        let mut body = Vec::new();
        for record in records {
            write_value(&mut body, &schema, record)?;
        }
        write_long(&mut output, records.len() as i64);
        write_long(&mut output, body.len() as i64);
        output.extend_from_slice(&body);
        output.extend_from_slice(&sync_marker);
    }

    Ok(output)
}

pub(crate) fn decode(bytes: &[u8]) -> BpResult<(AvroSchema, Vec<AvroValue>)> {
    let mut input = Input::new(bytes);
    if input.take(MAGIC.len())? != MAGIC {
        return Err(invalid("read Avro file", "invalid object container magic"));
    }

    let mut schema_json = None;
    let mut codec = String::from("null");
    loop {
        let mut count = input.read_long()?;
        if count == 0 {
            break;
        }
        if count < 0 {
            count = -count;
            let _block_size = input.read_long()?;
        }
        for _ in 0..count {
            let key = input.read_string()?;
            let value = input.read_bytes()?;
            match key.as_str() {
                "avro.schema" => {
                    schema_json = Some(
                        String::from_utf8(value)
                            .map_err(|error| invalid("read Avro metadata", error))?,
                    );
                }
                "avro.codec" => {
                    codec = String::from_utf8(value)
                        .map_err(|error| invalid("read Avro metadata", error))?;
                }
                _ => {}
            }
        }
    }

    if codec != "null" {
        return Err(invalid(
            "read Avro file",
            format!("unsupported Avro codec {codec}"),
        ));
    }

    let schema_json =
        schema_json.ok_or_else(|| invalid("read Avro file", "missing avro.schema"))?;
    let schema_value: Value = serde_json::from_str(&schema_json)
        .map_err(|error| invalid("parse Avro schema JSON", error))?;
    let schema = AvroSchema::parse(&schema_value)?;

    let sync_marker = input.take(SYNC_MARKER_LEN)?.to_vec();
    let mut records = Vec::new();
    while input.remaining() > 0 {
        let mut count = input.read_long()?;
        if count < 0 {
            count = -count;
            let _block_size = input.read_long()?;
        }
        let block_size = input.read_long()?;
        let block = input.take(usize::try_from(block_size).map_err(|error| {
            invalid("read Avro block", format!("invalid block size: {error}"))
        })?)?;
        let mut block_input = Input::new(block);
        for _ in 0..count {
            records.push(read_value(&mut block_input, &schema)?);
        }
        if block_input.remaining() != 0 {
            return Err(invalid("read Avro block", "unconsumed record bytes"));
        }
        let block_sync = input.take(SYNC_MARKER_LEN)?;
        if block_sync != sync_marker {
            return Err(invalid("read Avro block", "sync marker mismatch"));
        }
    }

    Ok((schema, records))
}

fn write_value(output: &mut Vec<u8>, schema: &AvroSchema, value: &AvroValue) -> BpResult<()> {
    match (schema, value) {
        (AvroSchema::Null, AvroValue::Null) => {}
        (AvroSchema::Boolean, AvroValue::Boolean(value)) => output.push(u8::from(*value)),
        (AvroSchema::Int, AvroValue::Int(value)) => write_long(output, i64::from(*value)),
        (AvroSchema::Long, AvroValue::Long(value)) => write_long(output, *value),
        (AvroSchema::Float, AvroValue::Float(value)) => {
            output.extend_from_slice(&value.to_le_bytes());
        }
        (AvroSchema::Double, AvroValue::Double(value)) => {
            output.extend_from_slice(&value.to_le_bytes());
        }
        (AvroSchema::Bytes, AvroValue::Bytes(value)) => write_bytes(output, value)?,
        (AvroSchema::String, AvroValue::String(value)) => write_string(output, value)?,
        (AvroSchema::Array(items), AvroValue::Array(values)) => {
            if !values.is_empty() {
                write_long(output, values.len() as i64);
                for value in values {
                    write_value(output, items, value)?;
                }
            }
            write_long(output, 0);
        }
        (AvroSchema::Map(values_schema), AvroValue::Map(values)) => {
            if !values.is_empty() {
                write_long(output, values.len() as i64);
                for (key, value) in values {
                    write_string(output, key)?;
                    write_value(output, values_schema, value)?;
                }
            }
            write_long(output, 0);
        }
        (AvroSchema::Record { fields, .. }, AvroValue::Record(values)) => {
            if fields.len() != values.len() {
                return Err(invalid(
                    "encode Avro record",
                    format!(
                        "expected {} values for record, got {}",
                        fields.len(),
                        values.len()
                    ),
                ));
            }
            for (field, value) in fields.iter().zip(values) {
                write_value(output, &field.schema, value).map_err(|error| {
                    invalid(
                        "encode Avro record",
                        format!("field {}: {error}", field.name),
                    )
                })?;
            }
        }
        (AvroSchema::Fixed { size, .. }, AvroValue::Fixed(value)) => {
            if value.len() != *size {
                return Err(invalid(
                    "encode Avro fixed",
                    format!("expected {size} bytes, got {}", value.len()),
                ));
            }
            output.extend_from_slice(value);
        }
        (AvroSchema::Enum { symbols, .. }, AvroValue::Enum(value)) => {
            let index = symbols
                .iter()
                .position(|symbol| symbol == value)
                .ok_or_else(|| invalid("encode Avro enum", format!("unknown symbol {value}")))?;
            write_long(output, index as i64);
        }
        (AvroSchema::Union(branches), value) => {
            let index = branches
                .iter()
                .position(|branch| branch.accepts(value))
                .ok_or_else(|| {
                    invalid("encode Avro union", format!("no branch accepts {value:?}"))
                })?;
            write_long(output, index as i64);
            write_value(output, &branches[index], value)?;
        }
        _ => {
            return Err(invalid(
                "encode Avro value",
                format!("value {value:?} does not match schema {schema:?}"),
            ));
        }
    }
    Ok(())
}

fn read_value(input: &mut Input<'_>, schema: &AvroSchema) -> BpResult<AvroValue> {
    Ok(match schema {
        AvroSchema::Null => AvroValue::Null,
        AvroSchema::Boolean => AvroValue::Boolean(input.take(1)?[0] != 0),
        AvroSchema::Int => AvroValue::Int(
            i32::try_from(input.read_long()?).map_err(|error| invalid("decode Avro int", error))?,
        ),
        AvroSchema::Long => AvroValue::Long(input.read_long()?),
        AvroSchema::Float => {
            let bytes: [u8; 4] = input.take(4)?.try_into().expect("four bytes");
            AvroValue::Float(f32::from_le_bytes(bytes))
        }
        AvroSchema::Double => {
            let bytes: [u8; 8] = input.take(8)?.try_into().expect("eight bytes");
            AvroValue::Double(f64::from_le_bytes(bytes))
        }
        AvroSchema::Bytes => AvroValue::Bytes(input.read_bytes()?),
        AvroSchema::String => AvroValue::String(input.read_string()?),
        AvroSchema::Array(items) => {
            let mut values = Vec::new();
            loop {
                let mut count = input.read_long()?;
                if count == 0 {
                    break;
                }
                if count < 0 {
                    count = -count;
                    let _block_size = input.read_long()?;
                }
                for _ in 0..count {
                    values.push(read_value(input, items)?);
                }
            }
            AvroValue::Array(values)
        }
        AvroSchema::Map(values_schema) => {
            let mut values = Vec::new();
            loop {
                let mut count = input.read_long()?;
                if count == 0 {
                    break;
                }
                if count < 0 {
                    count = -count;
                    let _block_size = input.read_long()?;
                }
                for _ in 0..count {
                    let key = input.read_string()?;
                    values.push((key, read_value(input, values_schema)?));
                }
            }
            AvroValue::Map(values)
        }
        AvroSchema::Record { fields, .. } => {
            let mut values = Vec::with_capacity(fields.len());
            for field in fields {
                values.push(read_value(input, &field.schema).map_err(|error| {
                    invalid(
                        "decode Avro record",
                        format!("field {}: {error}", field.name),
                    )
                })?);
            }
            AvroValue::Record(values)
        }
        AvroSchema::Fixed { size, .. } => AvroValue::Fixed(input.take(*size)?.to_vec()),
        AvroSchema::Enum { symbols, .. } => {
            let index = usize::try_from(input.read_long()?)
                .map_err(|error| invalid("decode Avro enum", error))?;
            let symbol = symbols
                .get(index)
                .ok_or_else(|| invalid("decode Avro enum", format!("unknown index {index}")))?;
            AvroValue::Enum(symbol.clone())
        }
        AvroSchema::Union(branches) => {
            let index = usize::try_from(input.read_long()?)
                .map_err(|error| invalid("decode Avro union", error))?;
            let branch = branches
                .get(index)
                .ok_or_else(|| invalid("decode Avro union", format!("unknown index {index}")))?;
            read_value(input, branch)?
        }
    })
}

fn write_long(output: &mut Vec<u8>, value: i64) {
    let mut encoded = ((value as u64) << 1) ^ ((value >> 63) as u64);
    while encoded & !0x7f != 0 {
        output.push(((encoded & 0x7f) as u8) | 0x80);
        encoded >>= 7;
    }
    output.push(encoded as u8);
}

fn write_bytes(output: &mut Vec<u8>, value: &[u8]) -> BpResult<()> {
    write_long(
        output,
        i64::try_from(value.len()).map_err(|error| invalid("encode Avro bytes", error))?,
    );
    output.extend_from_slice(value);
    Ok(())
}

fn write_string(output: &mut Vec<u8>, value: &str) -> BpResult<()> {
    write_bytes(output, value.as_bytes())
}

struct Input<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Input<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn take(&mut self, length: usize) -> BpResult<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| invalid("read Avro data", "offset overflow"))?;
        if end > self.bytes.len() {
            return Err(invalid("read Avro data", "unexpected end of file"));
        }
        let value = &self.bytes[self.position..end];
        self.position = end;
        Ok(value)
    }

    fn read_long(&mut self) -> BpResult<i64> {
        let mut value = 0_u64;
        let mut shift = 0_u32;
        loop {
            let byte = self.take(1)?[0];
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift >= 64 {
                return Err(invalid("read Avro long", "varint is too long"));
            }
        }
        Ok(((value >> 1) as i64) ^ -((value & 1) as i64))
    }

    fn read_bytes(&mut self) -> BpResult<Vec<u8>> {
        let length = usize::try_from(self.read_long()?)
            .map_err(|error| invalid("read Avro bytes", error))?;
        Ok(self.take(length)?.to_vec())
    }

    fn read_string(&mut self) -> BpResult<String> {
        String::from_utf8(self.read_bytes()?).map_err(|error| invalid("read Avro string", error))
    }
}

fn invalid(operation: &'static str, message: impl Display) -> BpError {
    BpError::io(
        operation,
        "Avro object container",
        std::io::Error::new(std::io::ErrorKind::InvalidData, message.to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn object_container_round_trips_records() {
        let schema = json!({
            "type": "record",
            "name": "entry",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "count", "type": "long"},
                {"name": "flag", "type": ["null", "boolean"], "default": null}
            ]
        })
        .to_string();
        let records = vec![
            AvroValue::Record(vec![
                AvroValue::String("first".to_string()),
                AvroValue::Long(7),
                AvroValue::Boolean(true),
            ]),
            AvroValue::Record(vec![
                AvroValue::String("second".to_string()),
                AvroValue::Long(-3),
                AvroValue::Null,
            ]),
        ];

        let encoded = encode(&schema, &records).unwrap();
        let (decoded_schema, decoded) = decode(&encoded).unwrap();

        assert!(matches!(decoded_schema, AvroSchema::Record { .. }));
        assert_eq!(decoded, records);
    }

    #[test]
    fn empty_object_container_decodes_without_records() {
        let schema = json!({
            "type": "record",
            "name": "entry",
            "fields": [{"name": "name", "type": "string"}]
        })
        .to_string();

        let encoded = encode(&schema, &[]).unwrap();
        let (_, decoded) = decode(&encoded).unwrap();

        assert!(decoded.is_empty());
    }

    #[test]
    fn varints_round_trip_boundaries() {
        for value in [0, 1, -1, 63, -64, i64::MAX, i64::MIN] {
            let mut output = Vec::new();
            write_long(&mut output, value);
            let mut input = Input::new(&output);
            assert_eq!(input.read_long().unwrap(), value);
        }
    }
}
