use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub(crate) mod milliseconds {
    use super::{DateTime, Deserialize, Deserializer, SecondsFormat, Serialize, Serializer, Utc};

    pub(crate) fn serialize<S>(value: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value
            .to_rfc3339_opts(SecondsFormat::Millis, true)
            .serialize(serializer)
    }

    #[allow(dead_code)]
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        DateTime::parse_from_rfc3339(&raw)
            .map(|parsed| parsed.with_timezone(&Utc))
            .map_err(serde::de::Error::custom)
    }

    pub(crate) mod option {
        use super::{DateTime, Deserialize, Deserializer, Serialize, Serializer, Utc};

        pub(crate) fn serialize<S>(
            value: &Option<DateTime<Utc>>,
            serializer: S,
        ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match value {
                Some(value) => value
                    .to_rfc3339_opts(super::SecondsFormat::Millis, true)
                    .serialize(serializer),
                None => serializer.serialize_none(),
            }
        }

        #[allow(dead_code)]
        pub(crate) fn deserialize<'de, D>(
            deserializer: D,
        ) -> Result<Option<DateTime<Utc>>, D::Error>
        where
            D: Deserializer<'de>,
        {
            let raw = Option::<String>::deserialize(deserializer)?;
            match raw {
                None => Ok(None),
                Some(raw) => DateTime::parse_from_rfc3339(&raw)
                    .map(|parsed| parsed.with_timezone(&Utc))
                    .map(Some)
                    .map_err(serde::de::Error::custom),
            }
        }
    }
}
