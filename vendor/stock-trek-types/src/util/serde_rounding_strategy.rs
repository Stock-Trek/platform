use rust_decimal::RoundingStrategy;
use serde::{
    Deserialize, Deserializer, Serializer,
    de::{Error, Unexpected},
};

pub fn serialize<S: Serializer>(
    strategy: &RoundingStrategy,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    #[allow(deprecated)]
    let s = match strategy {
        RoundingStrategy::AwayFromZero => "AwayFromZero",
        RoundingStrategy::MidpointAwayFromZero => "MidpointAwayFromZero",
        RoundingStrategy::MidpointNearestEven => "MidpointNearestEven",
        RoundingStrategy::MidpointTowardZero => "MidpointTowardZero",
        RoundingStrategy::ToNegativeInfinity => "ToNegativeInfinity",
        RoundingStrategy::ToPositiveInfinity => "ToPositiveInfinity",
        RoundingStrategy::ToZero => "ToZero",
        RoundingStrategy::BankersRounding => "BankersRounding",
        RoundingStrategy::RoundDown => "RoundDown",
        RoundingStrategy::RoundHalfDown => "RoundHalfDown",
        RoundingStrategy::RoundHalfUp => "RoundHalfUp",
        RoundingStrategy::RoundUp => "RoundUp",
    };
    serializer.serialize_str(s)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<RoundingStrategy, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    #[allow(deprecated)]
    match s.as_str() {
        "AwayFromZero" => Ok(RoundingStrategy::AwayFromZero),
        "MidpointAwayFromZero" => Ok(RoundingStrategy::MidpointAwayFromZero),
        "MidpointNearestEven" => Ok(RoundingStrategy::MidpointNearestEven),
        "MidpointTowardZero" => Ok(RoundingStrategy::MidpointTowardZero),
        "ToNegativeInfinity" => Ok(RoundingStrategy::ToNegativeInfinity),
        "ToPositiveInfinity" => Ok(RoundingStrategy::ToPositiveInfinity),
        "ToZero" => Ok(RoundingStrategy::ToZero),
        "BankersRounding" => Ok(RoundingStrategy::BankersRounding),
        "RoundDown" => Ok(RoundingStrategy::RoundDown),
        "RoundHalfDown" => Ok(RoundingStrategy::RoundHalfDown),
        "RoundHalfUp" => Ok(RoundingStrategy::RoundHalfUp),
        "RoundUp" => Ok(RoundingStrategy::RoundUp),
        _ => Err(D::Error::invalid_value(
            Unexpected::Str(&s),
            &"a string representing a valid RoundingStrategy",
        )),
    }
}
