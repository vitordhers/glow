use chrono::{NaiveDateTime, Utc};
use serde::{Deserialize, Deserializer};

pub fn parse_u64_as_datetime<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let ts = i64::deserialize(deserializer)?;
    Ok(NaiveDateTime::from_timestamp_opt(ts / 1000, 0).unwrap_or(Utc::now().naive_utc()))
}

pub fn parse_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: &str = serde::Deserialize::deserialize(deserializer)?;
    s.parse::<f64>().map_err(serde::de::Error::custom)
}

pub fn parse_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: &str = serde::Deserialize::deserialize(deserializer)?;
    s.parse::<i64>().map_err(serde::de::Error::custom)
}

pub fn deserialize_boolean<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let value: i32 = Deserialize::deserialize(deserializer)?;

    if value == 0 {
        Ok(false)
    } else if value == 1 {
        Ok(true)
    } else {
        Err(serde::de::Error::custom("Invalid boolean value"))
    }
}
