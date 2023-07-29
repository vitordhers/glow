pub fn parse_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: &str = serde::Deserialize::deserialize(deserializer)?;
    s.parse::<f64>().map_err(serde::de::Error::custom)
}

pub fn parse_f64_option<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: &str = serde::Deserialize::deserialize(deserializer)?;
    match s {
        "" | "0" | "0.0" => Ok(None),
        s => {
            let val = s.parse::<f64>().map_err(serde::de::Error::custom)?;
            if val == 0.0 {
                return Ok(None);
            }
            Ok(Some(val))
        }
    }
    // s.parse::<f64>().map_err(serde::de::Error::custom)
}

pub fn parse_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: &str = serde::Deserialize::deserialize(deserializer)?;
    s.parse::<i64>().map_err(serde::de::Error::custom)
}
