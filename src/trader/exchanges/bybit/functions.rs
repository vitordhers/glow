use super::enums::AdlRankIndicator;
use serde::Deserialize;
use serde::Deserializer;

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

pub fn deserialize_adl_rank_indicator<'de, D>(deserializer: D) -> Result<AdlRankIndicator, D::Error>
where
    D: Deserializer<'de>,
{
    let value: i32 = Deserialize::deserialize(deserializer)?;

    match value {
        0 => Ok(AdlRankIndicator::Empty),
        1 => Ok(AdlRankIndicator::First),
        2 => Ok(AdlRankIndicator::Second),
        3 => Ok(AdlRankIndicator::Third),
        4 => Ok(AdlRankIndicator::Fourth),
        5 => Ok(AdlRankIndicator::Fifth),
        _ => Err(serde::de::Error::custom(format!(
            "Invalid AdlRankIndicator value: {}",
            value
        ))),
    }
}
