use serde::{Deserialize, Serialize};

// uses ROI
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PriceLevel {
    #[serde(rename="sl")]
    StopLoss(f64), // 0 < f64 <= 75 in bybit
    #[serde(rename="tp")]
    TakeProfit(f64),
    // #[serde(rename="tsp")]
    // TrailingStopLoss(TrailingStopLoss),
}

// TODO: implement this in the future
// #[derive(Serialize, Deserialize, Debug, Clone)]
// pub enum TrailingStopLoss {
//     #[serde(rename="pcnt")]
//     Percent(f64, f64),
//     #[serde(rename="step")]
//     Stepped(f64, f64),
// }

impl PriceLevel {
    pub fn get_percentage(&self) -> f64 {
        match &self {
            PriceLevel::StopLoss(percentage) => *percentage,
            PriceLevel::TakeProfit(percentage) => *percentage,
            // PriceLevel::TrailingStopLoss(trailing_stop_loss) => match trailing_stop_loss {
            //     TrailingStopLoss::Percent(percentage, _) => *percentage,
            //     TrailingStopLoss::Stepped(percentage, _) => *percentage,
            // },
        }
    }

    pub fn get_hash_key(&self) -> String {
        match &self {
            PriceLevel::StopLoss(_) => "sl".to_string(),
            PriceLevel::TakeProfit(_) => "tp".to_string(),
            // PriceLevel::TrailingStopLoss(_) => "tsp".to_string(),
        }
    }
}
