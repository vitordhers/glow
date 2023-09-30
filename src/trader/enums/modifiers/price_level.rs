// uses ROI
#[derive(Debug, Clone)]
pub enum PriceLevel {
    StopLoss(f64), // 0 < f64 <= 75 in bybit
    TakeProfit(f64),
    TrailingStopLoss(TrailingStopLoss),
}

#[derive(Debug, Clone)]
pub enum TrailingStopLoss {
    Percent(f64, f64),
    Stepped(f64, f64),
}

impl PriceLevel {
    pub fn get_percentage(&self) -> f64 {
        match &self {
            PriceLevel::StopLoss(percentage) => *percentage,
            PriceLevel::TakeProfit(percentage) => *percentage,
            PriceLevel::TrailingStopLoss(trailing_stop_loss) => match trailing_stop_loss {
                TrailingStopLoss::Percent(percentage, _) => *percentage,
                TrailingStopLoss::Stepped(percentage, _) => *percentage,
            },
        }
    }

    pub fn get_hash_key(&self) -> String {
        match &self {
            PriceLevel::StopLoss(_) => "sl".to_string(),
            PriceLevel::TakeProfit(_) => "tp".to_string(),
            PriceLevel::TrailingStopLoss(_) => "tsp".to_string(),
        }
    }
}
