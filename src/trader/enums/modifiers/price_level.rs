// uses ROI
#[derive(Debug, Clone)]
pub enum PriceLevel {
    StopLoss(f64), // 0 < f64 <= 75 in bybit
    TakeProfit(f64),
}

impl PriceLevel {
    pub fn get_percentage(&self) -> f64 {
        match &self {
            PriceLevel::StopLoss(stop_loss) => *stop_loss,
            PriceLevel::TakeProfit(take_profit) => *take_profit,
        }
    }

    pub fn get_hash_key(&self) -> String {
        match &self {
            PriceLevel::StopLoss(_) => "sl".to_string(),
            PriceLevel::TakeProfit(_) => "tp".to_string(),
        }
    }
}
