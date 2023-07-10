#[derive(Debug, PartialEq, PartialOrd, Clone, Copy)]
#[repr(u8)]
pub enum PositionLockModifier {
    Nil = 0,
    Fee = 1,  // absolute value of revenue > transaction fee
    Loss = 2, // least percentage for the trade to close
}

// uses ROI
#[derive(Clone)]
pub enum PriceLevelModifier {
    StopLoss(f64), // 0 < f64 <= 75
    TakeProfit(f64),
}

impl PriceLevelModifier {
    pub fn is_stop_loss(&self) -> bool {
        match &self {
            PriceLevelModifier::StopLoss(_) => true,
            PriceLevelModifier::TakeProfit(_) => false,
        }
    }

    pub fn get_percentage(&self) -> f64 {
        match &self {
            PriceLevelModifier::StopLoss(stop_loss) => *stop_loss,
            PriceLevelModifier::TakeProfit(take_profit) => *take_profit,
        }
    }

    pub fn get_hash_key(&self) -> String {
        match &self {
            PriceLevelModifier::StopLoss(stop_loss) => "sl".to_string(),
            PriceLevelModifier::TakeProfit(take_profit) => "tp".to_string(),
        }
    }
}

#[derive(Clone)]
pub enum Leverage {
    Isolated(i32),
    Cross(i32),
}

impl Default for Leverage {
    fn default() -> Self {
        Self::Isolated(1)
    }
}

impl Leverage {
    pub fn get_factor(&self) -> f64 {
        match self {
            Leverage::Isolated(factor) => *factor as f64,
            Leverage::Cross(factor) => *factor as f64,
        }
    }
}
