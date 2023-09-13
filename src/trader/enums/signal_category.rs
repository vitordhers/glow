#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SignalCategory {
    GoShort,
    CloseShort,
    RevertShort,
    GoLong,
    CloseLong,
    RevertLong,
    KeepPosition,
    ClosePosition,
    RevertPosition,
    StopLoss,
    TakeProfit,
    LeverageBankrupcty,
}

impl Default for SignalCategory {
    fn default() -> Self {
        SignalCategory::KeepPosition
    }
}

impl SignalCategory {
    pub fn get_column(&self) -> &str {
        match self {
            Self::GoShort => "short",
            Self::CloseShort => "short_close",
            Self::RevertShort => "short_revert",
            Self::GoLong => "long",
            Self::CloseLong => "long_close",
            Self::RevertLong => "long_revert",
            Self::KeepPosition => "position_keep",
            Self::ClosePosition => "position_close",
            Self::RevertPosition => "position_revert",
            Self::StopLoss => "stop_loss",
            Self::TakeProfit => "take_profit",
            Self::LeverageBankrupcty => "leverage_bankruptcy",
        }
    }
}
