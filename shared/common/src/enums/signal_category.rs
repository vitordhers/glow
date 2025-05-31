use super::side::Side;

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy, Default)]
pub enum SignalCategory {
    #[default]
    KeepPosition,
    GoShort,
    CloseShort,
    // RevertShort,
    GoLong,
    CloseLong,
    // RevertLong,
    ClosePosition,
    // RevertPosition,
    StopLoss,
    // TrailingStopLoss,
    TakeProfit,
    LeverageBankrupcty,
}

impl SignalCategory {
    pub fn get_column(&self) -> &str {
        match self {
            Self::GoShort => "short",
            Self::CloseShort => "short_close",
            // Self::RevertShort => "short_revert",
            Self::GoLong => "long",
            Self::CloseLong => "long_close",
            // Self::RevertLong => "long_revert",
            Self::KeepPosition => "position_keep",
            Self::ClosePosition => "position_close",
            // Self::RevertPosition => "position_revert",
            Self::StopLoss => "stop_loss",
            // Self::TrailingStopLoss => "trailing_stop_loss",
            Self::TakeProfit => "take_profit",
            Self::LeverageBankrupcty => "leverage_bankruptcy",
        }
    }
}

impl From<SignalCategory> for Side {
    fn from(value: SignalCategory) -> Self {
        match value {
            SignalCategory::GoShort => Side::Sell,
            SignalCategory::CloseShort => Side::Buy,
            SignalCategory::GoLong => Side::Buy,
            SignalCategory::CloseLong => Side::Sell,
            SignalCategory::KeepPosition => Side::None,
            SignalCategory::ClosePosition => unreachable!(),
            SignalCategory::StopLoss => unreachable!(),
            // SignalCategory::TrailingStopLoss => unreachable!(),
            SignalCategory::TakeProfit => unreachable!(),
            SignalCategory::LeverageBankrupcty => unreachable!(),
        }
    }
}
