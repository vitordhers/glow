use super::super::errors::Error;
use polars::prelude::*;

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Hash)]
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
    LeverageBankrupcty
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

pub trait Signer {
    fn signal_category(&self) -> SignalCategory;
    fn compute_signal_column(&self, lf: &LazyFrame) -> Result<LazyFrame, Error>;
    fn clone_box(&self) -> Box<dyn Signer + Send + Sync>;
}

impl Clone for Box<dyn Signer + Send + Sync> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
