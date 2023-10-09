mod functions;
use polars::prelude::*;

use super::{
    errors::Error,
    functions::{
        current_timestamp_ms, get_symbol_ohlc_cols, get_symbol_window_ohlc_cols,
        round_down_nth_decimal, timestamp_minute_start,
    },
    traits::indicator::Indicator,
};

pub mod ema_trend;
pub mod stc;
pub mod stochastic;
pub mod stochastic_threshold;
pub mod tsi;

use ema_trend::ExponentialMovingAverageTrendIndicator;
use stc::STCIndicator;
use stochastic::StochasticIndicator;
use stochastic_threshold::StochasticThresholdIndicator;
use tsi::TSIIndicator;

#[derive(Clone)]
pub enum IndicatorWrapper {
    ExponentialMovingAverageTrendIndicator(ExponentialMovingAverageTrendIndicator),
    StochasticIndicator(StochasticIndicator),
    SchaffTrendCycleIndicator(STCIndicator),
    StochasticThresholdIndicator(StochasticThresholdIndicator),
    TrueStrengthIndexIndicator(TSIIndicator),
}

impl Indicator for IndicatorWrapper {
    fn name(&self) -> String {
        match self {
            Self::ExponentialMovingAverageTrendIndicator(ema_indicator) => ema_indicator.name(),
            Self::StochasticIndicator(stochastic_indicator) => stochastic_indicator.name(),
            Self::SchaffTrendCycleIndicator(stc_indicator) => stc_indicator.name(),
            Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
                stochastic_threshold_indicator.name()
            }
            Self::TrueStrengthIndexIndicator(tsi_indicator) => tsi_indicator.name(),
        }
    }
    fn get_indicator_columns(&self) -> Vec<(String, DataType)> {
        match self {
            Self::ExponentialMovingAverageTrendIndicator(ema_indicator) => {
                ema_indicator.get_indicator_columns()
            }
            Self::StochasticIndicator(stochastic_indicator) => {
                stochastic_indicator.get_indicator_columns()
            }
            Self::SchaffTrendCycleIndicator(stc_indicator) => stc_indicator.get_indicator_columns(),
            Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
                stochastic_threshold_indicator.get_indicator_columns()
            }
            Self::TrueStrengthIndexIndicator(tsi_indicator) => {
                tsi_indicator.get_indicator_columns()
            }
        }
    }

    fn set_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, Error> {
        match self {
            Self::ExponentialMovingAverageTrendIndicator(ema_indicator) => {
                ema_indicator.set_indicator_columns(lf)
            }
            Self::StochasticIndicator(stochastic_indicator) => {
                stochastic_indicator.set_indicator_columns(lf)
            }
            Self::SchaffTrendCycleIndicator(stc_indicator) => {
                stc_indicator.set_indicator_columns(lf)
            }
            Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
                stochastic_threshold_indicator.set_indicator_columns(lf)
            }
            Self::TrueStrengthIndexIndicator(tsi_indicator) => {
                tsi_indicator.set_indicator_columns(lf)
            }
        }
    }
    fn update_indicator_columns(&self, df: &DataFrame) -> Result<DataFrame, Error> {
        match self {
            Self::ExponentialMovingAverageTrendIndicator(ema_indicator) => {
                ema_indicator.update_indicator_columns(df)
            }
            Self::StochasticIndicator(stochastic_indicator) => {
                stochastic_indicator.update_indicator_columns(df)
            }
            Self::SchaffTrendCycleIndicator(stc_indicator) => {
                stc_indicator.update_indicator_columns(df)
            }
            Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
                stochastic_threshold_indicator.update_indicator_columns(df)
            }
            Self::TrueStrengthIndexIndicator(tsi_indicator) => {
                tsi_indicator.update_indicator_columns(df)
            }
        }
    }
    fn get_data_offset(&self) -> u32 {
        match self {
            Self::ExponentialMovingAverageTrendIndicator(ema_indicator) => {
                ema_indicator.get_data_offset()
            }
            Self::StochasticIndicator(stochastic_indicator) => {
                stochastic_indicator.get_data_offset()
            }
            Self::SchaffTrendCycleIndicator(stc_indicator) => stc_indicator.get_data_offset(),
            Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
                stochastic_threshold_indicator.get_data_offset()
            }
            Self::TrueStrengthIndexIndicator(tsi_indicator) => tsi_indicator.get_data_offset(),
        }
    }
}

impl From<StochasticIndicator> for IndicatorWrapper {
    fn from(value: StochasticIndicator) -> Self {
        IndicatorWrapper::StochasticIndicator(value)
    }
}

impl From<STCIndicator> for IndicatorWrapper {
    fn from(value: STCIndicator) -> Self {
        IndicatorWrapper::SchaffTrendCycleIndicator(value)
    }
}

impl From<ExponentialMovingAverageTrendIndicator> for IndicatorWrapper {
    fn from(value: ExponentialMovingAverageTrendIndicator) -> Self {
        IndicatorWrapper::ExponentialMovingAverageTrendIndicator(value)
    }
}

impl From<StochasticThresholdIndicator> for IndicatorWrapper {
    fn from(value: StochasticThresholdIndicator) -> Self {
        IndicatorWrapper::StochasticThresholdIndicator(value)
    }
}

impl From<TSIIndicator> for IndicatorWrapper {
    fn from(value: TSIIndicator) -> Self {
        IndicatorWrapper::TrueStrengthIndexIndicator(value)
    }
}
