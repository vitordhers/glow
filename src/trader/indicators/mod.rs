mod functions;
use polars::prelude::*;

use super::{errors::Error, traits::indicator::Indicator};

pub mod ema_trend;
pub mod fast_slow_rmas;
pub mod stc;
pub mod stochastic;
pub mod stochastic_threshold;
pub mod tsi;

use fast_slow_rmas::FastSlowRelativeMovingAverages;
use stochastic::StochasticIndicator;
// use ema_trend::ExponentialMovingAverageTrendIndicator;
// use stc::STCIndicator;
// use stochastic_threshold::StochasticThresholdIndicator;
// use tsi::TSIIndicator;

#[derive(Clone)]
pub enum IndicatorWrapper {
    StochasticIndicator(StochasticIndicator),
    FastSlowRelativeMovingAverages(FastSlowRelativeMovingAverages), // ExponentialMovingAverageTrendIndicator(ExponentialMovingAverageTrendIndicator),
                                                                    // SchaffTrendCycleIndicator(STCIndicator),
                                                                    // StochasticThresholdIndicator(StochasticThresholdIndicator),
                                                                    // TrueStrengthIndexIndicator(TSIIndicator),
}

impl Indicator for IndicatorWrapper {
    fn name(&self) -> String {
        match self {
            Self::StochasticIndicator(indicator) => indicator.name(),
            Self::FastSlowRelativeMovingAverages(indicator) => indicator.name(),
            // Self::ExponentialMovingAverageTrendIndicator(ema_indicator) => ema_indicator.name(),
            // Self::SchaffTrendCycleIndicator(stc_indicator) => stc_indicator.name(),
            // Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
            //     stochastic_threshold_indicator.name()
            // }
            // Self::TrueStrengthIndexIndicator(tsi_indicator) => tsi_indicator.name(),
        }
    }
    fn get_indicator_columns(&self) -> Vec<(String, DataType)> {
        match self {
            Self::StochasticIndicator(indicator) => indicator.get_indicator_columns(),
            Self::FastSlowRelativeMovingAverages(indicator) => indicator.get_indicator_columns(),
            // Self::ExponentialMovingAverageTrendIndicator(ema_indicator) => {
            //     ema_indicator.get_indicator_columns()
            // }
            // Self::SchaffTrendCycleIndicator(stc_indicator) => stc_indicator.get_indicator_columns(),
            // Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
            //     stochastic_threshold_indicator.get_indicator_columns()
            // }
            // Self::TrueStrengthIndexIndicator(tsi_indicator) => {
            //     tsi_indicator.get_indicator_columns()
            // }
        }
    }

    fn set_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, Error> {
        match self {
            Self::StochasticIndicator(indicator) => indicator.set_indicator_columns(lf),
            Self::FastSlowRelativeMovingAverages(indicator) => indicator.set_indicator_columns(lf),
            // Self::ExponentialMovingAverageTrendIndicator(ema_indicator) => {
            //     ema_indicator.set_indicator_columns(lf)
            // }
            // Self::SchaffTrendCycleIndicator(stc_indicator) => {
            //     stc_indicator.set_indicator_columns(lf)
            // }
            // Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
            //     stochastic_threshold_indicator.set_indicator_columns(lf)
            // }
            // Self::TrueStrengthIndexIndicator(tsi_indicator) => {
            //     tsi_indicator.set_indicator_columns(lf)
            // }
        }
    }
    fn update_indicator_columns(&self, df: &DataFrame) -> Result<DataFrame, Error> {
        match self {
            Self::StochasticIndicator(indicator) => indicator.update_indicator_columns(df),
            Self::FastSlowRelativeMovingAverages(indicator) => {
                indicator.update_indicator_columns(df)
            } // Self::ExponentialMovingAverageTrendIndicator(ema_indicator) => {
              //     ema_indicator.update_indicator_columns(df)
              // }
              // Self::SchaffTrendCycleIndicator(stc_indicator) => {
              //     stc_indicator.update_indicator_columns(df)
              // }
              // Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
              //     stochastic_threshold_indicator.update_indicator_columns(df)
              // }
              // Self::TrueStrengthIndexIndicator(tsi_indicator) => {
              //     tsi_indicator.update_indicator_columns(df)
              // }
        }
    }
    fn get_data_offset(&self) -> u32 {
        match self {
            Self::StochasticIndicator(indicator) => {
                indicator.get_data_offset()
            }
            Self::FastSlowRelativeMovingAverages(indicator) => {
                indicator.get_data_offset()
            }
            // Self::ExponentialMovingAverageTrendIndicator(ema_indicator) => {
            //     ema_indicator.get_data_offset()
            // }
            // Self::SchaffTrendCycleIndicator(stc_indicator) => stc_indicator.get_data_offset(),
            // Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
            //     stochastic_threshold_indicator.get_data_offset()
            // }
            // Self::TrueStrengthIndexIndicator(tsi_indicator) => tsi_indicator.get_data_offset(),
        }
    }
}

impl From<StochasticIndicator> for IndicatorWrapper {
    fn from(indicator: StochasticIndicator) -> Self {
        IndicatorWrapper::StochasticIndicator(indicator)
    }
}

impl From<FastSlowRelativeMovingAverages> for IndicatorWrapper {
    fn from(indicator: FastSlowRelativeMovingAverages) -> Self {
        IndicatorWrapper::FastSlowRelativeMovingAverages(indicator)
    }
}

// impl From<STCIndicator> for IndicatorWrapper {
//     fn from(value: STCIndicator) -> Self {
//         IndicatorWrapper::SchaffTrendCycleIndicator(value)
//     }
// }

// impl From<ExponentialMovingAverageTrendIndicator> for IndicatorWrapper {
//     fn from(value: ExponentialMovingAverageTrendIndicator) -> Self {
//         IndicatorWrapper::ExponentialMovingAverageTrendIndicator(value)
//     }
// }

// impl From<StochasticThresholdIndicator> for IndicatorWrapper {
//     fn from(value: StochasticThresholdIndicator) -> Self {
//         IndicatorWrapper::StochasticThresholdIndicator(value)
//     }
// }

// impl From<TSIIndicator> for IndicatorWrapper {
//     fn from(value: TSIIndicator) -> Self {
//         IndicatorWrapper::TrueStrengthIndexIndicator(value)
//     }
// }
