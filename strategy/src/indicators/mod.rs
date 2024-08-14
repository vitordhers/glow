use common::traits::indicator::Indicator;
use glow_error::GlowError;
use polars::prelude::*;
// pub mod fast_slow_cloud_rmas;
// use fast_slow_cloud_rmas::FastSlowCloudRMAs;
// pub mod stc;
// pub mod stochastic;
// pub mod stochastic_threshold;
// pub mod tsi;
// use fast_slow_cloud_rmas::FastSlowCloudRMAs;
// use stochastic::StochasticIndicator;
// use stc::STCIndicator;
// use stochastic_threshold::StochasticThresholdIndicator;
// use tsi::TSIIndicator;

#[derive(Clone)]
pub enum IndicatorWrapper {
    // Ema(EMA),
    /* StochasticIndicator(StochasticIndicator),
    // FastSlowCloudRMAs(FastSlowCloudRMAs),
    // SchaffTrendCycleIndicator(STCIndicator),
    // StochasticThresholdIndicator(StochasticThresholdIndicator),
    // TrueStrengthIndexIndicator(TSIIndicator), */
}

/// Indicators are defined as such:
/// They provide data in order to signals be set.
/// They always must be used when calculating signals, while preindicators generated columns may not always
/// be used in order to calculate signals.
impl Indicator for IndicatorWrapper {
    fn name(&self) -> &'static str {
        todo!("implement this")
        /*match self {
            Self::Ema(ema) => ema.name(),
             Self::StochasticIndicator(indicator) => indicator.name(),
            // Self::FastSlowCloudRMAs(indicator) => indicator.name(),
            // Self::SchaffTrendCycleIndicator(stc_indicator) => stc_indicator.name(),
            // Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
            //     stochastic_threshold_indicator.name()
            // }
            // Self::TrueStrengthIndexIndicator(tsi_indicator) => tsi_indicator.name(),
        }*/
    }
    fn get_indicator_columns(&self) -> Vec<(String, DataType)> {
        todo!("implement this")

        /*match self {
            Self::Ema(ema) => ema.get_indicator_columns(),
            // Self::StochasticIndicator(indicator) => indicator.get_indicator_columns(),
            Self::FastSlowCloudRMAs(indicator) => indicator.get_indicator_columns(),
            // Self::SchaffTrendCycleIndicator(stc_indicator) => stc_indicator.get_indicator_columns(),
            // Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
            //     stochastic_threshold_indicator.get_indicator_columns()
            // }
            // Self::TrueStrengthIndexIndicator(tsi_indicator) => {
            //     tsi_indicator.get_indicator_columns()
            // }
        }*/
    }

    fn set_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, GlowError> {
        todo!("implement this")

        /*
        match self {
             Self::Ema(ema) => ema.set_indicator_columns(lf),
            Self::StochasticIndicator(indicator) => indicator.set_indicator_columns(lf),
            // Self::FastSlowCloudRMAs(indicator) => indicator.set_indicator_columns(lf),
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
        */
    }
    fn update_indicator_columns(&self, df: &DataFrame) -> Result<DataFrame, GlowError> {
        todo!("implement this")

        /*match self {
            Self::Ema(ema) => ema.update_indicator_columns(df),
             Self::StochasticIndicator(indicator) => indicator.update_indicator_columns(df),
            // Self::FastSlowCloudRMAs(indicator) => indicator.update_indicator_columns(df),
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
        */
    }
    fn get_minimum_klines_for_benchmarking(&self) -> u32 {
        todo!("implement this")

        /*match self {
            Self::Ema(ema) => ema.get_minimum_klines_for_benchmarking(),
             Self::StochasticIndicator(indicator) => indicator.get_data_offset(),
            Self::FastSlowCloudRMAs(indicator) => indicator.get_data_offset(),
            Self::SchaffTrendCycleIndicator(stc_indicator) => stc_indicator.get_data_offset(),
            Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
                stochastic_threshold_indicator.get_data_offset()
            }
            Self::TrueStrengthIndexIndicator(tsi_indicator) => tsi_indicator.get_data_offset(),
        }*/
    }
    
}

// impl From<StochasticIndicator> for IndicatorWrapper {
//     fn from(indicator: StochasticIndicator) -> Self {
//         IndicatorWrapper::StochasticIndicator(indicator)
//     }
// }

// impl From<FastSlowCloudRMAs> for IndicatorWrapper {
//     fn from(indicator: FastSlowCloudRMAs) -> Self {
//         IndicatorWrapper::FastSlowCloudRMAs(indicator)
//     }
// }

// impl From<STCIndicator> for IndicatorWrapper {
//     fn from(value: STCIndicator) -> Self {
//         IndicatorWrapper::SchaffTrendCycleIndicator(value)
//     }
// }

// impl From<EMA> for IndicatorWrapper {
//     fn from(ema: EMA) -> Self {
//         Self::Ema(ema)
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
