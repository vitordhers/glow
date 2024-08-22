use common::{structs::SymbolsPair, traits::indicator::Indicator};
use glow_error::GlowError;
use polars::prelude::*;
pub mod ema;
use ema::{EMAParams, EMA};

/// Preindicators are defined as such:
/// They don't need any additional column information in order to be calculated, besides kline data.
/// Also, they may not be used when calculating signals, while indicators generated columns must always
/// be used in order to calculate signals. That being, preindicators may serve as helper columns in order
/// to calculate indicators.
#[derive(Clone, Debug)]
pub enum PreIndicatorWrapper {
    Ema(EMA),
}

#[derive(Debug, Clone)]
pub enum PreIndicatorParamsWrapper {
    Ema(EMAParams),
}

impl Indicator for PreIndicatorWrapper {
    type Params = PreIndicatorParamsWrapper;
    type Wrapper = Self;

    fn name(&self) -> &'static str {
        match self {
            Self::Ema(ema) => ema.name(),
        }
    }

    fn get_indicator_columns(&self) -> &Vec<(String, DataType)> {
        match self {
            Self::Ema(ema) => ema.get_indicator_columns(),
        }
    }

    fn set_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, GlowError> {
        match self {
            Self::Ema(ema) => ema.set_indicator_columns(lf),
        }
    }
    fn update_indicator_columns(&self, df: &DataFrame) -> Result<DataFrame, GlowError> {
        match self {
            Self::Ema(ema) => ema.update_indicator_columns(df),
        }
    }
    fn get_minimum_klines_for_benchmarking(&self) -> u32 {
        match self {
            Self::Ema(ema) => ema.get_minimum_klines_for_benchmarking(),
        }
    }
    fn patch_params(&self, params: Self::Params) -> Result<Self::Wrapper, GlowError> {
        match self {
            Self::Ema(ema) => ema.patch_params(params.into()),
        }
    }

    fn patch_symbols_pair(
        &self,
        updated_symbols_pair: SymbolsPair,
    ) -> Result<Self::Wrapper, GlowError> {
        match self {
            Self::Ema(ema) => Ok(ema.patch_symbols_pair(updated_symbols_pair)?),
        }
    }
}

impl From<EMA> for PreIndicatorWrapper {
    fn from(ema: EMA) -> Self {
        Self::Ema(ema)
    }
}
