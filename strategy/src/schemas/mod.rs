use enum_dispatch::enum_dispatch;
use simple_trend::SimpleTrendStrategySchema;
mod simple_trend;
use crate::{
    params::{Param, ParamId},
    StrategyId,
};
use common::structs::SymbolsPair;
use glow_error::GlowError;
use polars::prelude::{DataFrame, DataType, LazyFrame};
use std::collections::HashMap;

#[enum_dispatch(Schema)]
#[derive(Clone, Copy)]
pub enum StrategySchema {
    SimpleTrend(SimpleTrendStrategySchema),
}

#[enum_dispatch]
pub trait Schema: Clone + Sized {
    fn append_indicators_to_lf(
        &self,
        lf: LazyFrame,
        symbols_pair: SymbolsPair,
        params: &HashMap<ParamId, Param>,
    ) -> Result<LazyFrame, GlowError>;
    fn append_indicators_to_df(
        &self,
        lf: DataFrame,
        symbols_pair: SymbolsPair,
        params: &HashMap<ParamId, Param>,
    ) -> Result<DataFrame, GlowError>;
    fn append_signals_to_lf(
        &self,
        lf: LazyFrame,
        symbols_pair: SymbolsPair,
        params: &HashMap<ParamId, Param>,
    ) -> Result<LazyFrame, GlowError>;
    fn append_signals_to_df(
        &self,
        lf: DataFrame,
        symbols_pair: SymbolsPair,
        params: &HashMap<ParamId, Param>,
    ) -> Result<DataFrame, GlowError>;
    fn get_params_config(&self) -> HashMap<ParamId, Param>;
    fn get_indicators_columns(
        &self,
        symbols_pair: SymbolsPair,
        params: &HashMap<ParamId, Param>,
    ) -> Vec<(String, DataType)>;
    fn get_minimum_klines_for_calculation(&self, params: &HashMap<ParamId, Param>) -> u32;
    fn get_signals_columns(
        &self,
        symbols_pair: SymbolsPair,
        params: &HashMap<ParamId, Param>,
    ) -> Vec<(String, DataType)>;
}

impl Default for StrategySchema {
    fn default() -> Self {
        let schema = SimpleTrendStrategySchema::default();
        Self::SimpleTrend(schema)
    }
}

impl From<StrategyId> for StrategySchema {
    fn from(value: StrategyId) -> Self {
        match value {
            StrategyId::SimpleTrend => {
                let schema = value.into();
                StrategySchema::SimpleTrend(schema)
            }
        }
    }
}
