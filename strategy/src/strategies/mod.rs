use crate::{
    enums::StrategyId,
    params::{Param, ParamId},
};
use common::structs::SymbolsPair;
use glow_error::GlowError;
use polars::prelude::{DataFrame, DataType, LazyFrame};
use std::collections::HashMap;
mod simple_trend;
pub use simple_trend::*;

#[derive(Clone)]
pub struct Strategy<S>
where
    S: Schema,
{
    pub id: StrategyId,
    pub schema: S,
    pub symbols_pair: SymbolsPair,
    pub params: HashMap<ParamId, Param>,
}

impl<S> Strategy<S>
where
    S: Schema,
{
    pub fn patch_symbols_pair(&self, updated_symbols_pair: SymbolsPair) -> Self {
        let mut updated_strategy = self.clone();
        updated_strategy.symbols_pair = updated_symbols_pair;

        updated_strategy
    }

    pub fn patch_param(&self, param_id: ParamId, value: Param) -> Result<Self, GlowError> {
        let mut updated = self.clone();
        let params_config = self.schema.get_params_config();
        let param_to_update = params_config
            .get(&param_id)
            .expect(&format!("Param {:?} to exist at params", param_id));

        let _ = param_to_update.validate(&value)?;
        updated.params.insert(param_id, value);
        Ok(updated)
    }

    pub fn append_indicators_to_lf(&self, lf: LazyFrame) -> Result<LazyFrame, GlowError> {
        self.schema
            .append_indicators_to_lf(lf, self.symbols_pair, &self.params)
    }

    pub fn append_indicators_to_df(&self, lf: DataFrame) -> Result<DataFrame, GlowError> {
        self.schema
            .append_indicators_to_df(lf, self.symbols_pair, &self.params)
    }

    pub fn append_signals_to_lf(&self, lf: LazyFrame) -> Result<LazyFrame, GlowError> {
        self.schema
            .append_signals_to_lf(lf, self.symbols_pair, &self.params)
    }

    pub fn append_signals_to_df(&self, lf: DataFrame) -> Result<DataFrame, GlowError> {
        self.schema
            .append_signals_to_df(lf, self.symbols_pair, &self.params)
    }

    pub fn get_params_config(&self) -> HashMap<ParamId, Param> {
        self.schema.get_params_config()
    }

    pub fn get_indicators_columns(&self) -> Vec<(String, DataType)> {
        self.schema
            .get_indicators_columns(self.symbols_pair, &self.params)
    }

    pub fn get_minimum_klines_for_calculation(&self) -> u32 {
        self.schema.get_minimum_klines_for_calculation(&self.params)
    }
    pub fn get_signals_columns(&self) -> Vec<(String, DataType)> {
        self.schema
            .get_signals_columns(self.symbols_pair, &self.params)
    }
}

pub enum StrategiesSchemas {
    SimpleTrend(SimpleTrendStrategySchema),
}

pub const STRATEGIES_IDS: [StrategyId; 1] = [StrategyId::SimpleTrend];

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

// impl<I: IndicatorGen, S: SignalGen> StrategySchema<I, S> {
//     pub fn new(indicators_generator: I, signals_generator: S) -> Self {
//         Self {
//             indicators_generator,
//             signals_generator,
//         }
//     }
// }

// pub trait IndicatorGen {
//     fn get_columns(
//         symbols_pair: SymbolsPair,
//         params: &HashMap<ParamId, Param>,
//     ) -> Vec<(String, DataType)>;
//     fn append_indicators_to_lf(
//         lf: LazyFrame,
//         symbols_pair: SymbolsPair,
//         params: &HashMap<ParamId, Param>,
//     ) -> Result<LazyFrame, GlowError>;
//     fn append_indicators_to_df(
//         df: &DataFrame,
//         symbols_pair: SymbolsPair,
//         params: &HashMap<ParamId, Param>,
//     ) -> Result<DataFrame, GlowError>;
//     fn get_minimum_klines_for_calculation(params: &HashMap<ParamId, Param>) -> u32;
// }

// pub trait SignalGen {
//     // check if this is needed when creating DF
//     // fn get_columns() -> Vec<(String, DataType)>;
//     fn append_signals_to_lf(
//         lf: LazyFrame,
//         symbols_pair: SymbolsPair,
//         params: &HashMap<ParamId, Param>,
//     ) -> Result<LazyFrame, GlowError>;
//     fn append_signals_to_df(
//         df: &DataFrame,
//         symbols_pair: SymbolsPair,
//         params: &HashMap<ParamId, Param>,
//     ) -> Result<DataFrame, GlowError>;
// }
