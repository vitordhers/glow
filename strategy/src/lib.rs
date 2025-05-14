use common::structs::SymbolsPair;
use glow_error::GlowError;
use params::{Param, ParamId};
use polars::prelude::{DataFrame, DataType, LazyFrame};
use schemas::{Schema, StrategySchema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
pub mod functions;
pub mod params;
pub mod schemas;
pub mod r#static;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StrategyId {
    #[default]
    SimpleTrend,
}

#[derive(Clone)]
pub struct Strategy {
    pub id: StrategyId,
    pub schema: StrategySchema,
    pub symbols_pair: SymbolsPair,
    pub params: HashMap<ParamId, Param>,
}

impl Strategy {
    pub fn new(id: StrategyId, symbols_pair: SymbolsPair) -> Self {
        let schema: StrategySchema = id.into();
        let params = schema.get_params_config();

        Self {
            id,
            schema,
            symbols_pair,
            params,
        }
    }

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
            .unwrap_or_else(|| panic!("Param {:?} to exist at params", param_id));

        param_to_update.validate(&value)?;
        updated.params.insert(param_id, value);
        Ok(updated)
    }

    pub fn append_indicators_to_lf(&self, lf: LazyFrame) -> Result<LazyFrame, GlowError> {
        self.schema
            .append_indicators_to_lf(lf, self.symbols_pair, &self.params)
    }

    pub fn append_indicators_to_df(&self, df: DataFrame) -> Result<DataFrame, GlowError> {
        self.schema
            .append_indicators_to_df(df, self.symbols_pair, &self.params)
    }

    pub fn append_signals_to_lf(&self, lf: LazyFrame) -> Result<LazyFrame, GlowError> {
        self.schema
            .append_signals_to_lf(lf, self.symbols_pair, &self.params)
    }

    pub fn append_signals_to_df(&self, df: DataFrame) -> Result<DataFrame, GlowError> {
        self.schema
            .append_signals_to_df(df, self.symbols_pair, &self.params)
    }

    pub fn get_params_config(&self) -> HashMap<ParamId, Param> {
        self.schema.get_params_config()
    }

    pub fn get_indicators_columns(&self) -> Vec<(String, DataType)> {
        self.schema
            .get_indicators_columns(self.symbols_pair, &self.params)
    }
    pub fn get_signals_columns(&self) -> Vec<(String, DataType)> {
        self.schema
            .get_signals_columns(self.symbols_pair, &self.params)
    }

    pub fn get_minimum_klines_for_calculation(&self) -> u32 {
        self.schema.get_minimum_klines_for_calculation(&self.params)
    }
}

impl Default for Strategy {
    fn default() -> Self {
        let symbols_pair = SymbolsPair::default();
        let default_schema_id = StrategyId::default();
        Self::new(default_schema_id, symbols_pair)
    }
}
