use super::{cache::MaxReadsCache, features::FeatureGenerator, positions::PositionGenerator};
use chrono::{DateTime, Utc};
use common::structs::{Symbol, SymbolsPair};
use glow_error::GlowError;
use polars::prelude::*;
use std::{collections::HashMap, sync::LazyLock};

#[derive(Clone, Copy, Debug)]
pub struct BacktestPeriod {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Default, Hash, PartialEq, Eq)]
pub enum OptimizableStrategyId {
    #[default]
    SimpleEwma,
}

pub static STRATEGIES_MAP: LazyLock<HashMap<OptimizableStrategyId, Strategy>> =
    LazyLock::new(|| {
        //   [
        //     Strategy::new(
        //     "simple_ewma",
        //     vec![Box::new(EMAFeatureGenerator::new(
        //         "ewma",
        //         OptimizableParam::new("ewma", 5, 1, 200),
        //     ))],
        // )
        // ]
        HashMap::new()
    });

pub struct Strategy {
    pub name: &'static str,
    pub indicators: Vec<Box<dyn FeatureGenerator>>,
    pub open_signals: Vec<Box<dyn PositionGenerator>>,
    pub close_signals: Vec<Box<dyn PositionGenerator>>,
}

impl Strategy {
    pub fn new(
        name: &'static str,
        indicators: Vec<Box<dyn FeatureGenerator>>,
        open_signals: Vec<Box<dyn PositionGenerator>>,
        close_signals: Vec<Box<dyn PositionGenerator>>,
    ) -> Self {
        Self {
            name,
            indicators,
            open_signals,
            close_signals,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub enum FeaturesCache {
    #[default]
    None,
    List(Series),
    Eager(HashMap<u32, Series>),
    LazyMaxReads(MaxReadsCache),
}

// #[derive(Clone)]
pub struct StrategyOptimizer {
    pub symbol: Symbol,
    pub period: BacktestPeriod,
    pub features_cache_map: HashMap<&'static str, FeaturesCache>,
    pub data: LazyFrame,
    pub strategy: &'static Strategy,
}

impl StrategyOptimizer {
    pub fn new(period: BacktestPeriod, strategy_id: Option<OptimizableStrategyId>) -> Self {
        Self {
            symbol: *SymbolsPair::default().quote,
            period,
            features_cache_map: HashMap::new(),
            data: DataFrame::empty().lazy(),
            strategy: STRATEGIES_MAP
                .get(&strategy_id.unwrap_or_default())
                .unwrap(),
        }
    }

    async fn fetch_data(&mut self) -> Result<(), GlowError> {
        // fetch data then define cache strategy based on config
        todo!();
    }

    pub fn compute_features(&mut self) -> Result<(), GlowError> {
        let mut data = self.data.clone();
        for feature_generator in self.strategy.indicators.iter().clone() {
            data = feature_generator.compute(&data, &self.symbol, &mut self.features_cache_map)?;
        }
        Ok(())
    }
}

#[test]
fn test_create_strategy() {}

// pub trait OptimizableStrategy {
//     fn get_name(&self) -> &str;
//     fn get_indicators(&self) -> Vec<Box<dyn FeatureGenerator>>;
//     fn get_opening_indicators(&self) -> Vec<Box<dyn FeatureGenerator>>;
//
//     fn get_params(&self) -> Vec<CombinableParam> {
//         self.get_indicators()
//             .iter()
//             .map(|i| i.get_param())
//             .collect()
//     }
//     fn compute_features(&self, lf: &LazyFrame, symbol: &Symbol) -> Result<LazyFrame, GlowError> {
//         let mut result_lf = lf.clone();
//         for indicator_generator in self.get_opening_indicators() {
//             result_lf = indicator_generator.compute(&result_lf, symbol)?;
//         }
//         Ok(result_lf)
//     }
//
//     fn get_cartesian_product_len(&self) -> u32 {
//         self.get_params().iter().fold(0u32, |prev, p| {
//             let current_range_size: u32 = p.range_size().into();
//             if prev == 0 {
//                 return current_range_size;
//             }
//             prev * current_range_size
//         })
//     }
// }

#[test]
fn test_multi_cartesian_product() {
    use itertools::Itertools;
    let a = [1, 2];
    let b = [3, 4];
    let c = [5, 6];
    let d = [7, 8];
    let e = [9, 10];

    let inputs = vec![a.iter(), b.iter(), c.iter(), d.iter(), e.iter()];

    for combo in inputs.into_iter().multi_cartesian_product() {
        println!("{:?}", combo);
    }
}
