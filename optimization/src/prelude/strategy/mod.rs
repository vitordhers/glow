use super::{
    cache::{FeaturesCache, MaxReadsCache},
    features::FeatureGenerator,
    positions::PositionsGenerator,
};
use chrono::{DateTime, Utc};
use common::structs::{Symbol, SymbolsPair};
use glow_error::GlowError;
use polars::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    sync::LazyLock,
};

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

#[derive(Debug, Clone, Copy)]
pub struct FeatureParam<'a> {
    pub feature_name: &'static str,
    pub feature_param_name: &'a str,
    pub param_index: u32,
}

pub struct Strategy {
    pub name: &'static str,
    pub features_generators: Vec<Box<dyn FeatureGenerator>>,
    pub positions_generators: Box<dyn PositionsGenerator>,
}

impl Strategy {
    pub fn new(
        name: &'static str,
        features_generators: Vec<Box<dyn FeatureGenerator>>,
        positions_generators: Box<dyn PositionsGenerator>,
    ) -> Self {
        Self {
            name,
            features_generators,
            positions_generators,
        }
    }
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
        for feature_generator in self.strategy.features_generators.iter().clone() {
            data = feature_generator.compute(&data, &self.symbol, &mut self.features_cache_map)?;
        }
        Ok(())
    }

    fn select_features_for_param_combination(
        &mut self,
        param_combination: Vec<FeatureParam>,
    ) -> Result<LazyFrame, GlowError> {
        let mut features_lf = self.data.clone();
        let mut cols_to_drop = HashSet::new();
        for feature_param in param_combination {
            let cache = self.features_cache_map.get_mut(feature_param.feature_name);
            features_lf = append_feature_param_col(features_lf, feature_param, cache)?;
            cols_to_drop.insert(feature_param.feature_name);
        }
        // drop params from lf
        features_lf = features_lf.drop(cols_to_drop.iter().copied().collect::<Vec<_>>());
        Ok(features_lf)
    }
}

fn set_series_as_lf_col(lf: LazyFrame, feature_param_name: &str, series: Series) -> LazyFrame {
    // TODO: some sort of assertion would be nice here
    lf.with_column(lit(series).alias(feature_param_name))
}

fn lazily_append_feature_index_to_lf(lf: LazyFrame, feature_param: FeatureParam) -> LazyFrame {
    // TODO: both closure and output type of this depends on feature DType
    let output_type: SpecialEq<Arc<dyn FunctionOutputField>> =
        GetOutput::from_type(DataType::Float32);
    let param_index = feature_param.param_index as usize;
    lf.with_column(
        col(feature_param.feature_name)
            .map(
                move |column| {
                    let params = column.list()?;
                    let mut outer = vec![];
                    for series in params.into_no_null_iter() {
                        let ca = series.f32()?;
                        let inner: Vec<f32> = ca.into_no_null_iter().collect();
                        outer.push(inner[param_index]);
                    }
                    let series = Series::new("".into(), outer);
                    Ok(Some(series.into()))
                },
                output_type,
            )
            .alias(feature_param.feature_param_name),
    )
}

fn with_list_series_get_features_at_param(
    feature_series: &Series,
    feature_param: FeatureParam,
) -> Result<Series, GlowError> {
    let features_list_ca = feature_series.list()?;
    let mut features_list = Vec::new();
    for opt_series in features_list_ca.amortized_iter() {
        let inner_series = opt_series.expect("AmortSeries to be Some");
        // TODO: this must be converted according to feature DType
        let ca = inner_series.as_ref().f32()?;
        let values: Vec<f32> = ca.into_no_null_iter().collect();
        features_list.push(values[(feature_param.param_index) as usize])
    }
    Ok(Series::new(
        feature_param.feature_param_name.into(),
        features_list,
    ))
}

fn with_series_append_feature_index_to_lf(
    lf: LazyFrame,
    feature_series: &Series,
    feature_param: FeatureParam,
) -> Result<LazyFrame, GlowError> {
    let series = with_list_series_get_features_at_param(feature_series, feature_param)?;
    Ok(set_series_as_lf_col(
        lf,
        feature_param.feature_param_name,
        series,
    ))
}

fn append_feature_param_to_lf_from_mrc(
    lf: LazyFrame,
    param_cache: &mut MaxReadsCache,
    feature_param: FeatureParam,
) -> Result<LazyFrame, GlowError> {
    let series = param_cache
        .get(&feature_param.param_index)
        .unwrap_or_else(|| {
            let series =
                with_list_series_get_features_at_param(&param_cache.feature_series, feature_param)
                    .expect("get_params_from_list_at_index to yield valid series");
            Arc::new(series)
        });
    let _ = param_cache.insert(feature_param.param_index, &series);
    let lf = set_series_as_lf_col(lf, feature_param.feature_param_name, (*series).clone());
    Ok(lf)
}

fn append_feature_param_col(
    lf: LazyFrame,
    feature_param: FeatureParam,
    cache: Option<&mut FeaturesCache>,
) -> Result<LazyFrame, GlowError> {
    let lf = match cache.unwrap_or(&mut FeaturesCache::None) {
        FeaturesCache::None => lazily_append_feature_index_to_lf(lf, feature_param),
        FeaturesCache::Eager(cache) => set_series_as_lf_col(
            lf,
            feature_param.feature_param_name,
            cache.get(&feature_param.param_index).unwrap().clone(),
        ),
        FeaturesCache::List(feature_series) => {
            with_series_append_feature_index_to_lf(lf, feature_series, feature_param)?
        }
        FeaturesCache::LazyMaxReads(cache) => {
            append_feature_param_to_lf_from_mrc(lf, cache, feature_param)?
        }
    };
    Ok(lf)
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
