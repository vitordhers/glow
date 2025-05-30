use super::{cache::MaxReadsCache, strategy::FeaturesCache};
use common::structs::Symbol;
use glow_error::GlowError;
use polars::prelude::*;
use std::{collections::HashMap, sync::LazyLock};

pub static POSITIONS_NAME_MAP: LazyLock<HashMap<Signal, &'static str>> = LazyLock::new(|| {
    let mut positions_name_map = HashMap::new();
    positions_name_map.insert(Signal::Open(OpenSignal::Short), "short");
    positions_name_map.insert(Signal::Open(OpenSignal::Long), "long");
    positions_name_map.insert(Signal::Close(CloseSignal::Short), "short_close");
    positions_name_map.insert(Signal::Close(CloseSignal::Long), "short_close");
    positions_name_map.insert(Signal::Close(CloseSignal::Close), "close");
    positions_name_map
});

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub enum OpenSignal {
    Short,
    Long,
}

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub enum CloseSignal {
    Short,
    Long,
    Close,
}

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub enum Signal {
    Open(OpenSignal),
    Close(CloseSignal),
}

pub trait PositionGenerator: Sync + Send {
    fn get_signal(&self) -> Signal;
    fn get_position_name(&self) -> &'static str {
        POSITIONS_NAME_MAP.get(&self.get_signal()).unwrap()
    }
    fn compute(
        &self,
        lf: &LazyFrame,
        symbol: &Symbol,
        param_combination: Vec<(&str, u32)>,
        features_cache: &mut HashMap<&str, FeaturesCache>,
    ) -> Result<LazyFrame, GlowError>;
}

pub struct SimpleEmaShortGenerator {}

impl PositionGenerator for SimpleEmaShortGenerator {
    fn get_signal(&self) -> Signal {
        Signal::Open(OpenSignal::Short)
    }

    fn compute(
        &self,
        lf: &LazyFrame,
        symbol: &Symbol,
        param_combination: Vec<(&str, u32)>,
        features_cache: &mut HashMap<&str, FeaturesCache>,
    ) -> Result<LazyFrame, GlowError> {
        let mut features_lf = lf.clone();
        for (feature_name, param_index) in param_combination {
            let cache = features_cache.get_mut(feature_name);
            features_lf = replace_param_col(features_lf, feature_name, param_index, cache)?;
        }
        todo!();
        // let features_lf = match features_cache {
        //     FeaturesCacheStrategy::None => todo!(),
        //     FeaturesCacheStrategy::Eager(hash_map) => {
        //         let mut df = DataFrame::empty();
        //         let mut columns = vec![];
        //         // add start_time series
        //         for (param_name, param_index) in param_combination.iter() {
        //             let param_cache = hash_map.get(param_name).expect("hashmap to contain param");
        //             let column = get_param_from_cache(param_name, param_index, param_cache)?;
        //             columns.push(column);
        //         }
        //         df = df.hstack(&columns)?;
        //         df.lazy()
        //     }
        //     FeaturesCacheStrategy::LazyMaxReads(hash_map) => todo!(),
        // };
        // let updated_lf = lf
        //     .clone()
        //     .left_join(features_lf, "start_time", "start_time");
        // create new df from cache / features
        // lazify if needed
        // join lfs
    }
}
//
//

fn replace_lf_col_by_series(lf: LazyFrame, name: &str, series: Series) -> LazyFrame {
    // TODO: some sort of assertion would be nice here
    lf.with_column(lit(series).alias(name))
}

fn replace_param_col(
    lf: LazyFrame,
    feature_name: &str,
    param_index: u32,
    cache: Option<&mut FeaturesCache>,
) -> Result<LazyFrame, GlowError> {
    let lf = match cache.unwrap_or(&mut FeaturesCache::None) {
        FeaturesCache::None => replace_feature_index_in_lf(lf, feature_name, param_index),
        FeaturesCache::Eager(cache) => {
            replace_lf_col_by_series(lf, feature_name, cache.get(&param_index).unwrap().clone())
        }
        FeaturesCache::List(feature_series) => {
            append_feature_index_to_lf(lf, feature_series, feature_name, &param_index)?
        }
        FeaturesCache::LazyMaxReads(cache) => {
            append_feature_index_to_lf_from_mrc(lf, cache, feature_name, &param_index)?
        }
    };
    Ok(lf)
}

fn replace_feature_index_in_lf(lf: LazyFrame, feature_name: &str, param_index: u32) -> LazyFrame {
    // TODO: both closure and output type of this depends on feature DType
    let output_type: SpecialEq<Arc<dyn FunctionOutputField>> =
        GetOutput::from_type(DataType::Float32);
    let param_index = param_index as usize;
    lf.with_column(
        col(feature_name)
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
            .alias(feature_name),
    )
}

fn get_features_at_param_index_from_list_series(
    feature_series: &Series,
    feature_name: &str,
    param_index: &u32,
) -> Result<Series, GlowError> {
    let features_list_ca = feature_series.list()?;
    let mut features_list = Vec::new();
    for opt_series in features_list_ca.amortized_iter() {
        let inner_series = opt_series.expect("AmortSeries to be Some");
        // TODO: this must be converted according to feature DType
        let ca = inner_series.as_ref().f32()?;
        let values: Vec<f32> = ca.into_no_null_iter().collect();
        features_list.push(values[(*param_index) as usize])
    }
    Ok(Series::new(feature_name.into(), features_list))
}

fn append_feature_index_to_lf(
    lf: LazyFrame,
    feature_series: &Series,
    feature_name: &str,
    param_index: &u32,
) -> Result<LazyFrame, GlowError> {
    let series =
        get_features_at_param_index_from_list_series(feature_series, feature_name, param_index)?;
    Ok(lf.with_column(lit(series).alias(feature_name)))
}

fn append_feature_index_to_lf_from_mrc(
    lf: LazyFrame,
    param_cache: &mut MaxReadsCache,
    feature_name: &str,
    param_index: &u32,
) -> Result<LazyFrame, GlowError> {
    let series = param_cache.get(param_index).unwrap_or_else(|| {
        let series = get_features_at_param_index_from_list_series(
            &param_cache.feature_series,
            feature_name,
            param_index,
        )
        .expect("get_params_from_list_at_index to yield valid series");
        Arc::new(series)
    });
    let _ = param_cache.insert(*param_index, &series);
    let lf = replace_lf_col_by_series(lf, feature_name, (*series).clone());
    Ok(lf)
}

#[test]
fn test_iteration() {
    use itertools::*;
    use rayon::prelude::*;
    use std::collections::HashMap;
    let vec1 = (6..=30).collect::<Vec<u32>>(); // 25
    let mut vec1_params = HashMap::new();
    vec1.iter().for_each(|v| {
        vec1_params.insert(v, v + 1);
    });
    let vec2 = (51..=100).collect::<Vec<u32>>(); // 50
    let mut vec2_params = HashMap::new();
    vec2.iter().for_each(|v| {
        vec2_params.insert(v, v + 1);
    });
    let iter: Vec<_> = iproduct!(vec1, vec2).collect();
    iter.par_iter().for_each(|value| {
        println!("VALUE {:?}", &value);
    });
}

#[test]
fn test_col_replace() {
    let df = df![
        "a" => &[1, 2, 3],
        "b" => &[4, 5, 6]
    ]
    .unwrap();
    println!("INITIAL DF {:?}", df);
    let lf = df.lazy();
    let row_count = lf.clone().select([len().alias("count")]).collect().unwrap();
    let height = row_count.column("count").unwrap().get(0);
    println!("LF ROW COUNT {:?}", height);
    let new_col = Series::new("a".into(), &[10, 20, 30]);
    let lf = lf.with_column(lit(new_col).alias("a")); // ✅ Replaces "a"
    let out = lf.collect().unwrap();
    println!("REPLACED DF {:?}", out);
}
