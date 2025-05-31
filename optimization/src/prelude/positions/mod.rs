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

#[derive(Debug, Clone, Copy)]
pub struct FeatureParam<'a> {
    pub feature_name: &'static str,
    pub feature_param_name: &'a str,
    pub param_index: u32,
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
        param_combination: Vec<FeatureParam>,
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
        param_combination: Vec<FeatureParam>,
        features_cache: &mut HashMap<&str, FeaturesCache>,
    ) -> Result<LazyFrame, GlowError> {
        let mut features_lf = lf.clone();
        for feature_param in param_combination {
            let cache = features_cache.get_mut(feature_param.feature_name);
            features_lf = append_feature_param_col(features_lf, feature_param, cache)?;
        }
        todo!();
        // let short_col = SignalCategory::GoShort.get_column();
        // let long_col = SignalCategory::GoLong.get_column();
        // let close_short_col = SignalCategory::CloseShort.get_column();
        // let close_long_col = SignalCategory::CloseLong.get_column();
        // // let select_columns = vec![col("start_time"), col(signal_col)];
        //
        // let fast_ema_col = format!("{}_fast_ema", &symbols_pair.quote.name);
        // let slow_ema_col = format!("{}_slow_ema", &symbols_pair.quote.name);
        //
        // let fast_ema_lesser_than_slow_ema = col(&fast_ema_col).lt(col(&slow_ema_col));
        // let fast_ema_greater_than_slow_ema = col(&fast_ema_col).gt(col(&slow_ema_col));
        //
        // let prev_fast_ema_lesser_than_prev_slow_ema = col(&fast_ema_col)
        //     .shift_and_fill(lit(1), lit(NULL))
        //     .lt(col(&slow_ema_col).shift_and_fill(lit(1), lit(NULL)));
        // let prev_fast_ema_greater_than_prev_slow_ema = col(&fast_ema_col)
        //     .shift_and_fill(lit(1), lit(NULL))
        //     .gt(col(&slow_ema_col).shift_and_fill(lit(1), lit(NULL)));
        //
        // let signal_lf = lf.clone().with_columns([
        //     when(
        //         fast_ema_lesser_than_slow_ema
        //             .clone()
        //             .and(prev_fast_ema_greater_than_prev_slow_ema.clone()),
        //     )
        //     .then(lit(1))
        //     .otherwise(lit(0))
        //     .alias(short_col),
        //     when(
        //         fast_ema_greater_than_slow_ema
        //             .clone()
        //             .and(prev_fast_ema_lesser_than_prev_slow_ema.clone()),
        //     )
        //     .then(lit(1))
        //     .otherwise(lit(0))
        //     .alias(long_col),
        //     when(fast_ema_greater_than_slow_ema.and(prev_fast_ema_lesser_than_prev_slow_ema))
        //         .then(lit(1))
        //         .otherwise(lit(0))
        //         .alias(close_short_col),
        //     when(fast_ema_lesser_than_slow_ema.and(prev_fast_ema_greater_than_prev_slow_ema))
        //         .then(lit(1))
        //         .otherwise(lit(0))
        //         .alias(close_long_col),
        // ]);
        //
        // Ok(signal_lf)
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
