use super::{cache::FeaturesCache, strategy::FeatureParam};
use common::{
    enums::trading_action::{Direction, SignalType},
    structs::Symbol,
};
use glow_error::GlowError;
use polars::prelude::*;
use std::collections::HashMap;

pub trait PositionGenerator: Sync + Send {
    fn get_signal(&self) -> SignalType;
    fn get_position_name(&self) -> &'static str {
        self.get_signal().as_str()
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
    fn get_signal(&self) -> SignalType {
        SignalType::Open(Direction::Short)
    }

    fn compute(
        &self,
        lf: &LazyFrame,
        symbol: &Symbol,
        param_combination: Vec<FeatureParam>,
        features_cache: &mut HashMap<&str, FeaturesCache>,
    ) -> Result<LazyFrame, GlowError> {
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
        todo!()
    }
}
//
//

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

#[test]
fn test_bool_cols() -> Result<(), GlowError> {
    let bools = Series::new("".into(), &[true, false, true, true]);
    let column = Column::new("test_col".into(), bools.into_series());
    let df = DataFrame::new(vec![column])?;
    println!("{:?}", df);
    Ok(())
}
