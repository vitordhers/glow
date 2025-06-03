use super::{cache::FeaturesCache, strategy::FeatureParam};
use common::{
    enums::trading_action::{Direction, SignalSide, SignalType},
    structs::Symbol,
};
use glow_error::GlowError;
use polars::prelude::*;
use std::collections::HashMap;

type GetClosureFnResult =
    Box<dyn Fn(&mut [Column]) -> PolarsResult<Option<Column>> + Send + Sync + 'static>;

pub trait PositionsGenerator: Sync + Send {
    fn generates_signals(&self) -> Vec<SignalType>;
    fn get_closure(
        &self,
        param_combination: Vec<FeatureParam>,
        features_cache: &mut HashMap<&str, FeaturesCache>,
    ) -> GetClosureFnResult;
    fn compute(
        &self,
        lf: &LazyFrame,
        symbol: &Symbol,
        param_combination: Vec<FeatureParam>,
        features_cache: &mut HashMap<&str, FeaturesCache>,
    ) -> Result<LazyFrame, GlowError> {
        let result_lf = lf.clone();
        let close_price_col = symbol.get_close_col();
        let output_type: SpecialEq<Arc<dyn FunctionOutputField>> =
            GetOutput::from_type(DataType::UInt8);
        let arguments: &[Expr] =
            &param_combination
                .iter()
                .cloned()
                .fold(Vec::new(), |mut acc, param| {
                    acc.push(col(param.feature_param_name));
                    acc
                });
        let closure = self.get_closure(param_combination, features_cache);
        let result_lf = result_lf.with_column(
            col(close_price_col)
                .map_many(move |s| closure(s), arguments, output_type)
                .alias("signals"),
        );
        Ok(result_lf)
    }
}

pub struct SimpleEmaShortGenerator {}

impl PositionsGenerator for SimpleEmaShortGenerator {
    fn generates_signals(&self) -> Vec<SignalType> {
        vec![
            SignalType::Open(Direction::Short),
            SignalType::Open(Direction::Long),
            SignalType::Close(SignalSide::Side(Direction::Short)),
            SignalType::Close(SignalSide::Side(Direction::Long)),
        ]
    }

    fn get_closure(
        &self,
        param_combination: Vec<FeatureParam>,
        features_cache: &mut HashMap<&str, FeaturesCache>,
    ) -> GetClosureFnResult {
        let param_combination = param_combination.clone();
        let closure = move |cols: &mut [Column]| {
            let close_prices: Vec<f32> = cols
                .first()
                .expect("columns to have at least one member")
                .f32()
                .expect("")
                .into_no_null_iter()
                .collect();
            let fast_ewma_col = param_combination[0].feature_param_name;
            let slow_ewma_col = param_combination[1].feature_param_name;
            let fast_ewmas: Vec<f32> = cols
                .get(1)
                .expect("columns to have fast ewmas values")
                .f32()
                .expect("")
                .into_no_null_iter()
                .collect();
            let slow_ewmas: Vec<f32> = cols
                .get(2)
                .expect("columns to have slow ewmas values")
                .f32()
                .expect("")
                .into_no_null_iter()
                .collect();
            let min_window_to_calculate = usize::max(
                param_combination[0].param_index as usize,
                param_combination[1].param_index as usize,
            );
            let mut signals: Vec<Option<Vec<u8>>> = vec![None];
            for (index, _) in close_prices.iter().enumerate() {
                if index == 0 {
                    continue;
                }
                if index < min_window_to_calculate {
                    signals.push(None);
                    continue;
                }
                let prev_fast_ewma = fast_ewmas[index - 1];
                let curr_fast_ewma = fast_ewmas[index];
                let prev_slow_ewma = slow_ewmas[index - 1];
                let curr_slow_ewma = slow_ewmas[index];
                let fast_ewma_lesser_than_slow_ewma = curr_fast_ewma < curr_slow_ewma;
                let fast_ewma_greater_than_slow_ewma = curr_fast_ewma > curr_slow_ewma;
                let prev_fast_ewma_lesser_than_prev_slow_ewma = prev_fast_ewma < prev_slow_ewma;
                let prev_fast_ewma_greater_than_prev_slow_ewma = prev_fast_ewma > prev_slow_ewma;

                if fast_ewma_greater_than_slow_ewma && prev_fast_ewma_lesser_than_prev_slow_ewma {
                    signals.push(Some(vec![SignalType::Open(Direction::Long).into()]));
                    continue;
                }
                if fast_ewma_lesser_than_slow_ewma && prev_fast_ewma_greater_than_prev_slow_ewma {
                    signals.push(Some(vec![SignalType::Open(Direction::Short).into()]));
                    continue;
                }
                signals.push(None);
            }
        };
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
        Box::new(closure)
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
