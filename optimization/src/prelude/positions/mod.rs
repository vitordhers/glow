use super::strategy::FeatureParam;
use common::{
    enums::trading_action::{Direction, SignalSide, SignalType, *},
    structs::Symbol,
};
use glow_error::GlowError;
use polars::prelude::*;

type GetClosureFnResult =
    Box<dyn Fn(&mut [Column]) -> PolarsResult<Option<Column>> + Send + Sync + 'static>;

pub trait PositionsGenerator: Sync + Send {
    fn generates_signals(&self) -> Vec<SignalType>;
    fn get_closure(&self, param_combination: &Vec<FeatureParam>) -> GetClosureFnResult;
    fn compute(
        &self,
        lf: &LazyFrame,
        symbol: &Symbol,
        param_combination: Vec<FeatureParam>,
    ) -> Result<LazyFrame, GlowError> {
        let result_lf = lf.clone();
        let close_price_col = symbol.get_close_col();
        let output_type: SpecialEq<Arc<dyn FunctionOutputField>> =
            GetOutput::from_type(DataType::List(Box::new(DataType::UInt8)));
        let arguments: &[Expr] =
            &param_combination
                .iter()
                .cloned()
                .fold(Vec::new(), |mut acc, param| {
                    acc.push(col(param.feature_param_name));
                    acc
                });
        let closure = self.get_closure(&param_combination);
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

    fn get_closure(&self, param_combination: &Vec<FeatureParam>) -> GetClosureFnResult {
        let param_combination = param_combination.clone();
        let closure = move |cols: &mut [Column]| {
            let close_prices: Vec<f32> = cols
                .first()
                .expect("columns to have at least one member")
                .f32()
                .expect("")
                .into_no_null_iter()
                .collect();
            // let fast_ewma_col = param_combination[0].feature_param_name;
            // let slow_ewma_col = param_combination[1].feature_param_name;
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
            let mut ca_builder: ListPrimitiveChunkedBuilder<UInt8Type> =
                ListPrimitiveChunkedBuilder::new(
                    "".into(),
                    close_prices.len(),
                    2 * close_prices.len(),
                    DataType::UInt8,
                );
            let open_long_signal: u8 = SignalType::Open(Direction::Long).try_into().unwrap();
            let open_short_signal: u8 = SignalType::Open(Direction::Short).try_into().unwrap();
            let close_long_signal: u8 = SignalType::Close(SignalSide::Side(Direction::Long))
                .try_into()
                .unwrap();
            let close_short_signal: u8 = SignalType::Close(SignalSide::Side(Direction::Short))
                .try_into()
                .unwrap();
            for (index, _) in close_prices.iter().enumerate() {
                if index == 0 {
                    continue;
                }
                if index < min_window_to_calculate {
                    ca_builder.append_opt_slice(None);
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
                // fast_ema_greater_than_slow_ema && prev_fast_ema_lesser_than_prev_slow_ema -> long
                // fast_ema_greater_than_slow_ema && prev_fast_ema_lesser_than_prev_slow_ema -> close_short
                if fast_ewma_greater_than_slow_ewma && prev_fast_ewma_lesser_than_prev_slow_ewma {
                    ca_builder.append_opt_slice(Some(&[open_long_signal, close_short_signal]));
                    continue;
                }
                // fast_ema_lesser_than_slow_ema && prev_fast_ema_greater_than_prev_slow_ema -> short
                // fast_ema_lesser_than_slow_ema && (prev_fast_ema_greater_than_prev_slow_ema -> close_long
                if fast_ewma_lesser_than_slow_ewma && prev_fast_ewma_greater_than_prev_slow_ewma {
                    ca_builder.append_opt_slice(Some(&[open_short_signal, close_long_signal]));
                    continue;
                }
                ca_builder.append_opt_slice(None);
            }
            let chunked_array: ChunkedArray<ListType> = ca_builder.finish();
            let list_series = chunked_array.into_series();
            Ok(Some(list_series.into()))
        };
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
