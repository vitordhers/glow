use super::combinable_params::{CombinableParam, FeatureGenerator, OptimizableParam};
use common::structs::SymbolsPair;
use glow_error::GlowError;
use polars::{chunked_array::builder::fixed_size_list, prelude::*};

pub struct EMAFeatureGenerator {
    pub name: &'static str,
    pub param: OptimizableParam<u8>,
    pub symbols: SymbolsPair,
}

impl FeatureGenerator for EMAFeatureGenerator {
    fn get_name(&self) -> &str {
        self.name
    }

    fn get_param(&self) -> CombinableParam {
        CombinableParam::Uint8(self.param.clone())
    }

    fn compute(&self, lf: &LazyFrame) -> Result<LazyFrame, GlowError> {
        // firstly, get param range size
        let param_size = self.param.range_size();
        let close_col = self.symbols.quote.get_close_col();
        let mut result_lf = lf.clone();
        let output_type: SpecialEq<Arc<dyn FunctionOutputField>> = GetOutput::from_type(
            DataType::Array(Box::new(DataType::Float32), param_size.into()),
        );
        let range = self.param.range.clone();
        let name = PlSmallStr::from_static(self.name);

        result_lf = result_lf.with_column(
            col(close_col)
                .apply_many(
                    move |columns| {
                        // 1440 prices for the day
                        let close_prices = columns
                    .get(0)
                    .expect("close prices column to be defined at EMA Feature Generator compute")
                    .f32()?;
                        let close_prices: Vec<f32> = close_prices.into_no_null_iter().collect();
                        let mut alphas: Vec<f32> = vec![];
                        for param in range.clone() {
                            let alpha =
                                calculate_span_alpha(param.into()).expect("alpha to be valid");
                            alphas.push(alpha);
                        }
                        let capacity = range.len();
                        let values_capacity = range.len() * close_prices.len();
                        let mut series: ListPrimitiveChunkedBuilder<Float32Type> =
                            ListPrimitiveChunkedBuilder::new(
                                name.clone(),
                                capacity,
                                values_capacity,
                                DataType::Float32,
                            );

                        for (idx, param) in range.iter().enumerate() {
                            let mut values = vec![];

                            for window in close_prices.windows(*param as usize) {
                                let alpha = alphas[idx];
                                let ewmas = ewma(window, alpha);
                                values.push(ewmas);
                            }
                            // let series = Series::from_iter(values);
                            // let data: Vec<Option<Vec<f32>>> =
                            //     vec![Some(vec![1.0, 2.0, 3.0]), None, Some(vec![4.0, 5.0, 6.0])];

                            // let mut test2 = FixedSizeListNumericBuilder.

                            // let result: ChunkedArray<ListType> = test.finish();
                            // let test2: PrimitiveChunkedBuilder<Float32Type> =
                            //     PrimitiveChunkedBuilder::new("testname".into(), 100_usize);
                            // let array = new_empty_array(ArrowDataType::Float32);
                            // // let chunk: ArrayRef = Box::new(array);
                            // // let fdpta = FixedSizeListArray::
                            // let test = ListPrimitiveChunkedBuilder::new(name, capacity, values_capacity, inner_type);
                            let test: Vec<Option<f32>> = vec![Some(1.0), None, Some(3.0)];
                            let test2 = <ChunkedArray<Float32Type> as NewChunkedArray<
                                Float32Type,
                                f32,
                            >>::from_slice_options(
                                "name".into(), &test
                            );
                            // let series = Series::from_chunk_and_dtype(
                            //     "test3".into(),
                            //     array,
                            //     &DataType::Array(Box::new(DataType::Float32), *param as usize),
                            // );
                            // array.
                            // unsafe {
                            //     let series = Series::from_chunks_and_dtype_unchecked(
                            //         self.name.into(),
                            //         vec![array],
                            //         &DataType::Float32,
                            //     );
                            // }
                        }

                        Ok(Some(Series::new_null(name.clone(), 0).into()))
                    },
                    &[],
                    output_type,
                )
                .alias(self.name),
        );

        todo!()
        //
        // let cols = self.get_indicators_columns(symbols_pair, params);
        // let (ema_fast_col, _) = cols
        //     .first()
        //     .expect("EMA indicator to have column at index 0");
        // let fast_span_param = params
        //     .get(&ParamId::FastSpan)
        //     .expect("FastSpan param to be set at ParamsMap");
        // let fast_span = if let Param::UInt32(value, _) = fast_span_param {
        //     *value
        // } else {
        //     20
        // };
        // let (ema_slow_col, _) = cols
        //     .get(1)
        //     .expect("EMA indicator to have column at index 1");
        // let slow_span_param = params
        //     .get(&ParamId::SlowSpan)
        //     .expect("SlowSpan param to be set at ParamsMap");
        // let slow_span = if let Param::UInt32(value, _) = slow_span_param {
        //     *value
        // } else {
        //     100
        // };
        //
        // let fast_alpha = calculate_span_alpha(fast_span as f64)?;
        // let slow_alpha = calculate_span_alpha(slow_span as f64)?;
        // let fast_opts = EWMOptions {
        //     alpha: fast_alpha,
        //     adjust: false,
        //     bias: false,
        //     min_periods: 1,
        //     ignore_nulls: false,
        // };
        // let slow_opts = EWMOptions {
        //     alpha: slow_alpha,
        //     adjust: false,
        //     bias: false,
        //     min_periods: 1,
        //     ignore_nulls: false,
        // };
        //
        // let lf = lf
        //     .with_columns([
        //         col(close_col).ewm_mean(slow_opts).alias(ema_slow_col),
        //         col(close_col).ewm_mean(fast_opts).alias(ema_fast_col),
        //     ]);
        //     // .with_column(
        //     //     when(col(ema_fast_col).is_null().or(col(ema_slow_col).is_null()))
        //     //         .then(lit(NULL))
        //     //         .otherwise(
        //     //             when(col(ema_fast_col).gt(col(ema_slow_col)))
        //     //                 .then(true)
        //     //                 .otherwise(false),
        //     //         )
        //     //         .alias(format!("{}_{}_{}", TREND_COL_PREFIX, slow_span, fast_span)),
        //     // );
        //
        // Ok(lf)
    }
}

pub fn calculate_span_alpha(span: f32) -> Result<f32, GlowError> {
    if span < 1.0 {
        panic!("Require 'span' >= 1 (found {})", span);
    }
    Ok(2.0 / (span + 1.0))
}

pub fn ewma(data: &[f32], alpha: f32) -> Option<Vec<f32>> {
    if data.is_empty() {
        return None;
    }

    let mut result = Vec::with_capacity(data.len());
    let mut prev = data[0];

    for (i, &value) in data.iter().enumerate() {
        if i == 0 {
            prev = value;
        } else {
            prev = alpha * value + (1.0 - alpha) * prev;
        }

        result.push(prev);
    }

    Some(result)
}

#[test]
fn test() {
    let mut test: ListPrimitiveChunkedBuilder<Float32Type> = ListPrimitiveChunkedBuilder::new(
        "testname2".into(),
        100_usize,
        100_usize,
        DataType::Float32,
    );
    let range = (0_u32..=150).map(|v| v as f32).collect::<Vec<f32>>();
    test.append_slice(&range);
    test.append_slice(&range);
    let result: ChunkedArray<ListType> = test.finish();
    let series: Series = result.into();

    let df = DataFrame::new(vec![series.into()]).unwrap();
    // let test = FixedSizeListType::
    // unsafe {
    //     let builder = fixed_size_list::FixedSizeListNumericBuilder::new(
    //         "test2".into(),
    //         100_usize,
    //         100_usize,
    //         DataType::Float32,
    //     );
    // }

    println!("stupid df {:?}", df);
}
