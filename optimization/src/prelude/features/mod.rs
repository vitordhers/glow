use super::combinable_params::{CombinableParam, FeatureGenerator, OptimizableParam};
use common::structs::SymbolsPair;
use glow_error::GlowError;
use polars::prelude::*;

pub struct EMAFeatureGenerator {
    pub name: &'static str,
    pub param: OptimizableParam<u8>,
    pub symbols: SymbolsPair,
}

impl EMAFeatureGenerator {
    pub fn new(name: &'static str, param: OptimizableParam<u8>, symbols: SymbolsPair) -> Self {
        Self {
            name,
            param,
            symbols,
        }
    }
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
        let output_type: SpecialEq<Arc<dyn FunctionOutputField>> =
            GetOutput::from_type(DataType::List(Box::new(DataType::Float32)));
        let range = self.param.range.clone();
        let name = PlSmallStr::from_static(self.name);
        let name_ref = self.name.clone();
        result_lf = result_lf.with_column(
            col(close_col)
                .apply_many(
                    move |columns| {
                        let close_prices = columns[0].f32()?;
                        let close_prices: Vec<f32> = close_prices.into_no_null_iter().collect();
                        let series_len = close_prices.len();
                        let values_capacity = range.len() * series_len;
                        let mut values = vec![];
                        for param in range.iter() {
                            let alpha =
                                calculate_span_alpha((*param).into()).expect("alpha to be valid");
                            let ewmas = ewma(&close_prices, alpha);
                            values.push(ewmas);
                        }

                        let mut ca_builder: ListPrimitiveChunkedBuilder<Float32Type> =
                            ListPrimitiveChunkedBuilder::new(
                                "".into(),
                                series_len,
                                values_capacity,
                                DataType::Float32,
                            );

                        let transposed_values = transpose(values);
                        (0..close_prices.len()).for_each(|row_idx| {
                            let mut col_values = vec![];
                            for (col_idx, param) in range.clone().iter().enumerate() {
                                col_values.push(transposed_values[row_idx][col_idx]);
                            }
                            ca_builder.append_opt_slice(Some(&col_values));
                        });
                        let chunked_array: ChunkedArray<ListType> = ca_builder.finish();
                        let list_series = chunked_array.into_series();
                        Ok(Some(list_series.into()))
                    },
                    &[],
                    output_type,
                )
                .alias(name),
        );
        Ok(result_lf)
    }
}

pub fn calculate_span_alpha(span: f32) -> Result<f32, GlowError> {
    if span < 1.0 {
        panic!("Require 'span' >= 1 (found {})", span);
    }
    Ok(2.0 / (span + 1.0))
}

pub fn ewma(data: &[f32], alpha: f32) -> Vec<f32> {
    if data.is_empty() {
        return vec![];
    }

    let mut result = Vec::with_capacity(data.len());
    let mut prev = data[0];
    result.push(prev);

    for &x in &data[1..] {
        let ewma = alpha * x + (1.0 - alpha) * prev;
        result.push(ewma);
        prev = ewma;
    }

    result
}

// pub fn transpose(matrix: Vec<Vec<f32>>) -> Vec<Vec<f32>> {
//     if matrix.is_empty() {
//         return vec![];
//     }
//
//     let rows = matrix.len();
//     let cols = matrix[0].len();
//
//     let mut transposed = vec![vec![0.0; rows]; cols];
//
//     for i in 0..rows {
//         for j in 0..cols {
//             transposed[j][i] = matrix[i][j];
//         }
//     }
//
//     transposed
// }

fn transpose(matrix: Vec<Vec<f32>>) -> Vec<Vec<f32>> {
    assert!(!matrix.is_empty(), "Matrix must have at least one row");
    let row_len = matrix[0].len();

    // Ensure all rows have the same length
    for row in &matrix {
        assert_eq!(row.len(), row_len, "All rows must have the same length");
    }

    let mut transposed = vec![Vec::with_capacity(matrix.len()); row_len];

    for row in matrix {
        for (j, val) in row.into_iter().enumerate() {
            transposed[j].push(val);
        }
    }

    transposed
}

// fn transpose<T: Clone>(v: Vec<Vec<T>>) -> Vec<Vec<T>> {
//     if v.is_empty() || v[0].is_empty() {
//         return vec![];
//     }
//
//     let rows = v.len();
//     let cols = v[0].len();
//
//     let mut transposed = vec![Vec::with_capacity(rows); cols];
//     for row in v {
//         for (j, item) in row.into_iter().enumerate() {
//             transposed[j].push(item);
//         }
//     }
//
//     transposed
// }

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

#[test]
fn test_ema_feature() -> Result<(), GlowError> {
    use common::functions::csv::load_csv;
    use std::path::PathBuf;

    let name = "ema_20";
    let param = OptimizableParam::new(name, 5, 1, 20);
    let symbols = SymbolsPair::default();
    let unique_symbols = symbols.get_unique_symbols();
    let file_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("data")
        .join("test_data.csv");
    // let file_path = "optimization/data/test_data.csv";
    let mut schema_fields = vec![Field::new(
        "start_time".into(),
        DataType::Datetime(TimeUnit::Milliseconds, None),
    )];
    for symbol in unique_symbols {
        let (open_col, high_col, low_col, close_col) = symbol.get_ohlc_cols();
        schema_fields.push(Field::new(open_col.into(), DataType::Float32));
        schema_fields.push(Field::new(high_col.into(), DataType::Float32));
        schema_fields.push(Field::new(low_col.into(), DataType::Float32));
        schema_fields.push(Field::new(close_col.into(), DataType::Float32));
    }
    let default_schema = Schema::from_iter(schema_fields.clone());
    let loaded_df = load_csv(file_path, &default_schema)?;
    println!("@@@ LOADED DF {:?}", loaded_df);
    let loaded_lf = loaded_df.lazy();
    let ema_feature = EMAFeatureGenerator::new(name, param, symbols);
    let test_lf = ema_feature.compute(&loaded_lf)?;
    let result_df = test_lf.collect()?;
    println!("@@@ RESULT DF {:?}", result_df);

    Ok(())
}

#[test]
fn test_windows() {
    let vector = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13];

    for window in vector.windows(3) {
        println!("THIS IS WINDOW {:?}", window);
    }
}
