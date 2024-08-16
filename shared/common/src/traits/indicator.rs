use crate::structs::SymbolsPair;
use glow_error::GlowError;
use polars::prelude::*;

pub trait Indicator: Sized {
    type Params;
    type Wrapper;
    fn name(&self) -> &'static str;
    fn get_indicator_columns(&self) -> &Vec<(String, DataType)>;
    fn set_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, GlowError>;
    fn update_indicator_columns(&self, df: &DataFrame) -> Result<DataFrame, GlowError>;
    fn get_minimum_klines_for_benchmarking(&self) -> u32;
    fn patch_params(&self, params: Self::Params) -> Result<Self::Wrapper, GlowError>;
    fn patch_symbols_pair(&self, updated_symbols_pair: SymbolsPair) -> Result<Self::Wrapper, GlowError>;
}

// pub fn get_resampled_ohlc_window_data(
//     tick_data: &LazyFrame,
//     anchor_symbol: &String,
//     window_in_mins: &u32,
// ) -> Result<LazyFrame, GlowError> {
//     let end_time = col("start_time").last().alias("end_time");
//     let mut agg_expressions: Vec<Expr> = vec![end_time];
//     let (open_col, high_col, low_col, close_col) = get_symbol_ohlc_cols(anchor_symbol);
//     let (open_col_alias, high_col_alias, low_col_alias, close_col_alias) =
//         get_symbol_window_ohlc_cols(anchor_symbol, &window_in_mins.to_string());
//     let open = col(&open_col).first().alias(&open_col_alias);
//     let high = col(&high_col).max().alias(&high_col_alias);
//     let close = col(&close_col).last().alias(&close_col_alias);
//     let low = col(&low_col).min().alias(&low_col_alias);
//     let window_count = col("start_time").count().alias("window_count");

//     agg_expressions.push(open);
//     agg_expressions.push(high);
//     agg_expressions.push(close);
//     agg_expressions.push(low);
//     agg_expressions.push(window_count);

//     let window_in_nanos = *window_in_mins as i64 * SECONDS_IN_MIN * NANOS_IN_SECOND;

//     let resampled_data = tick_data
//         .clone()
//         .group_by_dynamic(
//             col("start_time"),
//             vec![],
//             DynamicGroupOptions {
//                 start_by: StartBy::DataPoint,
//                 index_column: "start_time".into(),
//                 every: Duration::new(window_in_nanos),
//                 period: Duration::new(window_in_nanos),
//                 offset: Duration::new(0),
//                 truncate: true,
//                 include_boundaries: false,
//                 closed_window: ClosedWindow::Left, // This should be left
//                 check_sorted: false,
//             },
//         )
//         .agg(agg_expressions);
//     Ok(resampled_data)
// }

// pub fn forward_fill_lf(
//     lf: LazyFrame,
//     from_mins: &u32,
//     to_mins: u32,
// ) -> Result<LazyFrame, GlowError> {
//     let period = to_mins as i64 * NANOS_IN_SECOND * SECONDS_IN_MIN;

//     let schema = lf.schema()?;
//     let schema = schema.as_ref();
//     let mut col_names: Vec<Expr> = vec![];
//     let mut aggs: Vec<Expr> = vec![];
//     let mut fill_null_cols = vec![];

//     for name in schema.iter_names() {
//         if name == "start_time" {
//             continue;
//         }
//         col_names.push(col(name));
//         aggs.push(col(name).mean());
//         fill_null_cols.push(col(name).forward_fill(Some(from_mins.clone())));
//     }

//     let result = lf
//         .group_by_dynamic(
//             col("start_time"),
//             vec![],
//             DynamicGroupOptions {
//                 start_by: StartBy::DataPoint,
//                 index_column: "start_time".into(),
//                 every: Duration::new(period),
//                 period: Duration::new(period),
//                 offset: Duration::new(0),
//                 truncate: true,
//                 include_boundaries: false,
//                 closed_window: ClosedWindow::Left, // MUST be LEFT otherwise, indicator columns get shifted by 1 period
//                 check_sorted: false,
//             },
//         )
//         .agg(aggs)
//         .with_columns(fill_null_cols);

//     Ok(result)
// }
