use chrono::DateTime;
use common::{
    functions::{count_decimal_places, get_symbol_close_col},
    structs::Contract,
    traits::indicator::Indicator,
};
use glow_error::GlowError;
use polars::prelude::*;

#[derive(Clone)]
pub struct FastSlowCloudRMAs {
    pub name: String,
    pub contract: Contract,
    pub slow_period: u32,
    pub fast_period: u32,
    pub cloud_length: u32,
    pub momentum_diff_span: u32,
}

impl Indicator for FastSlowCloudRMAs {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn get_indicator_columns(&self) -> Vec<(String, DataType)> {
        let mut columns_names = Vec::new();
        let anchor_symbol = &self.contract.symbol;

        let rma_fast_col = format!("{}_rma_fast", anchor_symbol);
        let rma_slow_col = format!("{}_rma_slow", anchor_symbol);
        let cloud_top_col = format!("{}_{}_cloud_top", anchor_symbol, &self.cloud_length);
        let cloud_base_col = format!("{}_{}_cloud_base", anchor_symbol, &self.cloud_length);
        let trend_diff_col = "trend_diff".to_string();
        let close_fast_diff_col = "close_fast_diff".to_string();
        let close_slow_diff_col = "close_slow_diff".to_string();
        let trend_col = "trend".to_string();

        let float_dtype = DataType::Float64;
        let utf8_dtype = DataType::Utf8;

        columns_names.push((rma_fast_col, float_dtype.clone()));
        columns_names.push((rma_slow_col, float_dtype.clone()));
        columns_names.push((cloud_top_col, float_dtype.clone()));
        columns_names.push((cloud_base_col, float_dtype.clone()));
        columns_names.push((trend_diff_col, float_dtype.clone()));
        columns_names.push((close_fast_diff_col, float_dtype.clone()));
        columns_names.push((close_slow_diff_col, float_dtype.clone()));
        columns_names.push((trend_col, utf8_dtype));
        columns_names
    }

    fn set_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, GlowError> {
        let anchor_symbol = &self.contract.symbol;
        let contract_decimals = count_decimal_places(self.contract.tick_size) as u32;
        let close_col = &get_symbol_close_col(anchor_symbol);

        // pine_rma(src, length) =>
        //      alpha = 1/length
        //      sum = 0.0
        //      sum := na(sum[1]) ? ta.sma(src, length) : alpha * src + (1 - alpha) * nz(sum[1])

        let rma_fast_col = &format!("{}_rma_fast", anchor_symbol);
        let fast_alpha = 1.0 / self.fast_period as f64;

        let returns_output: SpecialEq<Arc<dyn FunctionOutputField>> =
            GetOutput::from_type(DataType::Float64);

        let mut lf = lf.with_columns_seq([col(close_col)
            .apply_many(calculate_rma(rma_fast_col, fast_alpha), &[], returns_output)
            .round(contract_decimals)
            .alias(rma_fast_col)]);

        let rma_slow_col = &format!("{}_rma_slow", anchor_symbol);
        let slow_alpha = 1.0 / self.slow_period as f64;

        let returns_output: SpecialEq<Arc<dyn FunctionOutputField>> =
            GetOutput::from_type(DataType::Float64);

        lf = lf.with_columns_seq([col(close_col)
            .apply_many(calculate_rma(rma_slow_col, slow_alpha), &[], returns_output)
            .round(contract_decimals)
            .alias(rma_slow_col)]);

        lf = lf.with_column(
            when(
                col(rma_slow_col)
                    .lt(col(rma_fast_col))
                    .and(col(rma_fast_col).lt(col(close_col))),
            )
            .then(lit("strong_bull"))
            .when(
                col(rma_slow_col)
                    .lt(col(close_col))
                    .and(col(close_col).lt(col(rma_fast_col))),
            )
            .then(lit("slight_bull"))
            .when(
                col(close_col)
                    .lt(col(rma_slow_col))
                    .and(col(rma_slow_col).lt(col(rma_fast_col))),
            )
            .then(lit("revert_bull"))
            .when(
                col(close_col)
                    .lt(col(rma_fast_col))
                    .and(col(rma_fast_col).lt(col(rma_slow_col))),
            )
            .then(lit("strong_bear"))
            .when(
                col(rma_fast_col)
                    .lt(col(close_col))
                    .and(col(close_col).lt(col(rma_slow_col))),
            )
            .then(lit("slight_bear"))
            .when(
                col(rma_fast_col)
                    .lt(col(rma_slow_col))
                    .and(col(rma_slow_col).lt(col(close_col))),
            )
            .then(lit("revert_bear"))
            .otherwise(lit("undefined"))
            .alias("trend"),
        );

        let lag = self.momentum_diff_span as i64;

        let trend_lag0 = &format!("{}_trend_{}", anchor_symbol, 1 * lag);
        let trend_lag1 = &format!("{}_trend_{}", anchor_symbol, 2 * lag);
        let trend_lag2 = &format!("{}_trend_{}", anchor_symbol, 3 * lag);
        let trend_lag3 = &format!("{}_trend_{}", anchor_symbol, 4 * lag);

        let trend_diff_col = "trend_diff";
        let close_fast_diff_col = "close_fast_diff";
        let close_slow_diff_col = "close_slow_diff";

        let momentum_decimals = contract_decimals + 2;

        let returns_output: SpecialEq<Arc<dyn FunctionOutputField>> =
            GetOutput::from_type(DataType::Int32);

        let offset = self.get_data_offset();

        let highest_in_mins_col = "highest_in_mins";

        lf = lf.with_column(
            col(close_col)
                .apply_many(
                    move |series: &mut [Series]| {
                        let close_series: &Series = &series[0];
                        let start_time_series = &series[1];

                        let close_series_vec = close_series
                            .f64()
                            .unwrap()
                            .into_iter()
                            .collect::<Vec<Option<f64>>>();

                        let start_time_vec = start_time_series
                            .datetime()
                            .unwrap()
                            .into_iter()
                            .collect::<Vec<Option<i64>>>();

                        let mut highest_in_mins_vec: Vec<Option<i64>> = vec![];

                        for (current_index, current_close) in close_series_vec.iter().enumerate() {
                            let minutes: Option<i64>;
                            let close = current_close.expect("MISSING STUPID CURRENT CLOSE");
                            if current_index == 0 {
                                minutes = None;
                            } else {
                                // slice vector for fetching previous closes
                                let sliced_closes = &close_series_vec[0..current_index];
                                let mut index_from_higher_val = None;
                                for (index, &value) in sliced_closes.iter().enumerate().rev() {
                                    let prev_val = value.expect("STUPID VALUE MISSING");
                                    if prev_val >= close {
                                        index_from_higher_val = Some(index);
                                        break;
                                    }
                                }
                                if index_from_higher_val.is_none() {
                                    minutes = Some(offset.into());
                                } else {
                                    let index_from_higher_val = index_from_higher_val.unwrap();
                                    let former_high_ts =
                                        start_time_vec[index_from_higher_val].unwrap();
                                    let current_ts = start_time_vec[current_index].unwrap();
                                    let current_datetime =
                                        DateTime::from_timestamp(current_ts / 1000, 0).unwrap();
                                    let former_high_datetime =
                                        DateTime::from_timestamp(former_high_ts / 1000, 0).unwrap();

                                    let minutes_diff =
                                        (current_datetime - former_high_datetime).num_minutes();
                                    minutes = Some(minutes_diff);
                                }
                            }
                            highest_in_mins_vec.push(minutes);
                        }

                        let series = Series::new(highest_in_mins_col, highest_in_mins_vec);
                        Ok(Some(series))
                    },
                    &[col("start_time")],
                    returns_output,
                )
                .alias(highest_in_mins_col),
        );

        let returns_output: SpecialEq<Arc<dyn FunctionOutputField>> =
            GetOutput::from_type(DataType::Int32);

        let lowest_in_mins_col = "lowest_in_mins";

        lf = lf.with_column(
            col(close_col)
                .apply_many(
                    move |series: &mut [Series]| {
                        let close_series: &Series = &series[0];
                        let start_time_series = &series[1];

                        let close_series_vec = close_series
                            .f64()
                            .unwrap()
                            .into_iter()
                            .collect::<Vec<Option<f64>>>();

                        let start_time_vec = start_time_series
                            .datetime()
                            .unwrap()
                            .into_iter()
                            .collect::<Vec<Option<i64>>>();

                        let mut lowest_in_mins_vec: Vec<Option<i64>> = vec![];

                        for (current_index, current_close) in close_series_vec.iter().enumerate() {
                            let minutes: Option<i64>;
                            let close = current_close.expect("MISSING STUPID CURRENT CLOSE");
                            if current_index == 0 {
                                minutes = None;
                            } else {
                                // slice vector for fetching previous closes
                                let sliced_closes = &close_series_vec[0..current_index];
                                let mut index_from_lower_val = None;
                                for (index, &value) in sliced_closes.iter().enumerate().rev() {
                                    let prev_val = value.expect("STUPID VALUE MISSING");
                                    if prev_val <= close {
                                        index_from_lower_val = Some(index);
                                        break;
                                    }
                                }
                                if index_from_lower_val.is_none() {
                                    minutes = Some(offset.into());
                                } else {
                                    let index_from_lower_val = index_from_lower_val.unwrap();
                                    let former_low_ts =
                                        start_time_vec[index_from_lower_val].unwrap();
                                    let current_ts = start_time_vec[current_index].unwrap();
                                    let current_datetime =
                                        DateTime::from_timestamp(current_ts / 1000, 0).unwrap();
                                    let former_low_datetime =
                                        DateTime::from_timestamp(former_low_ts / 1000, 0).unwrap();

                                    let minutes_diff =
                                        (current_datetime - former_low_datetime).num_minutes();
                                    minutes = Some(minutes_diff);
                                }
                            }
                            lowest_in_mins_vec.push(minutes);
                        }

                        let series = Series::new(lowest_in_mins_col, lowest_in_mins_vec);
                        Ok(Some(series))
                    },
                    &[col("start_time")],
                    returns_output,
                )
                .alias(lowest_in_mins_col),
        );

        lf = lf
            .with_column(
                ((col(rma_fast_col) - col(rma_slow_col)) / col(rma_slow_col))
                    .round(momentum_decimals)
                    .alias(trend_diff_col),
            )
            .with_columns([
                ((col(close_col) - col(rma_fast_col)) / col(rma_fast_col))
                    .round(momentum_decimals)
                    .alias(close_fast_diff_col),
                ((col(close_col) - col(rma_slow_col)) / col(rma_slow_col))
                    .round(momentum_decimals)
                    .alias(close_slow_diff_col),
            ]);

        // let trend_diff_mg = "trend_diff_mg";

        // lf = lf.with_column(
        //     when(col(trend_diff_col).shift(1).is_null())
        //         .then(lit(NULL))
        //         .otherwise(col(trend_diff_col) - col(trend_diff_col).shift(1))
        //         .alias(trend_diff_mg),
        // );

        let rolling_opts = RollingOptions {
            window_size: Duration::parse(&format!("{}m", self.momentum_diff_span)),
            min_periods: self.momentum_diff_span as usize,
            weights: None,
            center: false,
            by: Some("start_time".to_string()),
            fn_params: None,
            closed_window: Some(ClosedWindow::Right),
        };

        let returns_output: SpecialEq<Arc<dyn FunctionOutputField>> =
            GetOutput::from_type(DataType::Utf8);

        let trends_col = "trends";
        let trends_col_clone = trends_col.clone();

        lf = lf
            .with_column(
                when(col(trend_diff_col).is_null())
                    .then(lit(NULL))
                    .otherwise(col(trend_diff_col).rolling_sum(rolling_opts.clone()))
                    .round(momentum_decimals)
                    .alias(trend_lag0),
            )
            .with_column(
                when(col(trend_lag0).shift(lag).is_null())
                    .then(lit(NULL))
                    .otherwise(col(trend_lag0).shift(lag))
                    .alias(trend_lag1),
            )
            .with_column(
                when(col(trend_lag1).shift(lag).is_null())
                    .then(lit(NULL))
                    .otherwise(col(trend_lag1).shift(lag))
                    .alias(trend_lag2),
            )
            .with_column(
                when(col(trend_lag2).shift(lag).is_null())
                    .then(lit(NULL))
                    .otherwise(col(trend_lag2).shift(lag))
                    .alias(trend_lag3),
            )
            .with_column(
                col(trend_lag0)
                    .apply_many(
                        move |series: &mut [Series]| {
                            let trend_lag_0_series: &Series = &series[0];
                            let trend_lag_1_series: &Series = &series[1];
                            let trend_lag_2_series: &Series = &series[2];
                            let trend_lag_3_series: &Series = &series[3];

                            let trend_0_vec = trend_lag_0_series
                                .f64()
                                .unwrap()
                                .into_iter()
                                .collect::<Vec<Option<f64>>>();

                            let trend_1_vec = trend_lag_1_series
                                .f64()
                                .unwrap()
                                .into_iter()
                                .collect::<Vec<Option<f64>>>();

                            let trend_2_vec = trend_lag_2_series
                                .f64()
                                .unwrap()
                                .into_iter()
                                .collect::<Vec<Option<f64>>>();

                            let trend_3_vec = trend_lag_3_series
                                .f64()
                                .unwrap()
                                .into_iter()
                                .collect::<Vec<Option<f64>>>();

                            let mut trend_seq_vec: Vec<Option<String>> = vec![];

                            for (index, _) in trend_3_vec.iter().enumerate() {
                                let mut value = None;
                                if index == 0 {
                                    trend_seq_vec.push(value);
                                    continue;
                                }
                                let current_trend_0 = trend_0_vec[index];
                                let current_trend_1 = trend_1_vec[index];
                                let current_trend_2 = trend_2_vec[index];
                                let current_trend_3 = trend_3_vec[index];

                                if current_trend_0.is_none()
                                    | current_trend_1.is_none()
                                    | current_trend_2.is_none()
                                    | current_trend_3.is_none()
                                {
                                    trend_seq_vec.push(value);
                                } else {
                                    let current_trend_0 = trend_0_vec[index].unwrap();
                                    let current_trend_1 = trend_1_vec[index].unwrap();
                                    let current_trend_2 = trend_2_vec[index].unwrap();
                                    let current_trend_3 = trend_3_vec[index].unwrap();

                                    let numbers = vec![
                                        current_trend_3,
                                        current_trend_2,
                                        current_trend_1,
                                        current_trend_0,
                                    ];

                                    let mut indexed_numbers: Vec<(f64, usize)> = numbers
                                        .iter()
                                        .enumerate()
                                        .map(|(i, &num)| (num, i))
                                        .collect();

                                    // Sort the vector by the numbers in ascending order
                                    indexed_numbers.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

                                    // Extract the original indexes to create the result string
                                    let result: String = indexed_numbers
                                        .iter()
                                        .map(|(_, i)| (i + 1).to_string())
                                        .collect();

                                    value = Some(result);

                                    trend_seq_vec.push(value);
                                }
                            }

                            let series = Series::new(&trends_col_clone, trend_seq_vec);
                            Ok(Some(series))
                        },
                        &[col(trend_lag1), col(trend_lag2), col(trend_lag3)],
                        returns_output,
                    )
                    .alias(trends_col),
            );

        let cloud_rolling_ops = RollingOptions {
            window_size: Duration::parse(&format!("{}m", self.cloud_length)),
            min_periods: self.cloud_length as usize,
            weights: None,
            center: false,
            by: Some("start_time".to_string()),
            closed_window: Some(ClosedWindow::Right),
            fn_params: None,
        };

        let cloud_top = &format!("{}_{}_cloud_top", anchor_symbol, &self.cloud_length);
        let cloud_base = &format!("{}_{}_cloud_base", anchor_symbol, &self.cloud_length);

        lf = lf.with_columns([
            col(close_col)
                .rolling_max(cloud_rolling_ops.clone())
                .alias(cloud_top),
            col(close_col)
                .rolling_min(cloud_rolling_ops)
                .alias(cloud_base),
        ]);

        // let path = "data/test".to_string();
        // let file_name = format!("fast_slow_mas.csv");
        // save_csv(path.clone(), file_name, &lf.clone().collect()?, true)?;

        lf = lf.select([
            col("start_time"),
            col(rma_fast_col),
            col(rma_slow_col),
            col(cloud_top),
            col(cloud_base),
            col("trend"),
            col(trend_diff_col),
            col(close_fast_diff_col),
            col(close_slow_diff_col),
            col(trend_lag3),
            col(trend_lag2),
            col(trend_lag1),
            col(trend_lag0),
            col(trends_col),
            col(highest_in_mins_col),
            col(lowest_in_mins_col),
        ]);

        Ok(lf)
    }

    fn update_indicator_columns(&self, df: &DataFrame) -> Result<DataFrame, GlowError> {
        let mut new_lf = df.clone().lazy();
        new_lf = self.set_indicator_columns(new_lf)?;
        let new_df = new_lf.collect()?;
        let mut result_df = df.clone();

        for (column, _) in self.get_indicator_columns() {
            let series = new_df.column(&column)?;
            let _ = result_df.replace(&column, series.to_owned());
        }

        Ok(result_df)
    }

    fn get_data_offset(&self) -> u32 {
        self.slow_period + 2000
    }
}

fn calculate_rma(
    rma_col: &String,
    alpha: f64,
) -> Box<dyn Fn(&mut [Series]) -> Result<Option<Series>, PolarsError> + 'static + Send + Sync> {
    let rma_col_clone = rma_col.clone();

    let closure = move |series: &mut [Series]| {
        let close_series: &Series = &series[0];

        let close_series_vec = close_series
            .f64()
            .unwrap()
            .into_iter()
            .collect::<Vec<Option<f64>>>();

        let mut rma_series_vec: Vec<Option<f64>> = vec![];

        for (index, current_close) in close_series_vec.iter().enumerate() {
            let value;
            if index == 0 {
                value = current_close.clone();
            } else {
                let last_rma = rma_series_vec[index - 1];
                if last_rma.is_none() {
                    let slice = &close_series_vec[1..=index];
                    let average: f64 =
                        slice.iter().map(|v| v.unwrap()).sum::<f64>() / slice.len() as f64;
                    value = Some(average);
                } else {
                    let last_rma = last_rma.unwrap_or_default();
                    let current_close = current_close.unwrap();
                    let first_close = alpha * current_close;
                    value = Some(first_close + ((1.0 - alpha) * last_rma));
                }
            }
            rma_series_vec.push(value);
        }

        let series = Series::new(&rma_col_clone, rma_series_vec);
        Ok(Some(series))
    };

    Box::new(closure)
}
