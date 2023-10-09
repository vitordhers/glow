use chrono::{NaiveDateTime, Timelike};
use polars::prelude::*;

use crate::trader::{
    errors::Error,
    functions::{get_symbol_ohlc_cols, get_symbol_window_ohlc_cols, round_down_nth_decimal},
    traits::indicator::{forward_fill_lf, get_resampled_ohlc_window_data, Indicator},
};

use super::functions::get_calculation_minutes;

#[derive(Clone)]
pub struct StochasticIndicator {
    pub name: String,
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
    pub starting_datetime: NaiveDateTime,
    pub k_window: u32,
    pub k_smooth: u32,
    pub d_smooth: u32,
}

impl Indicator for StochasticIndicator {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn get_indicator_columns(&self) -> Vec<(String, DataType)> {
        let mut columns_names = Vec::new();
        for window in &self.windows {
            let suffix = format!("{}_{}", self.anchor_symbol, window);

            let k_raw_column_title = format!("K%_raw_{}", suffix);

            let k_column_title = format!("K%_{}", suffix);
            let k_column_dtype = DataType::Float64;
            let k_non_shifted_column_title = format!("K%_{}_non_shifted", suffix);

            let d_column_title = format!("D%_{}", suffix);
            let d_column_dtype = DataType::Float64;
            let d_non_shifted_column_title = format!("D%_{}_non_shifted", suffix);

            columns_names.push((k_raw_column_title, k_column_dtype.clone()));
            columns_names.push((k_column_title, k_column_dtype.clone()));
            columns_names.push((k_non_shifted_column_title, k_column_dtype));
            columns_names.push((d_column_title, d_column_dtype.clone()));
            columns_names.push((d_non_shifted_column_title, d_column_dtype));
        }
        columns_names
    }

    fn set_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, Error> {
        let mut resampled_lfs = vec![];

        let lf_full_mins = lf.clone().select([col("start_time")]);

        for window in &self.windows {
            let resampled_data = get_resampled_ohlc_window_data(&lf, &self.anchor_symbol, window)?;
            let (_, high_col, low_col, close_col) =
                get_symbol_window_ohlc_cols(&self.anchor_symbol, &window.to_string());

            let suffix = format!("{}_{}", self.anchor_symbol, window);

            let k_raw_column = format!("K%_raw_{}", suffix);
            let k_non_shifted_column = format!("K%_{}_non_shifted", suffix);
            let k_column = format!("K%_{}", suffix);
            let d_non_shifted_column = format!("D%_{}_non_shifted", suffix);
            let d_column = format!("D%_{}", suffix);

            let k_window = self.k_window;
            let smooth_window = self.k_smooth;
            let d_window = self.d_smooth;

            let rolling_k_opts = RollingOptions {
                window_size: Duration::parse(&format!("{}i", k_window)),
                min_periods: k_window as usize,
                center: false,
                by: Some("start_time".to_string()),
                weights: None,
                closed_window: Some(ClosedWindow::Right),
                fn_params: None,
            };

            let rolling_smooth_opts = RollingOptions {
                window_size: Duration::parse(&format!("{}i", smooth_window)),
                min_periods: smooth_window as usize,
                center: false,
                by: Some("start_time".to_string()),
                weights: None,
                closed_window: Some(ClosedWindow::Right),
                fn_params: None,
            };

            let rolling_mean_d_opts = RollingOptions {
                window_size: Duration::parse(&format!("{}i", d_window)),
                min_periods: d_window as usize,
                center: false,
                by: Some("start_time".to_string()),
                weights: None,
                closed_window: Some(ClosedWindow::Right),
                fn_params: None,
            };

            let lowest_col = &format!("rolling_{}_lowest_{}", k_window, window);
            let highest_col = &format!("rolling_{}_highest_{}", k_window, window);

            let resampled_lf = resampled_data
                .with_columns([
                    col(&high_col)
                        .rolling_max(rolling_k_opts.clone())
                        .alias(highest_col),
                    col(&low_col)
                        .rolling_min(rolling_k_opts.clone())
                        .alias(lowest_col),
                ])
                .with_column(
                    (lit(100) * (col(&close_col) - col(lowest_col))
                        / (col(highest_col) - col(lowest_col)))
                    .alias(&k_raw_column),
                )
                .with_column(
                    (col(&k_raw_column).rolling_mean(rolling_smooth_opts))
                        .alias(&k_non_shifted_column),
                )
                .with_column(
                    col(&k_non_shifted_column)
                        .shift(1)
                        .round(2)
                        .alias(&k_column),
                )
                .with_columns([
                    (col(&k_column)
                        .rolling_mean(rolling_mean_d_opts.clone())
                        .round(2))
                    .alias(&d_column),
                    (col(&k_non_shifted_column).rolling_mean(rolling_mean_d_opts))
                        .alias(&d_non_shifted_column),
                ]);

            // let log_df = resampled_lf.clone().collect()?;

            // let path = "data/test".to_string();
            // let file_name = format!("log_resampled_window_{}.csv", window);
            // save_csv(path.clone(), file_name, &log_df, true)?;

            // resampled_lf = resampled_lf.select(vec![
            //     col("start_time"),
            //     col(&k_column),
            //     col(&k_non_shifted_column),
            //     col(&d_column),
            //     col(&d_non_shifted_column),
            // ]);

            let resampled_lf_with_full_mins = lf_full_mins
                .clone()
                .left_join(resampled_lf, "start_time", "start_time")
                .sort(
                    "start_time",
                    SortOptions {
                        descending: false,
                        nulls_last: false,
                        multithreaded: true,
                        maintain_order: true,
                    },
                );

            let mut resampled_lf_min = forward_fill_lf(resampled_lf_with_full_mins, window, 1)?;

            // let file_name = format!("log_full_minutes_window_{}.csv", window);
            // save_csv(
            //     path.clone(),
            //     file_name,
            //     &resampled_lf_min.clone().collect()?,
            //     true,
            // )?;

            resampled_lf_min = resampled_lf_min.select(vec![
                col("start_time"),
                // col(&k_raw_column),
                col(&k_non_shifted_column),
                col(&k_column),
                col(&d_non_shifted_column),
                col(&d_column),
            ]);

            resampled_lfs.push(resampled_lf_min);
        }

        let mut new_lf = lf_full_mins.clone();

        for resampled_lf in resampled_lfs {
            new_lf = new_lf.left_join(resampled_lf, "start_time", "start_time");
        }

        Ok(new_lf)
    }

    fn update_indicator_columns(&self, df: &DataFrame) -> Result<DataFrame, Error> {
        let mut result_df = df.clone();
        let initial_minute = self.starting_datetime.minute();
        // println!("@@@@ initial_minute {:?}", initial_minute);

        let smooth_param = 5;
        let d_param = 4;

        // let start_time = last_minute;

        let (_, high_col, low_col, close_col) = get_symbol_ohlc_cols(&self.anchor_symbol);

        for window in &self.windows {
            let window_usize = window.clone() as usize;
            let current_minute = df.column("start_time")?.datetime()?.last();

            let last_minute_start_ts = df
                .column("start_time")?
                .datetime()?
                .shift(*window as i64)
                .last();
            let last_minute_start_ts = last_minute_start_ts.unwrap();
            let last_start_time = NaiveDateTime::from_timestamp_millis(last_minute_start_ts);
            // println!("stupid trading view urges that at minute {:?}, the fucking minute {:?} should be calculated",
            //     NaiveDateTime::from_timestamp_millis(current_minute.unwrap()).unwrap(),
            //     last_start_time.unwrap()
            // );

            let last_minute = last_start_time.unwrap().minute();

            let calculation_minutes = get_calculation_minutes(initial_minute, window);
            let suffix = format!("{}_{}", self.anchor_symbol, window);

            let k_column = format!("K%_{}", suffix);
            let d_column = format!("D%_{}", suffix);

            let mut k_col = df.column(&k_column)?.f64()?.to_vec();
            let mut d_col = df.column(&d_column)?.f64()?.to_vec();
            let last_k;
            let last_d;

            let df_height = result_df.height();

            let last_index = df_height - 1;
            // let last_index = df_height - 1 - window_usize; // hollywood

            if calculation_minutes.contains(&last_minute) {
                let k_timeframe = 14 * window;

                if ((last_index + 1) as u32) < k_timeframe {
                    last_k = None;
                    last_d = None;
                } else {
                    let tailed_df = df.tail(Some(k_timeframe as usize));

                    let last_close = tailed_df.column(&close_col)?.f64()?.last().unwrap();

                    let timeframe_lowest = tailed_df.column(&low_col)?.f64()?.min().unwrap();

                    let timeframe_highest = tailed_df.column(&high_col)?.f64()?.max().unwrap();

                    last_k = Some(round_down_nth_decimal(
                        (last_close - timeframe_lowest) / (timeframe_highest - timeframe_lowest)
                            * 100.00,
                        2,
                    ));

                    let k_col = df.column(&k_column)?.f64()?.to_vec();

                    // let mut prev_values = vec![raw_k.unwrap()];

                    // for smooth in 1..=smooth_param {
                    //     let prev_val = k_col[k_col.len() - (*window * smooth) as usize]
                    //         .unwrap_or(raw_k.unwrap());
                    //     prev_values.push(prev_val);
                    // }

                    // last_k = Some(round_down_nth_decimal(
                    //     prev_values.iter().sum::<f64>() / smooth_param as f64,
                    //     2,
                    // ));

                    // let mut prev_values = vec![last_k.unwrap()];

                    // for smooth in 1..=d_param {
                    //     let prev_val = k_col[k_col.len() - (*window * smooth) as usize]
                    //         .unwrap_or(last_k.unwrap());
                    //     prev_values.push(prev_val);
                    // }

                    // last_d = Some(round_down_nth_decimal(
                    //     prev_values.iter().sum::<f64>() / d_param as f64,
                    //     2,
                    // ));

                    let prev_val_1 =
                        k_col[k_col.len() - *window as usize].unwrap_or(last_k.unwrap());
                    let prev_val_2 =
                        k_col[k_col.len() - ((*window * 2) as usize)].unwrap_or(last_k.unwrap());

                    last_d = Some(round_down_nth_decimal(
                        (prev_val_1 + prev_val_2 + last_k.unwrap()) / 3.0,
                        2,
                    ));

                    // if window == &3 {
                    //     println!(
                    //         "{:?} | window = {} highest = {:?}, close = {:?}, lowest = {:?}, last_k = {:?}, last_d = {:?}",
                    //         last_start_time.unwrap(),
                    //         window,
                    //         timeframe_highest,
                    //         last_close,
                    //         timeframe_lowest,
                    //         last_k.unwrap(),
                    //         last_d.unwrap()
                    //     );
                    // }
                }
            } else {
                // penultimate index
                // last_k = k_col[k_col.len() - window_usize - 2]; // hollywood
                // last_d = d_col[d_col.len() - window_usize - 2]; // hollywood
                last_k = k_col[k_col.len() - 2];
                last_d = d_col[d_col.len() - 2];
            }

            k_col[last_index] = last_k;
            d_col[last_index] = last_d;

            let updated_k_series = Series::new(&k_column, k_col);
            result_df.replace(&k_column, updated_k_series)?;
            let updated_d_series = Series::new(&d_column, d_col);
            result_df.replace(&d_column, updated_d_series)?;
        }

        Ok(result_df)
    }

    fn get_data_offset(&self) -> u32 {
        let max_window = self.windows.iter().max();

        match max_window {
            Some(window) => {
                (window * self.k_window) // 14 * 15 = 
                    + (window * (self.k_smooth - 1))
                    + (window * (self.d_smooth - 1))
            }
            None => 0,
        }
    }
}
