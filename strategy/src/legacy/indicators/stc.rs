use crate::functions::calculate_span_alpha;
use common::{
    functions::get_symbol_window_ohlc_cols,
    traits::indicator::{forward_fill_lf, get_resampled_ohlc_window_data, Indicator},
};
use glow_error::GlowError;
use polars::prelude::*;

#[derive(Clone)]
pub struct STCIndicator {
    pub name: String,
    pub anchor_symbol: String,
    pub windows: Vec<u32>,
    pub length: u32,
    pub short_span: u32, // fast_length, 12
    pub long_span: u32,  // slow length, 26
    pub weight: f64,
}

impl Indicator for STCIndicator {
    fn name(&self) -> String {
        self.name.clone()
    }
    fn get_indicator_columns(&self) -> Vec<(String, DataType)> {
        let mut columns_names = Vec::new();

        for window in &self.windows {
            // let ema_short_col_title = format!("{}_ema_s", &self.anchor_symbol);
            // let ema_long_col_title = format!("{}_ema_l", &self.anchor_symbol);
            let suffix = &format!("{}_{}", self.anchor_symbol, window);

            let stc_col = format!("STC_{}", suffix);
            let non_shifted_stc_col = format!("non_shifted_{}", stc_col);
            let non_shifted_stc_col_dtype = DataType::Float64;
            let stc_col_dtype = DataType::Float64;

            columns_names.push((non_shifted_stc_col, non_shifted_stc_col_dtype));
            columns_names.push((stc_col, stc_col_dtype));
        }
        // columns_names.push((self.trend_col.clone(), DataType::Int32));
        columns_names
    }

    fn get_data_offset(&self) -> u32 {
        let max_window = self.windows.iter().max();

        match max_window {
            Some(window) => (window * ((2 * self.length) + self.long_span)) + 1000,
            None => 0,
        }
    }

    fn set_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, GlowError> {
        let mut resampled_lfs = vec![];

        let lf_full_mins = lf.clone().select([col("start_time")]);

        for window in &self.windows {
            let resampled_data = get_resampled_ohlc_window_data(&lf, &self.anchor_symbol, window)?;
            let (_, _, _, close_col) =
                get_symbol_window_ohlc_cols(&self.anchor_symbol, &window.to_string());
            let suffix = &format!("{}_{}", self.anchor_symbol, window);

            // calculate MACD (ema long - ema short)

            let ema_short_col = &format!("ema_s_{}", suffix);
            let short_alpha = calculate_span_alpha(self.short_span as f64)?;
            let short_opts = EWMOptions {
                alpha: short_alpha,
                adjust: false,
                bias: false,
                min_periods: self.short_span as usize,
                ignore_nulls: true,
            };

            let ema_long_col = &format!("ema_l_{}", suffix);
            let long_alpha = calculate_span_alpha(self.long_span as f64)?;
            let long_opts = EWMOptions {
                alpha: long_alpha,
                adjust: false,
                bias: false,
                min_periods: self.long_span as usize,
                ignore_nulls: true,
            };

            let mut resampled_lf = resampled_data.with_columns([
                col(&close_col).ewm_mean(short_opts).alias(ema_short_col),
                col(&close_col).ewm_mean(long_opts).alias(ema_long_col),
            ]);
            // long span +

            let macd_col = &format!("macd_{}", suffix);

            resampled_lf =
                resampled_lf.with_column((col(ema_short_col) - col(ema_long_col)).alias(macd_col));

            let rolling_k_opts = RollingOptions {
                window_size: Duration::parse(&format!("{}i", self.length)),
                min_periods: self.length as usize,
                center: false,
                by: Some("start_time".to_string()),
                weights: None,
                closed_window: Some(ClosedWindow::Right),
                fn_params: None,
            };

            let macd_highest_col = &format!("macd_{}_highest", suffix);
            let macd_lowest_col = &format!("macd_{}_lowest", suffix);

            resampled_lf = resampled_lf.with_columns([
                col(&macd_col)
                    .rolling_max(rolling_k_opts.clone())
                    .alias(macd_highest_col),
                col(&macd_col)
                    .rolling_min(rolling_k_opts.clone())
                    .alias(macd_lowest_col),
            ]);
            // length +

            let macd_diff_col = &format!("macd_high_low_diff_{}", suffix);
            let macd_k_col = &format!("macd_K%_{}", suffix);

            resampled_lf = resampled_lf
                .with_column(
                    when(
                        col(macd_highest_col)
                            .is_null()
                            .or(col(macd_lowest_col).is_null()),
                    )
                    .then(lit(0))
                    .otherwise(col(macd_highest_col) - col(macd_lowest_col))
                    .alias(macd_diff_col),
                )
                .with_column(
                    when(col(macd_diff_col).gt(lit(0)))
                        .then(
                            lit(100)
                                * ((col(macd_col) - col(macd_lowest_col)) / col(macd_diff_col)),
                        )
                        .otherwise(lit(NULL))
                        .alias(macd_k_col),
                );

            let returns_output: SpecialEq<Arc<dyn FunctionOutputField>> =
                GetOutput::from_type(DataType::Float64);

            let weighted_macd_col = &format!("weighted_macd_{}", suffix);
            let weighted_macd_col_clone = weighted_macd_col.clone();
            let weight_clone = self.weight.clone();

            resampled_lf = resampled_lf.with_column(
                col(macd_k_col)
                    .apply_many(
                        move |series| {
                            let macd_k_series: &Series = &series[0];

                            let macd_k_series_vec = macd_k_series
                                .f64()
                                .unwrap()
                                .into_iter()
                                .collect::<Vec<Option<f64>>>();

                            let mut weighted_macd_k_series_vec: Vec<Option<f64>> = vec![];

                            for (index, macd_k) in macd_k_series_vec.iter().enumerate() {
                                let value;
                                if index == 0 {
                                    value = macd_k.clone();
                                } else {
                                    if weighted_macd_k_series_vec[index - 1].is_none() {
                                        value = macd_k_series_vec[index];
                                    } else {
                                        let prev_weighted_macd_k =
                                            weighted_macd_k_series_vec[index - 1].unwrap();
                                        value = Some(
                                            prev_weighted_macd_k
                                                + (weight_clone
                                                    * (macd_k.unwrap_or_default()
                                                        - prev_weighted_macd_k)),
                                        );
                                    }
                                }
                                weighted_macd_k_series_vec.push(value);
                            }

                            let series =
                                Series::new(&weighted_macd_col_clone, weighted_macd_k_series_vec);
                            Ok(Some(series))
                        },
                        &[],
                        returns_output,
                    )
                    .alias(weighted_macd_col),
            );

            let weighted_macd_highest_col = &format!("{}_highest", weighted_macd_col);
            let weighted_macd_lowest_col = &format!("{}_lowest", weighted_macd_col);

            resampled_lf = resampled_lf.with_columns([
                col(weighted_macd_col)
                    .rolling_max(rolling_k_opts.clone())
                    .alias(weighted_macd_highest_col),
                col(weighted_macd_col)
                    .rolling_min(rolling_k_opts.clone())
                    .alias(weighted_macd_lowest_col),
            ]);
            // length +

            let stochastic_weighted_macd_col = &format!("K%_{}", weighted_macd_col);

            resampled_lf = resampled_lf.with_column(
                when((col(weighted_macd_highest_col) - col(weighted_macd_lowest_col)).gt(lit(0)))
                    .then(
                        lit(100)
                            * ((col(weighted_macd_col) - col(weighted_macd_lowest_col))
                                / (col(weighted_macd_highest_col) - col(weighted_macd_lowest_col))),
                    )
                    .otherwise(col(weighted_macd_col).shift(1))
                    .alias(stochastic_weighted_macd_col),
            );

            let stc_col = &format!("STC_{}", suffix);
            let non_shifted_stc_col = &format!("non_shifted_{}", stc_col);
            let stc_col_clone = stc_col.clone();

            let returns_output: SpecialEq<Arc<dyn FunctionOutputField>> =
                GetOutput::from_type(DataType::Float64);

            resampled_lf = resampled_lf
                .with_column(
                    col(stochastic_weighted_macd_col)
                        .apply_many(
                            move |series| {
                                let stochastic_weighted_macd_series: &Series = &series[0];

                                let stochastic_weighted_macd_series_vec =
                                    stochastic_weighted_macd_series
                                        .f64()
                                        .unwrap()
                                        .into_iter()
                                        .collect::<Vec<Option<f64>>>();

                                let mut stc_series_vec: Vec<Option<f64>> = vec![];

                                for (index, weighted_macd_k) in
                                    stochastic_weighted_macd_series_vec.iter().enumerate()
                                {
                                    let value;
                                    if index == 0 {
                                        value = Some(weighted_macd_k.unwrap_or_default());
                                    } else {
                                        if stc_series_vec[index - 1].is_none() {
                                            value =
                                                stochastic_weighted_macd_series_vec[index].clone();
                                        } else {
                                            let prev_stc = stc_series_vec[index - 1].unwrap();
                                            value = Some(
                                                prev_stc
                                                    + (weight_clone
                                                        * (weighted_macd_k.unwrap_or_default()
                                                            - prev_stc)),
                                            );
                                        }
                                    }
                                    stc_series_vec.push(value);
                                }

                                let series = Series::new(&stc_col_clone, stc_series_vec);
                                Ok(Some(series))
                            },
                            &[],
                            returns_output,
                        )
                        .alias(non_shifted_stc_col),
                )
                .with_column(col(non_shifted_stc_col).shift(1).round(2).alias(stc_col));

            let log_df = resampled_lf.clone().collect()?;

            let path = "data/test".to_string();
            let file_name = format!("stc_log_resampled_window_{}.csv", window);
            // save_csv(path.clone(), file_name, &log_df, true)?;

            resampled_lf = resampled_lf.select(vec![
                col("start_time"),
                // col(ema_short_col),
                // col(ema_long_col),
                // col(macd_highest_col),
                // col(macd_lowest_col),
                // col(macd_k_col),
                // col(weighted_macd_col),
                // col(weighted_macd_highest_col),
                // col(weighted_macd_lowest_col),
                // col(stochastic_weighted_macd_col),
                col(non_shifted_stc_col),
                col(stc_col),
            ]);

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

            // let file_name = format!("stc_log_full_minutes_window_{}.csv", window);
            // save_csv(
            //     path.clone(),
            //     file_name,
            //     &resampled_lf_min.clone().collect()?,
            //     true,
            // )?;

            resampled_lf_min = resampled_lf_min.select(vec![
                col("start_time"),
                col(non_shifted_stc_col),
                col(stc_col),
            ]);

            resampled_lfs.push(resampled_lf_min);
        }

        let mut new_lf = lf_full_mins.clone();

        for resampled_lf in resampled_lfs {
            new_lf = new_lf.left_join(resampled_lf, "start_time", "start_time");
        }

        Ok(new_lf)
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
}
