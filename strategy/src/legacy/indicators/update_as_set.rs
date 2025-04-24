mod functions;
use chrono::{NaiveDateTime, Timelike};
use polars::prelude::*;

use super::{
    errors::Error,
    functions::{
        current_timestamp_ms, get_symbol_ohlc_cols, get_symbol_window_ohlc_cols,
        round_down_nth_decimal, timestamp_minute_start,
    },
    traits::indicator::{forward_fill_lf, get_resampled_ohlc_window_data, Indicator},
};

use functions::calculate_span_alpha;

#[derive(Clone)]
pub struct StochasticIndicator {
    pub name: String,
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
    pub starting_datetime: NaiveDateTime,
}

#[derive(Clone)]
pub struct StochasticThresholdIndicator {
    pub name: String,
    pub upper_threshold: i32,
    pub lower_threshold: i32,
    pub trend_col: String,
}

#[derive(Clone)]
pub struct ExponentialMovingAverageIndicator {
    pub name: String,
    pub anchor_symbol: String,
    pub long_span: usize,
    pub short_span: usize,
    pub trend_col: String,
}

impl Indicator for StochasticIndicator {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn get_indicator_columns(&self) -> Vec<(String, DataType)> {
        let mut columns_names = Vec::new();
        for window in &self.windows {
            let suffix = format!("{}_{}", self.anchor_symbol, window);

            let k_column_title = format!("K%_{}", suffix);
            let k_column_dtype = DataType::Float64;
            let d_column_title = format!("D%_{}", suffix);
            let d_column_dtype = DataType::Float64;
            columns_names.push((k_column_title, k_column_dtype));
            columns_names.push((d_column_title, d_column_dtype));
        }
        columns_names
    }

    fn set_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, Error> {
        let mut resampled_lfs = vec![];

        let lf_full_mins = lf.clone().select([col("start_time")]);

        for window in &self.windows {
            let resampled_data = get_resampled_ohlc_window_data(&lf, &self.anchor_symbol, window)?;
            let (_, high_col, close_col, low_col) =
                get_symbol_window_ohlc_cols(&self.anchor_symbol, &window.to_string());

            let suffix = format!("{}_{}", self.anchor_symbol, window);

            let k_column = format!("K%_{}", suffix);
            let d_column = format!("D%_{}", suffix);

            let k_window = 14;
            let d_window = 3;

            let rolling_k_opts = RollingOptions {
                window_size: Duration::parse(&format!("{}m", k_window)),
                min_periods: k_window as usize,
                center: false,
                by: Some("start_time".to_string()),
                weights: None,
                closed_window: Some(ClosedWindow::Right),
                fn_params: None,
            };

            let rolling_mean_d_opts = RollingOptions {
                window_size: Duration::parse(&format!("{}m", d_window)),
                min_periods: d_window as usize,
                center: false,
                by: Some("start_time".to_string()),
                weights: None,
                closed_window: Some(ClosedWindow::Right),
                fn_params: None,
            };

            let resampled_lf = resampled_data
                .with_column(
                    lit(100)
                        * (col(&close_col) - col(&low_col).rolling_min(rolling_k_opts.clone()))
                        / (col(&high_col).rolling_max(rolling_k_opts.clone())
                            - col(&low_col).rolling_min(rolling_k_opts))
                        .round(2)
                        .alias(&k_column),
                )
                // TODO: define forward fill with windows
                // .with_column(col(&k_column).forward_fill(None).keep_name())
                .with_column(
                    (col(&k_column).rolling_mean(rolling_mean_d_opts).round(2)).alias(&d_column),
                )
                .select(vec![col("start_time"), col(&k_column), col(&d_column)]);

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
            let full_mins_df = resampled_lf_with_full_mins
                .collect()?
                .fill_null(FillNullStrategy::Forward(Some(*window)))?;
            let resampled_lf_with_full_mins = full_mins_df.lazy();

            // let resampled_lf_min = forward_fill_lf(resampled_lf_with_full_mins, window, 1)?;

            // // TODO: implement with with_columns_seq
            // // let mut resampled_lf_min = forward_fill_lf(resampled_lf_with_full_mins, window, 1)?;
            // // resampled_lf_min = resampled_lf_min.with_columns([
            // //     when(col(&k_column).is_null())
            // //         .then(col(&k_column).drop_nulls().last())
            // //         .otherwise(col(&k_column)),
            // //     when(col(&d_column).is_null())
            // //         .then(col(&d_column).drop_nulls().last())
            // //         .otherwise(col(&d_column)),
            // // ]);

            resampled_lfs.push(resampled_lf_with_full_mins);
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
            
            // let last_index = df_height - 1;
            let last_index = df_height - 1 - window_usize; // hollywood

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
                last_k = k_col[k_col.len() - window_usize - 2]; // hollywood
                last_d = d_col[d_col.len() - window_usize - 2]; // hollywood
                // last_k = k_col[k_col.len() - 2];
                // last_d = d_col[d_col.len() - 2];
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
}

impl Indicator for StochasticThresholdIndicator {
    fn name(&self) -> String {
        self.name.clone()
    }
    fn get_indicator_columns(&self) -> Vec<(String, DataType)> {
        let mut columns_names = Vec::new();
        let long_threshold_col_title = "long_threshold".to_string();
        let long_threshold_col_dtype = DataType::Int32;

        let short_threshold_col_title = "short_threshold".to_string();
        let short_threshold_col_dtype = DataType::Int32;

        columns_names.push((long_threshold_col_title, long_threshold_col_dtype));
        columns_names.push((short_threshold_col_title, short_threshold_col_dtype));
        columns_names
    }
    fn set_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, Error> {
        let trend_col = &self.trend_col;
        let long_threshold_col = "long_threshold";
        let short_threshold_col = "short_threshold";
        let upper_threshold = self.upper_threshold;
        let lower_threshold = self.lower_threshold;
        let lf = lf.with_columns([
            when(col(trend_col).eq(1))
                .then(lit(upper_threshold))
                .otherwise(lit(lower_threshold))
                .alias(&short_threshold_col),
            when(col(trend_col).eq(1))
                .then(lit(lower_threshold))
                .otherwise(lit(upper_threshold))
                .alias(long_threshold_col),
        ]);
        let lf = lf.select([
            col("start_time"),
            col(long_threshold_col),
            col(short_threshold_col),
        ]);
        Ok(lf)
    }

    fn update_indicator_columns(&self, df: &DataFrame) -> Result<DataFrame, Error> {
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

impl Indicator for ExponentialMovingAverageIndicator {
    fn name(&self) -> String {
        self.name.clone()
    }
    fn get_indicator_columns(&self) -> Vec<(String, DataType)> {
        let mut columns_names = Vec::new();

        let ema_short_col_title = format!("{}_ema_s", &self.anchor_symbol);
        let ema_short_col_dtype = DataType::Float64;
        let ema_long_col_title = format!("{}_ema_l", &self.anchor_symbol);
        let ema_long_col_dtype = DataType::Float64;

        columns_names.push((ema_short_col_title, ema_short_col_dtype));
        columns_names.push((ema_long_col_title, ema_long_col_dtype));
        columns_names.push((self.trend_col.clone(), DataType::Int32));
        columns_names
    }

    fn set_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, Error> {
        let long_span = self.long_span;
        let short_span = self.short_span;
        let (_, _, _, close_col) = get_symbol_ohlc_cols(&self.anchor_symbol);
        let ema_short_col = &format!("{}_ema_s", &self.anchor_symbol);
        let ema_long_col = &format!("{}_ema_l", &self.anchor_symbol);
        let trend_col = &self.trend_col;

        let long_alpha = calculate_span_alpha(long_span as f64)?;

        let long_opts = EWMOptions {
            alpha: long_alpha,
            adjust: false,
            bias: false,
            min_periods: 1,
            ignore_nulls: false,
        };

        let short_alpha = calculate_span_alpha(short_span as f64)?;

        let short_opts = EWMOptions {
            alpha: short_alpha,
            adjust: false,
            bias: false,
            min_periods: 1,
            ignore_nulls: false,
        };

        let mut lf = lf
            .with_columns([
                col(&close_col).ewm_mean(long_opts).alias(ema_long_col),
                col(&close_col).ewm_mean(short_opts).alias(ema_short_col),
            ])
            .with_column(
                when(col(ema_short_col).is_null().or(col(ema_long_col).is_null()))
                    .then(lit(NULL))
                    .otherwise(
                        when(col(&ema_short_col).gt(col(ema_long_col)))
                            .then(1)
                            .otherwise(0),
                    )
                    .alias(trend_col),
            );

        lf = lf.select([
            col("start_time"),
            col(&self.trend_col),
            col(ema_long_col),
            col(ema_short_col),
        ]);

        Ok(lf)
    }

    fn update_indicator_columns(&self, df: &DataFrame) -> Result<DataFrame, Error> {
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

#[derive(Clone)]
pub enum IndicatorWrapper {
    StochasticIndicator(StochasticIndicator),
    StochasticThresholdIndicator(StochasticThresholdIndicator),
    ExponentialMovingAverageIndicator(ExponentialMovingAverageIndicator),
}

impl Indicator for IndicatorWrapper {
    fn name(&self) -> String {
        match self {
            Self::StochasticIndicator(stochastic_indicator) => stochastic_indicator.name(),
            Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
                stochastic_threshold_indicator.name()
            }
            Self::ExponentialMovingAverageIndicator(ema_indicator) => ema_indicator.name(),
        }
    }
    fn get_indicator_columns(&self) -> Vec<(String, DataType)> {
        match self {
            Self::StochasticIndicator(stochastic_indicator) => {
                stochastic_indicator.get_indicator_columns()
            }
            Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
                stochastic_threshold_indicator.get_indicator_columns()
            }
            Self::ExponentialMovingAverageIndicator(ema_indicator) => {
                ema_indicator.get_indicator_columns()
            }
        }
    }

    fn set_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, Error> {
        match self {
            Self::StochasticIndicator(stochastic_indicator) => {
                stochastic_indicator.set_indicator_columns(lf)
            }
            Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
                stochastic_threshold_indicator.set_indicator_columns(lf)
            }
            Self::ExponentialMovingAverageIndicator(ema_indicator) => {
                ema_indicator.set_indicator_columns(lf)
            }
        }
    }
    fn update_indicator_columns(&self, df: &DataFrame) -> Result<DataFrame, Error> {
        match self {
            Self::StochasticIndicator(stochastic_indicator) => {
                stochastic_indicator.update_indicator_columns(df)
            }
            Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
                stochastic_threshold_indicator.update_indicator_columns(df)
            }
            Self::ExponentialMovingAverageIndicator(ema_indicator) => {
                ema_indicator.update_indicator_columns(df)
            }
        }
    }
}

impl From<StochasticIndicator> for IndicatorWrapper {
    fn from(value: StochasticIndicator) -> Self {
        IndicatorWrapper::StochasticIndicator(value)
    }
}

impl From<ExponentialMovingAverageIndicator> for IndicatorWrapper {
    fn from(value: ExponentialMovingAverageIndicator) -> Self {
        IndicatorWrapper::ExponentialMovingAverageIndicator(value)
    }
}

impl From<StochasticThresholdIndicator> for IndicatorWrapper {
    fn from(value: StochasticThresholdIndicator) -> Self {
        IndicatorWrapper::StochasticThresholdIndicator(value)
    }
}

fn get_calculation_minutes(initial_minute: u32, window: &u32) -> Vec<u32> {
    let mut minutes = vec![initial_minute];

    let mut forward_minute = initial_minute;
    while forward_minute < 60 {
        forward_minute += window;
        if forward_minute < 60 {
            minutes.push(forward_minute);
        }
    }

    let mut backward_minute = initial_minute;
    while backward_minute > 0 {
        match backward_minute.checked_sub(*window) {
            Some(result) => {
                backward_minute = result;
                minutes.push(backward_minute);
            }
            None => {
                backward_minute = 0;
            }
        }
    }

    minutes.sort();

    minutes
}
