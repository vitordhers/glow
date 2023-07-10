use polars::prelude::*;

use crate::trader::{
    functions::get_symbol_window_ohlc_cols,
    models::indicator::{forward_fill_lf, get_resampled_ohlc_window_data},
};

use super::{errors::Error, functions::get_symbol_ohlc_cols, models::indicator::Indicator};
pub struct StochasticIndicator {
    pub name: String,
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
}

impl Indicator for StochasticIndicator {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn get_indicator_columns(&self) -> Vec<String> {
        let mut columns_names = Vec::new();
        for window in &self.windows {
            let suffix = format!("{}_{}", self.anchor_symbol, window);

            let k_column = format!("K%_{}", suffix);
            let d_column = format!("D%_{}", suffix);
            columns_names.push(k_column);
            columns_names.push(d_column);
        }
        columns_names
    }

    fn compute_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, Error> {
        println!("stochastic fn");
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
                window_size: Duration::parse(&format!("{}i", k_window)),
                min_periods: k_window as usize,
                center: false,
                by: Some("start_time".to_string()),
                weights: None,
                closed_window: Some(ClosedWindow::Right),
            };

            let rolling_mean_d_opts = RollingOptions {
                window_size: Duration::parse(&format!("{}i", d_window)),
                min_periods: d_window as usize,
                center: false,
                by: Some("start_time".to_string()),
                weights: None,
                closed_window: Some(ClosedWindow::Right),
            };

            let resampled_lf = resampled_data
                .with_column(
                    ((lit(100)
                        * (col(&close_col) - col(&low_col).rolling_min(rolling_k_opts.clone()))
                        / (col(&high_col).rolling_max(rolling_k_opts.clone())
                            - col(&low_col).rolling_min(rolling_k_opts)))
                    .round(2))
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
                    },
                );

            let resampled_df_min = forward_fill_lf(resampled_lf_with_full_mins, window, 1)?;

            resampled_lfs.push(resampled_df_min);
        }

        let mut new_lf = lf_full_mins.clone();

        for resampled_lf in resampled_lfs {
            new_lf = new_lf.left_join(resampled_lf, "start_time", "start_time");
        }
        println!("stochastic fn end");
        Ok(new_lf)
    }

    fn clone_box(&self) -> Box<dyn Indicator + Send + Sync> {
        Box::new(Self {
            anchor_symbol: self.anchor_symbol.clone(),
            name: self.name.clone(),
            windows: self.windows.clone(),
        })
    }
}

pub struct StochasticThresholdIndicator {
    pub name: String,
    pub upper_threshold: i32,
    pub lower_threshold: i32,
    pub trend_col: String,
}

impl Indicator for StochasticThresholdIndicator {
    fn name(&self) -> String {
        self.name.clone()
    }
    fn get_indicator_columns(&self) -> Vec<String> {
        let mut columns_names = Vec::new();
        let long_threshold_col = "long_threshold";
        let short_threshold_col = "short_threshold";

        columns_names.push(String::from(long_threshold_col));
        columns_names.push(String::from(short_threshold_col));
        columns_names
    }
    fn compute_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, Error> {
        println!("stochastic threshold fn");
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
        println!("stochastic threshold fn end");
        Ok(lf)
    }

    fn clone_box(&self) -> Box<dyn Indicator + Send + Sync> {
        Box::new(Self {
            name: self.name.clone(),
            upper_threshold: self.upper_threshold.clone(),
            lower_threshold: self.lower_threshold.clone(),
            trend_col: self.trend_col.clone(),
        })
    }
}

pub struct ExponentialMovingAverageIndicator {
    pub name: String,
    pub anchor_symbol: String,
    pub long_span: usize,
    pub short_span: usize,
    pub trend_col: String,
}

impl Indicator for ExponentialMovingAverageIndicator {
    fn name(&self) -> String {
        self.name.clone()
    }
    fn get_indicator_columns(&self) -> Vec<String> {
        let mut columns_names = Vec::new();

        let ema_short_col = format!("{}_ema_s", &self.anchor_symbol);
        let ema_long_col = format!("{}_ema_l", &self.anchor_symbol);

        columns_names.push(ema_short_col);
        columns_names.push(ema_long_col);
        columns_names
    }
    fn compute_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, Error> {
        println!("ema fn");

        let long_span = self.long_span;
        let short_span = self.short_span;
        let (_, _, _, close_col) = get_symbol_ohlc_cols(&self.anchor_symbol);
        let ema_short_col = format!("{}_ema_s", &self.anchor_symbol);
        let ema_long_col = format!("{}_ema_l", &self.anchor_symbol);
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

        let lf = lf.with_columns([
            col(&close_col).ewm_mean(long_opts).alias(&ema_long_col),
            col(&close_col).ewm_mean(short_opts).alias(&ema_short_col),
        ]);
        let lf = lf
            .with_column(
                when(
                    col(&ema_short_col)
                        .is_null()
                        .or(col(&ema_long_col).is_null()),
                )
                .then(lit(NULL))
                .otherwise(
                    when(col(&ema_short_col).gt(col(&ema_long_col)))
                        .then(0)
                        .otherwise(1),
                )
                .alias(trend_col),
            )
            .select([
                col("start_time"),
                col(&self.trend_col),
                col(&ema_long_col),
                col(&ema_short_col),
            ]);

        Ok(lf)
    }

    fn clone_box(&self) -> Box<dyn Indicator + Send + Sync> {
        Box::new(Self {
            name: self.name.clone(),
            anchor_symbol: self.anchor_symbol.clone(),
            long_span: self.long_span.clone(),
            short_span: self.long_span.clone(),
            trend_col: self.trend_col.clone(),
        })
    }
}

fn calculate_span_alpha(span: f64) -> Result<f64, Error> {
    if span < 1.0 {
        panic!("Require 'span' >= 1 (found {})", span);
    }
    Ok(2.0 / (span + 1.0))
}
