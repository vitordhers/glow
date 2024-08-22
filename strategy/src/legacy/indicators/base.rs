mod functions;
use polars::prelude::*;

use crate::trader::functions::get_symbol_window_ohlc_cols;

use super::{
    errors::Error,
    functions::get_symbol_ohlc_cols,
    traits::indicator::{forward_fill_lf, get_resampled_ohlc_window_data, Indicator},
};

use functions::calculate_span_alpha;

#[derive(Clone)]
pub struct StochasticIndicator {
    pub name: String,
    pub windows: Vec<u32>,
    pub anchor_symbol: String,
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
                window_size: Duration::parse(&format!("{}i", k_window)),
                min_periods: k_window as usize,
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
                .with_columns([
                    when(col("window_count").lt(lit(*window)))
                        .then(col(&k_column).shift(1))
                        .otherwise(col(&k_column))
                        .alias(&k_column),
                    when(col("window_count").lt(lit(*window)))
                        .then(col(&d_column).shift(1))
                        .otherwise(col(&d_column))
                        .alias(&d_column),
                ])
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

            // let resampled_lf_min = forward_fill_lf(resampled_lf_with_full_mins, window, 1)?;

            // TODO: implement with with_columns_seq
            let resampled_lf_min = forward_fill_lf(resampled_lf_with_full_mins, window, 1)?;

            resampled_lfs.push(resampled_lf_min);
        }

        let mut new_lf = lf_full_mins.clone();

        for resampled_lf in resampled_lfs {
            new_lf = new_lf.left_join(resampled_lf, "start_time", "start_time");
        }
        let mut new_df = new_lf.collect()?;

        new_df = new_df.fill_null(FillNullStrategy::Forward(None))?;
        new_lf = new_df.lazy();

        Ok(new_lf)
    }

    fn update_indicator_columns(&self, df: &DataFrame) -> Result<DataFrame, Error> {
        let mut new_lf = df.clone().lazy();
        new_lf = self.set_indicator_columns(new_lf)?;
        let new_df = new_lf.collect()?;
        let mut result_df = df.clone();

        let last_index = new_df.height() - 1;

        for (column, _) in self.get_indicator_columns() {
            let last_value = new_df.column(&column)?.f64()?.to_vec();
            let last_value = last_value.get(last_index).unwrap();
            let mut current_values = result_df.column(&column)?.f64()?.to_vec();

            // this at least unorthodox techinique had to be used, as simply pushing last_value
            // to current_vales was pushing a None value for no apparent reason.
            current_values[last_index] = last_value.clone();

            let updated_series = Series::new(&column, current_values);
            let _ = result_df.replace(&column, updated_series.to_owned());
        }

        Ok(result_df)
    }

    fn clone_box(&self) -> Box<dyn Indicator + Send + Sync> {
        Box::new(Self {
            anchor_symbol: self.anchor_symbol.clone(),
            name: self.name.clone(),
            windows: self.windows.clone(),
        })
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

    fn clone_box(&self) -> Box<dyn Indicator + Send + Sync> {
        Box::new(Self {
            name: self.name.clone(),
            upper_threshold: self.upper_threshold.clone(),
            lower_threshold: self.lower_threshold.clone(),
            trend_col: self.trend_col.clone(),
        })
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

    fn clone_box(&self) -> Box<dyn Indicator + Send + Sync> {
        match self {
            Self::StochasticIndicator(stochastic_indicator) => stochastic_indicator.clone_box(),
            Self::StochasticThresholdIndicator(stochastic_threshold_indicator) => {
                stochastic_threshold_indicator.clone_box()
            }
            Self::ExponentialMovingAverageIndicator(ema_indicator) => ema_indicator.clone_box(),
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
