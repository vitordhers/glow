use crate::{
    shared::csv::save_csv,
    trader::{
        errors::Error,
        functions::get_symbol_window_ohlc_cols,
        traits::indicator::{forward_fill_lf, get_resampled_ohlc_window_data, Indicator},
    },
};
use polars::prelude::*;

use super::functions::calculate_span_alpha;

#[derive(Clone)]
pub struct TSIIndicator {
    pub name: String,
    pub anchor_symbol: String,
    pub windows: Vec<u32>,
    pub short_span: u32,
    pub long_span: u32,
}

// TSI = (PCDS/APCDS) x 100
// PC = CCP − PCP
// PCS = 25-period EMA of PC
// PCDS = 13-period EMA of PCS
// APC = AVCCP − PCP
// APCS = 25-period EMA of APC
// APCDS = 13-period EMA of APCS
// where:
// PCDS = PC double smoothed
// APCDS = Absolute PC double smoothed
// PC = Price change
// CCP = Current close price
// PCP = Prior close price
// PCS = PC smoothed
// EMA = Exponential moving average
// APC = Absolute PC
// APCS = Absolute PC smoothed

impl Indicator for TSIIndicator {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn get_indicator_columns(&self) -> Vec<(String, DataType)> {
        let mut columns_names = Vec::new();

        for window in &self.windows {
            let suffix = &format!("{}_{}", self.anchor_symbol, window);

            let stc_col = format!("TSI_{}", suffix);
            let non_shifted_stc_col = format!("non_shifted_{}", stc_col);
            let non_shifted_stc_col_dtype = DataType::Float64;
            let stc_col_dtype = DataType::Float64;

            columns_names.push((non_shifted_stc_col, non_shifted_stc_col_dtype));
            columns_names.push((stc_col, stc_col_dtype));
        }
        columns_names
    }

    fn get_data_offset(&self) -> u32 {
        let max_window = self.windows.iter().max();

        match max_window {
            Some(window) => (window * (self.short_span + self.long_span)) + 1000,
            None => 0,
        }
    }

    fn set_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, Error> {
        let mut resampled_lfs = vec![];

        let lf_full_mins = lf.clone().select([col("start_time")]);

        for window in &self.windows {
            let resampled_data = get_resampled_ohlc_window_data(&lf, &self.anchor_symbol, window)?;
            let (_, _, _, close_col) =
                get_symbol_window_ohlc_cols(&self.anchor_symbol, &window.to_string());
            let suffix = &format!("{}_{}", self.anchor_symbol, window);

            let pc_col = &format!("price_change_{}", suffix); // price momentum
            let apc_col = &format!("absolute_price_change_{}", suffix);

            let mut resampled_lf = resampled_data.with_columns([
                when(col(&close_col).shift(1).is_null())
                    .then(lit(0))
                    .otherwise(col(&close_col) - col(&close_col).shift(1))
                    .alias(pc_col),
                when(col(&close_col).shift(1).is_null())
                    .then(lit(0))
                    .otherwise((col(&close_col) - col(&close_col).shift(1)).abs())
                    .alias(apc_col),
            ]);

            let pcs_col = &format!("smoothed_{}", pc_col);
            let apcs_col = &format!("absolute_smoothed_{}", apc_col);

            let long_alpha = calculate_span_alpha(self.long_span as f64)?;
            let long_opts = EWMOptions {
                alpha: long_alpha,
                adjust: false,
                bias: false,
                min_periods: self.long_span as usize,
                ignore_nulls: true,
            };

            resampled_lf = resampled_lf.with_columns([
                col(&pc_col).ewm_mean(long_opts).alias(pcs_col),
                col(&apc_col).ewm_mean(long_opts).alias(apcs_col),
            ]);
            // long span +

            let pcds_col = &format!("double_smoothed_{}", pc_col);
            let apcds_col = &format!("absolute_double_smoothed_{}", apc_col);

            let short_alpha = calculate_span_alpha(self.short_span as f64)?;
            let short_opts = EWMOptions {
                alpha: short_alpha,
                adjust: false,
                bias: false,
                min_periods: self.short_span as usize,
                ignore_nulls: true,
            };

            resampled_lf = resampled_lf.with_columns([
                col(&pcs_col).ewm_mean(short_opts).alias(pcds_col),
                col(&apcs_col).ewm_mean(short_opts).alias(apcds_col),
            ]);
            // short span +

            let non_shifted_tsi_col = &format!("non_shifted_TSI_{}", suffix);
            let tsi_col = &format!("TSI_{}", suffix);

            resampled_lf = resampled_lf
                .with_column(
                    (lit(100) * (col(pcds_col) / col(apcds_col)))
                        .round(4)
                        .alias(non_shifted_tsi_col),
                )
                .with_column(col(non_shifted_tsi_col).shift(1).alias(tsi_col));

            let log_df = resampled_lf.clone().collect()?;

            let path = "data/test".to_string();
            let file_name = format!("tsi_log_resampled_window_{}.csv", window);
            save_csv(path.clone(), file_name, &log_df, true)?;

            resampled_lf = resampled_lf.select(vec![
                col("start_time"),
                col(non_shifted_tsi_col),
                col(tsi_col),
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

            let file_name = format!("tsi_log_full_minutes_window_{}.csv", window);
            save_csv(
                path.clone(),
                file_name,
                &resampled_lf_min.clone().collect()?,
                true,
            )?;

            resampled_lf_min = resampled_lf_min.select(vec![
                col("start_time"),
                col(non_shifted_tsi_col),
                col(tsi_col),
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
