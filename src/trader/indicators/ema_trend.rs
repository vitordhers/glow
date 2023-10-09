use crate::trader::{errors::Error, functions::get_symbol_ohlc_cols, traits::indicator::Indicator};
use polars::prelude::*;

use super::functions::calculate_span_alpha;

#[derive(Clone)]
pub struct ExponentialMovingAverageTrendIndicator {
    pub name: String,
    pub anchor_symbol: String,
    pub long_span: usize,
    pub short_span: usize,
    pub trend_col: String,
}

impl Indicator for ExponentialMovingAverageTrendIndicator {
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

    fn get_data_offset(&self) -> u32 {
        self.long_span as u32
    }
}
