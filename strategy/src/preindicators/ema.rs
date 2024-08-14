use common::{structs::Symbol, traits::indicator::Indicator};
use glow_error::{assert_or_error, GlowError};
use polars::prelude::*;

use crate::functions::calculate_span_alpha;

use super::PreIndicatorParamsWrapper;

#[derive(Clone)]
pub struct EMA {
    pub name: &'static str,
    pub anchor_symbol: &'static Symbol,
    pub trend_col: String,
    pub params: EMAParams,
}

// impl EMA {
//     fn new(anchor_symbol: &str) -> Self {
//         let anchor_symbol = SYMBOLS_MAP.get(anchor_symbol).unwrap();
//         Self {
//             name: "EMA",
//             anchor_symbol,

//         }
//     }
// }

#[derive(Clone, Copy)]
pub struct EMAParams {
    pub long_span: usize,
    pub short_span: usize,
}

impl Into<EMAParams> for PreIndicatorParamsWrapper {
    fn into(self) -> EMAParams {
        match self {
            Self::Ema(params) => params
        }
    }
}

impl Indicator for EMA {
    type Params = EMAParams;

    fn name(&self) -> &'static str {
        self.name.clone()
    }

    fn get_indicator_columns(&self) -> Vec<(String, DataType)> {
        let mut columns_names = Vec::new();

        let fast_ema_col_title = format!("{}_fast_ema", &self.anchor_symbol.name);
        let fast_ema_col_dtype = DataType::Float64;
        let slow_ema_col_title = format!("{}_slow_ema", &self.anchor_symbol.name);
        let slow_ema_col_dtype = DataType::Float64;

        columns_names.push((fast_ema_col_title, fast_ema_col_dtype));
        columns_names.push((slow_ema_col_title, slow_ema_col_dtype));
        columns_names.push((self.trend_col.clone(), DataType::Int32));
        columns_names
    }

    fn set_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, GlowError> {
        let EMAParams {
            long_span,
            short_span,
        } = self.params;
        // let (_, _, _, close_col) = get_symbol_ohlc_cols(&self.anchor_symbol);
        let close_col = self.anchor_symbol.get_close_col();
        let ema_short_col = &format!("{}_ema_s", &self.anchor_symbol.name);
        let ema_long_col = &format!("{}_ema_l", &self.anchor_symbol.name);
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

    fn get_minimum_klines_for_benchmarking(&self) -> u32 {
        self.params.long_span as u32
    }

    fn patch_params(&mut self, params: Self::Params) -> Result<(), GlowError> {
        assert_or_error!(params.long_span > params.short_span);
        self.params = params;
        Ok(())
    }
}
