use super::{PreIndicatorParamsWrapper, PreIndicatorWrapper};
use crate::functions::calculate_span_alpha;
use common::{
    r#static::get_default_symbol,
    structs::{Symbol, SymbolsPair},
    traits::indicator::Indicator,
};
use glow_error::{assert_or_error, GlowError};
use polars::prelude::*;

const NAME: &'static str = "EMA";
const TREND_COL: &'static str = "EMA_bullish";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EMAParams {
    pub long_span: usize,
    pub short_span: usize,
}

impl Default for EMAParams {
    fn default() -> Self {
        Self {
            long_span: 200,
            short_span: 20,
        }
    }
}

impl Into<EMAParams> for PreIndicatorParamsWrapper {
    fn into(self) -> EMAParams {
        match self {
            Self::Ema(params) => params,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EMA {
    pub name: &'static str,
    pub anchor_symbol: &'static Symbol,
    pub params: EMAParams,
    columns: Vec<(String, DataType)>,
}

impl EMA {
    fn new(params: EMAParams, anchor_symbol: &'static Symbol) -> Self {
        let mut columns = Vec::new();
        columns.push((
            format!("{}_fast_ema", anchor_symbol.name),
            DataType::Float64,
        ));
        columns.push((
            format!("{}_slow_ema", anchor_symbol.name),
            DataType::Float64,
        ));
        columns.push((TREND_COL.to_string(), DataType::Boolean));
        Self {
            name: NAME,
            anchor_symbol,
            params,
            columns,
        }
    }
}

impl Default for EMA {
    fn default() -> Self {
        Self::new(EMAParams::default(), get_default_symbol())
    }
}

impl Indicator for EMA {
    type Params = EMAParams;
    type Wrapper = PreIndicatorWrapper;

    fn name(&self) -> &'static str {
        self.name
    }

    fn get_indicator_columns(&self) -> &Vec<(String, DataType)> {
        &self.columns
    }

    fn set_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, GlowError> {
        let EMAParams {
            long_span,
            short_span,
        } = self.params;

        let close_col = self.anchor_symbol.get_close_col();
        let cols = self.get_indicator_columns();

        let (ema_short_col, _) = cols
            .get(0)
            .expect("EMA indicator to have column at index 0");
        let (ema_long_col, _) = cols
            .get(1)
            .expect("EMA indicator to have column at index 1");

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
                col(close_col).ewm_mean(long_opts).alias(ema_long_col),
                col(close_col).ewm_mean(short_opts).alias(ema_short_col),
            ])
            .with_column(
                when(col(ema_short_col).is_null().or(col(ema_long_col).is_null()))
                    .then(lit(NULL))
                    .otherwise(
                        when(col(ema_short_col).gt(col(ema_long_col)))
                            .then(true)
                            .otherwise(false),
                    )
                    .alias(&TREND_COL),
            );

        lf = lf.select([
            col("start_time"),
            col(&TREND_COL),
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
            let series = new_df.column(column)?;
            let _ = result_df.replace(&column, series.to_owned());
        }

        Ok(result_df)
    }

    fn get_minimum_klines_for_benchmarking(&self) -> u32 {
        self.params.long_span as u32
    }

    fn patch_params(&self, params: Self::Params) -> Result<Self::Wrapper, GlowError> {
        let mut updated = self.clone();
        if self.params == params {
            return Ok(updated.into());
        }
        assert_or_error!(params.long_span > params.short_span);
        updated.params = params;
        Ok(updated.into())
    }

    fn patch_symbols_pair(
        &self,
        updated_symbols_pair: SymbolsPair,
    ) -> Result<Self::Wrapper, GlowError> {
        let mut updated = self.clone();
        if self.anchor_symbol == updated_symbols_pair.anchor {
            return Ok(updated.into());
        }
        updated.anchor_symbol = updated_symbols_pair.anchor;
        Ok(updated.into())
    }
}
