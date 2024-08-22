use super::Schema;
use crate::{
    functions::calculate_span_alpha,
    params::{NumberParamConfig, Param, ParamId},
    StrategyId,
};
use common::{enums::signal_category::SignalCategory, structs::SymbolsPair};
use glow_error::GlowError;
use polars::prelude::*;
use std::collections::HashMap;

const TREND_COL: &'static str = "EMA_bullish";

#[derive(Clone)]
pub struct SimpleTrendStrategySchema {}

impl Schema for SimpleTrendStrategySchema {
    fn append_indicators_to_lf(
        &self,
        lf: LazyFrame,
        symbols_pair: SymbolsPair,
        params: &HashMap<ParamId, Param>,
    ) -> Result<LazyFrame, GlowError> {
        let close_col = symbols_pair.anchor.get_close_col();

        let cols = self.get_indicators_columns(symbols_pair, params);

        let (ema_fast_col, _) = cols
            .get(0)
            .expect("EMA indicator to have column at index 0");
        let fast_span_param = params
            .get(&ParamId::FastSpan)
            .expect("FastSpan param to be set at ParamsMap");
        let fast_span = if let Param::UInt32(value, _) = fast_span_param {
            value.clone()
        } else {
            20
        };

        let (ema_slow_col, _) = cols
            .get(1)
            .expect("EMA indicator to have column at index 1");
        let slow_span_param = params
            .get(&ParamId::SlowSpan)
            .expect("SlowSpan param to be set at ParamsMap");
        let slow_span = if let Param::UInt32(value, _) = slow_span_param {
            value.clone()
        } else {
            100
        };

        let fast_alpha = calculate_span_alpha(fast_span as f64)?;
        let slow_alpha = calculate_span_alpha(slow_span as f64)?;
        let fast_opts = EWMOptions {
            alpha: fast_alpha,
            adjust: false,
            bias: false,
            min_periods: 1,
            ignore_nulls: false,
        };
        let slow_opts = EWMOptions {
            alpha: slow_alpha,
            adjust: false,
            bias: false,
            min_periods: 1,
            ignore_nulls: false,
        };

        let mut lf = lf
            .with_columns([
                col(close_col).ewm_mean(slow_opts).alias(ema_slow_col),
                col(close_col).ewm_mean(fast_opts).alias(ema_fast_col),
            ])
            .with_column(
                when(col(ema_fast_col).is_null().or(col(ema_slow_col).is_null()))
                    .then(lit(NULL))
                    .otherwise(
                        when(col(ema_fast_col).gt(col(ema_slow_col)))
                            .then(true)
                            .otherwise(false),
                    )
                    .alias(&TREND_COL),
            );

        // TODO: check if select needs to be done
        lf = lf.select([
            col("start_time"),
            col(&TREND_COL),
            col(ema_slow_col),
            col(ema_fast_col),
        ]);

        Ok(lf)
    }

    fn append_indicators_to_df(
        &self,
        df: DataFrame,
        symbols_pair: SymbolsPair,
        params: &HashMap<ParamId, Param>,
    ) -> Result<DataFrame, GlowError> {
        // TODO: rewrite this
        let mut new_lf = df.clone().lazy();
        new_lf = self.append_indicators_to_lf(new_lf, symbols_pair, params)?;
        let new_df = new_lf.collect()?;
        let mut result_df = df.clone();

        for (column, _) in self.get_indicators_columns(symbols_pair, params) {
            let series = new_df.column(column.as_str())?;
            let _ = result_df.replace(&column, series.to_owned());
        }

        Ok(result_df)
    }

    fn append_signals_to_lf(
        &self,
        lf: LazyFrame,
        symbols_pair: SymbolsPair,
        _: &HashMap<ParamId, Param>,
    ) -> Result<LazyFrame, GlowError> {
        let short_col = SignalCategory::GoShort.get_column();
        let long_col = SignalCategory::GoLong.get_column();
        let close_short_col = SignalCategory::CloseShort.get_column();
        let close_long_col = SignalCategory::CloseLong.get_column();
        // let select_columns = vec![col("start_time"), col(signal_col)];

        let fast_ema_col = format!("{}_fast_ema", &symbols_pair.anchor.name);
        let slow_ema_col = format!("{}_slow_ema", &symbols_pair.anchor.name);

        let signal_lf = lf.clone().with_columns([
            when(
                col(&fast_ema_col).lt(col(&slow_ema_col)).and(
                    col(&slow_ema_col)
                        .shift(1)
                        .lt_eq(col(&fast_ema_col).shift(1)),
                ),
            )
            .then(lit(1))
            .otherwise(lit(0))
            .alias(short_col),
            when(
                col(&fast_ema_col).gt(col(&slow_ema_col)).and(
                    col(&slow_ema_col)
                        .shift(1)
                        .gt_eq(col(&fast_ema_col).shift(1)),
                ),
            )
            .then(lit(1))
            .otherwise(lit(0))
            .alias(long_col),
            when(
                col(&fast_ema_col).gt(col(&slow_ema_col)).and(
                    col(&slow_ema_col)
                        .shift(1)
                        .gt_eq(col(&fast_ema_col).shift(1)),
                ),
            )
            .then(lit(1))
            .otherwise(lit(0))
            .alias(close_long_col),
            when(
                col(&fast_ema_col).gt(col(&slow_ema_col)).and(
                    col(&slow_ema_col)
                        .shift(1)
                        .gt_eq(col(&fast_ema_col).shift(1)),
                ),
            )
            .then(lit(1))
            .otherwise(lit(0))
            .alias(close_short_col),
        ]);

        Ok(signal_lf)
    }

    fn append_signals_to_df(
        &self,
        df: DataFrame,
        symbols_pair: SymbolsPair,
        params: &HashMap<ParamId, Param>,
    ) -> Result<DataFrame, GlowError> {
        let mut updated_lf = df.clone().lazy();
        updated_lf = self.append_signals_to_lf(updated_lf, symbols_pair, params)?;
        let updated_df = updated_lf.collect()?;
        let mut result_df = updated_df.clone();

        let signals = [
            SignalCategory::GoShort,
            SignalCategory::GoLong,
            SignalCategory::CloseLong,
            SignalCategory::CloseShort,
        ];

        // TODO: check this
        for signal in signals.iter() {
            let column = signal.get_column();
            let series = result_df.column(column)?;
            let _ = result_df.replace(&column, series.to_owned());
        }
        // let signal = self.signal_category();

        Ok(result_df)
    }

    fn get_params_config(&self) -> HashMap<ParamId, Param> {
        let mut default_params = HashMap::new();
        default_params.insert(
            ParamId::SlowSpan,
            Param::UInt32(100, NumberParamConfig::new(100, Some(50), Some(200))),
        );

        default_params.insert(
            ParamId::FastSpan,
            Param::UInt32(20, NumberParamConfig::new(20, Some(1), Some(50))),
        );

        default_params
    }

    fn get_indicators_columns(
        &self,
        symbols_pair: SymbolsPair,
        _: &HashMap<ParamId, Param>,
    ) -> Vec<(String, DataType)> {
        let mut columns = Vec::new();
        columns.push((
            format!("{}_fast_ema", symbols_pair.anchor.name),
            DataType::Float64,
        ));
        columns.push((
            format!("{}_slow_ema", symbols_pair.anchor.name),
            DataType::Float64,
        ));
        columns.push((TREND_COL.to_string(), DataType::Boolean));
        columns
    }

    fn get_minimum_klines_for_calculation(&self, params: &HashMap<ParamId, Param>) -> u32 {
        let slow_span_param = params
            .get(&ParamId::SlowSpan)
            .expect("LongSpan param to be set at ParamsMap");
        let slow_span = if let Param::UInt32(value, _) = slow_span_param {
            value.clone()
        } else {
            // TODO: review this
            0
        };

        slow_span
    }

    fn get_signals_columns(
        &self,
        _: SymbolsPair,
        _: &HashMap<ParamId, Param>,
    ) -> Vec<(String, DataType)> {
        let available_signals = [
            SignalCategory::GoShort,
            SignalCategory::GoLong,
            SignalCategory::CloseShort,
            SignalCategory::CloseLong,
        ];

        available_signals
            .iter()
            .map(|s| (s.get_column().to_string(), DataType::UInt32))
            .collect()
    }
}

impl From<StrategyId> for SimpleTrendStrategySchema {
    fn from(value: StrategyId) -> Self {
        match value {
            StrategyId::SimpleTrend => SimpleTrendStrategySchema {},
        }
    }
}
