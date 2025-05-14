use super::combinable_params::{CombinableParam, FeatureGenerator, OptimizableParam};
use common::structs::SymbolsPair;
use glow_error::GlowError;
use polars::prelude::*;

pub struct EMAFeatureGenerator {
    pub name: &'static str,
    pub param: OptimizableParam<u8>,
    pub symbols: SymbolsPair,
}

impl FeatureGenerator for EMAFeatureGenerator {
    fn get_name(&self) -> &str {
        self.name
    }

    fn get_param(&self) -> CombinableParam {
        CombinableParam::Uint8(self.param.clone())
    }

    fn compute(&self, lf: &LazyFrame) -> Result<LazyFrame, GlowError> {
        // firstly, get param range size
        let param_size = self.param.range_size();
        let close_col = self.symbols.quote.get_close_col();
        let result_lf = lf.clone();
        let output_type: SpecialEq<Arc<dyn FunctionOutputField>> =
            GetOutput::from_type(DataType::Float32);
        result_lf.with_column(col(close_col).apply_many(
            |columns| {
                // 1440 prices for the day
                let close_prices = columns
                    .get(0)
                    .expect("close prices column to be defined at EMA Feature Generator compute")
                    .f32()?;
                let values: Vec<f32> = close_prices.into_no_null_iter().collect();
                let range = self.param.range;

                for param in range {
                    let alpha = calculate_span_alpha(param.into()).expect("alpha to be valid");
                    let ewmas = ewma(&values, alpha, Some(param.into()));
                }

                let series = Column::new("downside_risk".into(), vec![trade_downside_risk]);
                Ok(Some(series))
            },
            &[],
            output_type,
        ));
        todo!()
        //
        // let cols = self.get_indicators_columns(symbols_pair, params);
        // let (ema_fast_col, _) = cols
        //     .first()
        //     .expect("EMA indicator to have column at index 0");
        // let fast_span_param = params
        //     .get(&ParamId::FastSpan)
        //     .expect("FastSpan param to be set at ParamsMap");
        // let fast_span = if let Param::UInt32(value, _) = fast_span_param {
        //     *value
        // } else {
        //     20
        // };
        // let (ema_slow_col, _) = cols
        //     .get(1)
        //     .expect("EMA indicator to have column at index 1");
        // let slow_span_param = params
        //     .get(&ParamId::SlowSpan)
        //     .expect("SlowSpan param to be set at ParamsMap");
        // let slow_span = if let Param::UInt32(value, _) = slow_span_param {
        //     *value
        // } else {
        //     100
        // };
        //
        // let fast_alpha = calculate_span_alpha(fast_span as f64)?;
        // let slow_alpha = calculate_span_alpha(slow_span as f64)?;
        // let fast_opts = EWMOptions {
        //     alpha: fast_alpha,
        //     adjust: false,
        //     bias: false,
        //     min_periods: 1,
        //     ignore_nulls: false,
        // };
        // let slow_opts = EWMOptions {
        //     alpha: slow_alpha,
        //     adjust: false,
        //     bias: false,
        //     min_periods: 1,
        //     ignore_nulls: false,
        // };
        //
        // let lf = lf
        //     .with_columns([
        //         col(close_col).ewm_mean(slow_opts).alias(ema_slow_col),
        //         col(close_col).ewm_mean(fast_opts).alias(ema_fast_col),
        //     ]);
        //     // .with_column(
        //     //     when(col(ema_fast_col).is_null().or(col(ema_slow_col).is_null()))
        //     //         .then(lit(NULL))
        //     //         .otherwise(
        //     //             when(col(ema_fast_col).gt(col(ema_slow_col)))
        //     //                 .then(true)
        //     //                 .otherwise(false),
        //     //         )
        //     //         .alias(format!("{}_{}_{}", TREND_COL_PREFIX, slow_span, fast_span)),
        //     // );
        //
        // Ok(lf)
    }
}

pub fn calculate_span_alpha(span: f32) -> Result<f32, GlowError> {
    if span < 1.0 {
        panic!("Require 'span' >= 1 (found {})", span);
    }
    Ok(2.0 / (span + 1.0))
}

pub fn ewma(data: &[f32], alpha: f32, min_periods: Option<usize>) -> Vec<Option<f32>> {
    if data.is_empty() {
        return vec![];
    }

    let min_periods = min_periods.unwrap_or(1); // default to 1 if not specified
    let mut result = Vec::with_capacity(data.len());
    let mut prev = data[0];

    for (i, &value) in data.iter().enumerate() {
        if i == 0 {
            prev = value;
        } else {
            prev = alpha * value + (1.0 - alpha) * prev;
        }

        if i + 1 >= min_periods {
            result.push(Some(prev));
        } else {
            result.push(None);
        }
    }

    result
}
