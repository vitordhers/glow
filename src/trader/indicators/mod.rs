use polars::prelude::*;

use crate::trader::{
    constants::{NANOS_IN_SECOND, SECONDS_IN_MIN},
    functions::get_symbol_window_ohlc_cols,
};

use super::{errors::Error, models::indicator::Indicator};

pub fn stochastic(
    indicator: &Indicator,
    _stochastic_column: &String,
    anchor_symbol: &String,
    lf: &LazyFrame,
) -> Result<LazyFrame, Error> {
    println!("stochastic fn");
    // TODO: rewrite this using LazyFrames instead
    let (_, high_col, close_col, low_col) =
        get_symbol_window_ohlc_cols(anchor_symbol, &indicator.window_in_mins.to_string());

    let k_column = format!("{}_K%_{}", anchor_symbol, indicator.window_in_mins);
    let d_column = format!("{}_D%_{}", anchor_symbol, indicator.window_in_mins);

    let df = lf.clone().collect()?;
    // let close_k = df.column(&close_col)?;

    let rolling_k_opts = RollingOptions {
        window_size: Duration::new(14 * SECONDS_IN_MIN * NANOS_IN_SECOND),
        min_periods: 1,
        center: false,
        by: Some(String::from("start_time")),
        weights: None,
        closed_window: Some(ClosedWindow::Right),
    };

    let rolling_min_k_low = df
        .column(&low_col)?
        .rolling_min(RollingOptionsImpl::from(rolling_k_opts.clone()))?;

    let rolling_max_k_high = df
        .column(&high_col)?
        .rolling_max(RollingOptionsImpl::from(rolling_k_opts))?;

    let mut k =
        100.mul(&((close_k - &rolling_min_k_low) / (&rolling_max_k_high - &rolling_min_k_low)));
    k.rename(&k_column);

    let rolling_mean_d_opts = RollingOptions {
        window_size: Duration::new(3 * SECONDS_IN_MIN * NANOS_IN_SECOND),
        min_periods: 1,
        center: false,
        by: Some(String::from("start_time")),
        weights: None,
        closed_window: Some(ClosedWindow::Right),
    };

    // let mut d = k.rolling_mean(RollingOptionsImpl::from(rolling_mean_d_opts))?;
    // d.rename(&d_column);

    // let start_time_series = df.column("start_time")?;

    // let df = DataFrame::new(vec![start_time_series.clone(), k, d])?;

    let k_window = 14;
    let df = lf
        .filter(col("start_time"))
        .with_columns(col(&close_col).sub(col(&low_col)).alias(k_column));

    // col(&close_col)
    //         - col(&low_col)
    //             .rolling_min(rolling_k_opts)
    //             //  (col(&high_col).rolling_max(rolling_k_opts)
    //             //     - col(&low_col).rolling_min(rolling_k_opts))
    //             .alias(k_column),

    Ok(df.lazy())
}
