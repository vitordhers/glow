use polars::prelude::*;

use crate::{
    shared::csv::save_csv,
    trader::{errors::Error, functions::get_symbol_ohlc_cols},
};

#[allow(dead_code)]
#[derive(Clone)]
pub struct Performance {
    benchmark_data: DataFrame,
    data: DataFrame,
    benchmark_stats: Statistics,
    stats: Statistics,
}

impl Performance {
    // pub fn new(data: &LazyFrame, traded_symbol: &String) -> Self {
    //     let benchmark_df = calculate_benchmark_stats(data, traded_symbol)
    //         .expect("calculate benchmark stats error");

    //     // calculate benchmark stats
    //     Self {
    //         data: DataFrame::default(),
    //         benchmark_data: benchmark_df,
    //         benchmark_stats: Statistics::default(),
    //         stats: Statistics::default(),
    //     }
    // }

    pub fn default() -> Self {
        Self {
            data: DataFrame::default(),
            benchmark_data: DataFrame::default(),
            benchmark_stats: Statistics::default(),
            stats: Statistics::default(),
        }
    }
}

// risk-adjusted return = reward / risk = mean returns / std of returns
// sharpe-ratio = excess return / risk = (mean return - risk free return) / std of returns
// downside deviation (semi-deviation) = negative std of returns / or deviation from targeted minimum return (TMR)
// sortino-ratio = excess return / downside risk = (mean return - TMR) / downside deviation
// calmar-ratio = reward / tail risk = CAGR (compound anual growth rate) / max drawdown
// max drawdown duration = the worst (longest) amount of time an investment has seen between peaks (equity highs)

impl Performance {
    pub fn set_benchmark(
        &mut self,
        strategy_lf: &LazyFrame,
        traded_symbol: &String,
        initial_balance: f64
    ) -> Result<(), Error> {
        self.benchmark_data = calculate_benchmark_data(strategy_lf, traded_symbol)?;

        let path = "data/test".to_string();
        let file_name = "benchmark.csv".to_string();

        save_csv(path, file_name, &self.benchmark_data, true)?;

        Ok(())
    }
}

pub fn calculate_benchmark_data(
    strategy_lf: &LazyFrame,
    traded_symbol: &String,
) -> Result<DataFrame, Error> {
    let trades_lf = calculate_trades(strategy_lf)?;
    let trading_lf = calculate_trading_sessions(trades_lf, traded_symbol)?;

    let df = trading_lf.collect()?;

    println!("SESSIONS DF {:?}", df);

    Ok(df)
}

pub fn calculate_benchmark_stats(
    benchmark_data: &DataFrame,
    initial_balance: f64
) -> Result<Statistics, Error> {


    Ok(Statistics::default())
}

pub fn calculate_trades(lf: &LazyFrame) -> Result<LazyFrame, Error> {
    let lf = lf.clone().with_column(
        when(
            col("position")
                .shift(1)
                .is_not_null()
                .and(col("position").neq(col("position").shift(1))),
        )
        .then(1)
        .otherwise(0)
        .alias("trade"),
    );

    Ok(lf)
}

pub fn calculate_trading_sessions(
    lf: LazyFrame,
    traded_symbol: &String,
) -> Result<LazyFrame, Error> {
    let (_, _, price_col, _) = get_symbol_ohlc_cols(traded_symbol);
    let mut lf = lf.with_column(col("trade").sign().cumsum(false).alias("session"));

    let aggs = [
        col("start_time").first().alias("start"),
        col("start_time").last().alias("end"),
        col(&price_col).first().alias("start_price"),
        col(&price_col).last().alias("end_price"),
        col("position").mean().alias("position_mean"),
    ];

    lf = lf
        .groupby([col("session")])
        .agg(aggs)
        .filter(col("position_mean").neq(0))
        .sort("start", SortOptions::default())
        .with_column(
            ((col("end_price") - col("start_price")) / col("start_price")).alias("return_pct"),
        );

    Ok(lf)
}
#[allow(dead_code)]
#[derive(Clone)]
pub struct Statistics {
    success_rate: f64,
    risk: f64,
    downside_deviation: f64,
    risk_adjusted_return: f64,
    max_drawdown: f64,
    max_drawdown_duration: f64,
    sharpe_ratio: f64,
    sortino_ratio: f64,
    calmar_ratio: f64,
}

impl Default for Statistics {
    fn default() -> Self {
        Self {
            success_rate: 0.0,
            risk: 0.0,
            downside_deviation: 0.0,
            risk_adjusted_return: 0.0,
            max_drawdown: 0.0,
            max_drawdown_duration: 0.0,
            sharpe_ratio: 0.0,
            sortino_ratio: 0.0,
            calmar_ratio: 0.0,
        }
    }
}
