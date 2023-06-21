use chrono::{Datelike, Local};
use polars::prelude::*;
use reqwest::Client;
use serde::Deserialize;
use std::fmt::Debug;

use crate::{
    shared::csv::save_csv,
    trader::{errors::Error, functions::get_symbol_ohlc_cols},
};

#[allow(dead_code)]
#[derive(Clone)]
pub struct Performance {
    http: Client,
    benchmark_data: DataFrame,
    data: DataFrame,
    benchmark_stats: Statistics,
    stats: Statistics,
    risk_free_daily_rate: Option<f64>,
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
            http: Client::new(),
            data: DataFrame::default(),
            benchmark_data: DataFrame::default(),
            benchmark_stats: Statistics::default(),
            stats: Statistics::default(),
            risk_free_daily_rate: None,
        }
    }
}

// risk-adjusted return = reward / risk = mean returns / std of returns
// sharpe-ratio = excess return / risk = (mean return - risk free return) / std of returns
// downside deviation (semi-deviation) = negative std of returns / or deviation from targeted minimum return (TMR)
// sortino-ratio = excess return / downside risk = (mean return - TMR) / downside deviation
// max drawdown duration = the worst (longest) amount of time an investment has seen between peaks (equity highs)
// calmar-ratio = reward / tail risk = CAGR (compound anual growth rate) / max drawdown

impl Performance {
    pub fn set_benchmark(
        &mut self,
        strategy_lf: &LazyFrame,
        traded_symbol: &String,
        initial_balance: f64,
    ) -> Result<(), Error> {
        self.benchmark_data = calculate_benchmark_data(strategy_lf, traded_symbol)?;

        let path = "data/test".to_string();
        let file_name = "benchmark.csv".to_string();

        save_csv(path, file_name, &self.benchmark_data, true)?;

        Ok(())
    }

    fn set_risk_free_daily_rate(&mut self, rate: Option<f64>) {
        self.risk_free_daily_rate = rate;
    }
}
// move to trader
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
    initial_balance: f64,
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
    let (_, _, _, price_col) = get_symbol_ohlc_cols(traded_symbol);
    let mut lf = lf.with_column(col("trade").sign().cumsum(false).alias("session"));

    let aggs = [
        col("start_time").first().alias("start"),
        col("start_time").last().alias("end"),
        col(&price_col).first().alias("start_price"),
        col(&price_col).last().alias("end_price"),
        col(&price_col).std(0).alias("std_price"),
        col(&price_col).min().alias("min_price"),
        col(&price_col).max().alias("max_price"),
        col("position").mean().alias("position_mean"),
    ];

    lf = lf
        .filter(col("position").neq(0))
        .groupby([col("session")])
        .agg(aggs)
        .sort("start", SortOptions::default())
        .with_columns(vec![
            ((col("end_price") - col("start_price")) / col("start_price")).alias("relative_return"),
            ((col("max_price") - col("start_price")).abs() / col("start_price"))
                .alias("long_potential"),
            ((col("min_price") - col("start_price")).abs() / col("start_price"))
                .alias("short_potential"),
        ])
        .with_column(
            (when(col("short_potential").gt(col("long_potential")))
                .then(col("short_potential"))
                .otherwise(col("long_potential")))
            .alias("trade_potential"),
        )
        .with_column(
            (when(col("short_potential").gt(col("long_potential")))
                .then(col("short_potential"))
                .otherwise(col("long_potential")))
            .alias("trade_potential"),
        )
        .with_column(
            (col("position_mean") * (col("relative_return") / col("trade_potential")))
                .alias("potential_seized"),
        );

    Ok(lf)
}

pub fn calculate_performance_on_trading_sessions(lf: LazyFrame) {}

pub async fn get_latest_us_treasury_bills_yearly_rate(http: &Client) -> Result<f64, Error> {
    let now = Local::now();
    let first_day_of_year = now.with_month(1).unwrap().with_day(1).unwrap();
    let formatted_current_day = now.format("%Y-%m-%d");
    let formatted_first_day_of_year = first_day_of_year.format("%Y-%m-%d");

    let url =  format!("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates?fields=record_date,security_desc,avg_interest_rate_amt&filter=record_date:lt:{},src_line_nbr:eq:1,record_date:gt:{}&sort=-record_date", formatted_current_day, formatted_first_day_of_year);
    let result: HttpTreasuryResponseWrapper<TreasuryRateResponse> =
        http.get(url).send().await?.json().await?;

    let yearly_rate: f64 = result.data.first().unwrap().avg_interest_rate_amt.parse()?;

    let current_year = now.year();
    let is_leap_year = chrono::NaiveDate::from_ymd_opt(current_year, 1, 1)
        .unwrap()
        .with_month(12)
        .unwrap()
        .day()
        == 29;

    let days_in_current_year = if is_leap_year { 366 } else { 365 };

    let eir_daily =
        ((1.0 + yearly_rate) / days_in_current_year as f64).powi(days_in_current_year - 1);

    Ok(eir_daily)
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
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct HttpTreasuryResponseWrapper<T> {
    data: Vec<T>,
    meta: HttpTreasuryResponseMeta<T>,
    links: HttpTreasuryResponseLinks,
}
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct TreasuryRateResponse {
    record_date: String,
    security_desc: String,
    avg_interest_rate_amt: String,
}
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct HttpTreasuryResponseMeta<T> {
    count: i32,
    labels: T,
    #[serde(rename = "dataTypes")]
    data_types: T,
    #[serde(rename = "dataFormats")]
    data_formats: T,
    #[serde(rename = "total-count")]
    total_count: i32,
    #[serde(rename = "total-pages")]
    total_pages: i32,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct HttpTreasuryResponseLinks {
    #[serde(rename = "self")]
    self_: String,
    first: String,
    prev: Option<String>,
    next: Option<String>,
    last: String,
}
