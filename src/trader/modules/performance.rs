use chrono::Duration;
use chrono::{Datelike, Local};
use polars::prelude::*;
use reqwest::Client;
use serde::Deserialize;
use std::fmt::Debug;

use crate::trader::functions::round_down_nth_decimal;
use crate::trader::models::behavior_subject::BehaviorSubject;
use crate::trader::traits::exchange::Exchange;
use crate::{
    shared::csv::save_csv,
    trader::{errors::Error, functions::get_symbol_ohlc_cols},
};

#[allow(dead_code)]
#[derive(Clone)]
pub struct Performance {
    http: Client,
    exchange: BehaviorSubject<Box<dyn Exchange + Send + Sync>>,
    benchmark_data: DataFrame,
    trading_data: DataFrame,
    benchmark_stats: Statistics,
    trading_stats: Statistics,
    risk_free_returns: f64,
}

impl Performance {
    pub fn new(exchange: BehaviorSubject<Box<dyn Exchange + Send + Sync>>) -> Self {
        Self {
            http: Client::new(),
            exchange,
            trading_data: DataFrame::default(),
            benchmark_data: DataFrame::default(),
            benchmark_stats: Statistics::default(),
            trading_stats: Statistics::default(),
            risk_free_returns: 0.0,
        }
    }

    // pub fn default() -> Self {
    //     Self {
    //         http: Client::new(),
    //         trading_data: DataFrame::default(),
    //         benchmark_data: DataFrame::default(),
    //         benchmark_stats: Statistics::default(),
    //         trading_stats: Statistics::default(),
    //         risk_free_returns: 0.0,
    //     }
    // }
}

// risk-adjusted-return = reward / risk = mean returns / std of returns
// sharpe-ratio = excess return / risk = (mean return - risk free return) / std of returns
// downside deviation (semi-deviation) = negative std of returns / or deviation from targeted minimum return (TMR)
// sortino-ratio = excess return / downside risk = (mean return - TMR) / downside deviation
// max drawdown duration = the worst (longest) amount of time an investment has seen between peaks (equity highs)
// calmar-ratio = reward / tail risk = CAGR (compound anual growth rate) / max drawdown -> for that matter, use benchmark period growth rate

impl Performance {
    pub fn set_benchmark(&mut self, strategy_lf: &LazyFrame) -> Result<(), Error> {
        let path = "data/test".to_string();
        let file_name = "benchmark.csv".to_string();
        let strategy_df = strategy_lf.clone().collect()?;
        save_csv(path.clone(), file_name, &strategy_df, true)?;

        let binding = self.exchange.ref_value();
        let traded_contract = binding.get_traded_contract();
        let traded_symbol = &traded_contract.symbol;

        self.benchmark_data =
            calculate_benchmark_data(strategy_lf, self.risk_free_returns, traded_symbol)?;
        let file_name = "trades.csv".to_string();
        save_csv(path, file_name, &self.benchmark_data, true)?;
        // self.benchmark_data = strategy_lf.clone().collect()?;

        Ok(())
    }

    fn set_risk_free_daily_returns(&mut self, returns: f64) {
        self.risk_free_returns = returns;
    }
}
// move to trader
pub fn calculate_benchmark_data(
    strategy_lf: &LazyFrame,
    risk_free_returns: f64,
    traded_symbol: &String,
) -> Result<DataFrame, Error> {
    let trades_lf = calculate_trades(strategy_lf, traded_symbol)?;
    // let path = "data/test".to_string();
    // let file_name = "trades_lf.csv".to_string();
    // let trades_df = trades_lf.clone().collect()?;
    // save_csv(path, file_name, &trades_df, true)?;

    let trading_lf = calculate_trading_sessions(trades_lf, traded_symbol)?;

    let df = trading_lf.collect()?;

    let benchmark_stats = calculate_benchmark_stats(&df, risk_free_returns)?;

    println!("Benchmark stats {:?}", benchmark_stats);

    Ok(df)
}

pub fn calculate_trades(lf: &LazyFrame, traded_symbol: &String) -> Result<LazyFrame, Error> {
    let (_, _, _, price_col) = get_symbol_ohlc_cols(traded_symbol);
    let lf = lf.clone().with_columns([
        when(
            col("position")
                .shift(1)
                .is_not_null()
                .and(col("position").neq(col("position").shift(1))),
        )
        .then(1)
        .otherwise(0)
        .alias("trade"),
        // when(
        //     col("returns")
        //         .shift(1)
        //         .is_not_null()
        //         .and(col("returns").shift(1).neq(0.0)),
        // )
        // .then((col("returns") - col("returns").shift(1)) / col("returns").shift(1))
        // .otherwise(0.0)
        // .alias("returns_pct"),
        // col("P&L").shift(-1).keep_name(),
        // col("balance").shift(-1).keep_name(),
        // (col("trade_fees") + col("trade_fees").shift(-1)).keep_name(),
    ]);

    Ok(lf)
}

pub fn calculate_trading_sessions(
    lf: LazyFrame,
    traded_symbol: &String,
) -> Result<LazyFrame, Error> {
    let (_, _, _, price_col) = get_symbol_ohlc_cols(traded_symbol);
    let mut lf = lf
        .with_column(col("trade").sign().cumsum(false).alias("session"))
        // extends session to the next cell, where it is closed
        .with_column(
            when(
                col("trade").neq(col("trade").shift(1)).and(
                    col("position")
                        .shift(1)
                        .neq(0)
                        .and(col("position").shift(1).is_not_null()),
                ),
            )
            .then(col("session").shift(1))
            .otherwise(col("session"))
            .keep_name(),
        );
    let path = "data/test".to_string();
    let file_name = "trades_lf.csv".to_string();
    let trades_df = lf.clone().collect()?;
    save_csv(path, file_name, &trades_df, true)?;
    let returns_output: SpecialEq<Arc<dyn FunctionOutputField>> =
        GetOutput::from_type(DataType::Float64);

    let aggs = [
        col("start_time").first().alias("start"),
        col("start_time").last().alias("end"),
        col(&price_col).first().alias("start_price"),
        col(&price_col).last().alias("end_price"),
        col(&price_col).min().alias("min_price"),
        col(&price_col).max().alias("max_price"),
        col("action").last().alias("close_signal"),
        col("position").first().alias("position"),
        col("returns").last().keep_name(),
        col("returns").max().alias("max_returns"),
        (col("returns").last() / col("returns").max()).alias("returns_seized"),
        col("units").mean().keep_name(),
        col("P&L").last().keep_name(),
        col("balance").last().keep_name(),
        col("returns").std(0).alias("risk"),
        col("trade_fees").sum().keep_name(),
        col("returns")
            .apply_many(
                |series| {
                    let positions_series: &Series = &series[1];
                    let returns_series: &Series = &series[0];
                    let position = positions_series.head(Some(1)).mean().unwrap();

                    // if position is long
                    if position == 0.0 {
                        return Ok(Some(Series::new("downside_risk", vec![0.0])));
                    };

                    let negative_returns_vec = returns_series
                        .f64()
                        .unwrap()
                        .into_no_null_iter()
                        .filter(|returns| returns < &0.0)
                        .collect::<Vec<f64>>();
                    let negative_returns_len = negative_returns_vec.len() as f64;
                    let negative_returns_mean: f64 =
                        negative_returns_vec.iter().copied().sum::<f64>() / negative_returns_len;
                    let downside_squared = negative_returns_vec
                        .into_iter()
                        .map(|returns| (returns - negative_returns_mean).powi(2))
                        .collect::<Vec<f64>>();

                    let filtered_downside_squared_sum: f64 = downside_squared.iter().sum();
                    let trade_downside_risk =
                        (filtered_downside_squared_sum / negative_returns_len).sqrt();
                    let series = Series::new("downside_risk", vec![trade_downside_risk]);
                    Ok(Some(series))
                },
                &[col("position")],
                returns_output,
            )
            .alias("downside_risk"),
    ];

    lf = lf
        .groupby([col("session")])
        .agg(aggs)
        .sort("start", SortOptions::default())
        .filter(col("position").neq(0).or(col("session").eq(0)))
        .with_column(((col("balance").cummax(false) - col("balance")).abs()/ col("balance").cummax(false)).alias("drawdown"))
        // .with_columns(vec![
        //     ((col("end_price") - col("start_price")) / col("start_price")).alias("relative_return"),
        //     // ().alias("")
        //     // ((col("max_price") - col("start_price")).abs() / col("start_price"))
        //     //     .alias("long_potential"),
        //     // ((col("min_price") - col("start_price")).abs() / col("start_price"))
        //     //     .alias("short_potential"),
        // ])
        // .with_column(
        //     (when(col("short_potential").gt(col("long_potential")))
        //         .then(col("short_potential"))
        //         .otherwise(col("long_potential")))
        //     .alias("trade_potential"),
        // )
        // .with_column(
        //     (when(col("short_potential").gt(col("long_potential")))
        //         .then(col("short_potential"))
        //         .otherwise(col("long_potential")))
        //     .alias("trade_potential"),
        // )
        // .with_column(
        //     (col("position_mean") * (col("relative_return") / col("trade_potential")))
        //         .alias("potential_seized"),
        // )
        ;

    Ok(lf)
}

pub fn calculate_benchmark_stats(
    benchmark_data: &DataFrame,
    risk_free_returns: f64,
) -> Result<Statistics, Error> {
    let initial_data_filter_mask = benchmark_data.column("position")?.not_equal(0)?;
    let df = benchmark_data.filter(&initial_data_filter_mask)?;

    let start_series = df.column("start")?;
    let end_series = df.column("end")?;
    let returns_series = df.column("returns")?;
    let risk_series = df.column("risk")?;
    let downside_risk_series = df.column("downside_risk")?;
    let drawdown_series = df.column("drawdown")?;
    let balance_series = df.column("balance")?;

    let success_rate = calculate_success_rate(returns_series)?;
    let risk = risk_series.mean().unwrap();
    let downside_deviation = downside_risk_series.mean().unwrap();

    let (max_drawdown, max_drawdown_duration) =
        calculate_max_drawdown_and_duration(start_series, end_series, drawdown_series)?;
    let risk_adjusted_return = calculate_risk_adjusted_returns(returns_series, risk_series)?;
    let sharpe_ratio = calculate_sharpe_ratio(returns_series, risk_series, risk_free_returns)?;

    let sortino_ratio =
        calculate_sortino_ratio(returns_series, downside_risk_series, risk_free_returns)?;
    let calmar_ratio = calculate_calmar_ratio(balance_series, max_drawdown)?;

    Ok(Statistics::new(
        success_rate,
        risk,
        downside_deviation,
        risk_adjusted_return,
        max_drawdown,
        max_drawdown_duration,
        sharpe_ratio,
        sortino_ratio,
        calmar_ratio,
    ))
}

fn calculate_success_rate(returns_series: &Series) -> Result<f64, Error> {
    let ca = returns_series.f64()?;
    let positive_count = ca.into_no_null_iter().filter(|&x| x > 0.0).count();
    let total_count = ca.into_no_null_iter().len();
    let success_rate = if total_count != 0 {
        positive_count as f64 / total_count as f64
    } else {
        0.0 as f64
    };

    Ok(round_down_nth_decimal(success_rate, 2))
}

fn calculate_risk_adjusted_returns(
    returns_series: &Series,
    risk_series: &Series,
) -> Result<f64, Error> {
    let returns_ca = returns_series.f64()?;
    let risk_ca = risk_series.f64()?;

    let risk_adjusted_returns = returns_ca
        .into_no_null_iter()
        .enumerate()
        .zip(risk_ca.into_no_null_iter())
        .fold(0.0, |acc, ((_index, returns), risk)| {
            let iteration = if risk != 0.0 { returns / risk } else { 0.0 };
            acc + iteration
        });
    Ok(risk_adjusted_returns)
}

fn calculate_sharpe_ratio(
    returns_series: &Series,
    risk_series: &Series,
    risk_free_returns: f64,
) -> Result<f64, Error> {
    let returns_ca = returns_series.f64()?;
    let risk_ca = risk_series.f64()?;

    let risk_adjusted_returns = returns_ca
        .into_no_null_iter()
        .zip(risk_ca.into_no_null_iter())
        .fold(-risk_free_returns, |acc, (returns, risk)| {
            let iteration = if risk != 0.0 { returns / risk } else { 0.0 };
            acc + iteration
        });
    Ok(risk_adjusted_returns)
}

fn calculate_sortino_ratio(
    returns_series: &Series,
    downside_risk_series: &Series,
    risk_free_returns: f64,
) -> Result<f64, Error> {
    let returns_ca = returns_series.f64()?;
    let downside_risk_ca = downside_risk_series.f64()?;

    let risk_adjusted_returns = returns_ca
        .into_no_null_iter()
        .zip(downside_risk_ca.into_no_null_iter())
        .fold(-risk_free_returns, |acc, (returns, risk)| {
            let iteration = if risk != 0.0 { returns / risk } else { 0.0 };
            acc + iteration
        });
    Ok(risk_adjusted_returns)
}

fn calculate_calmar_ratio(balance_series: &Series, max_drawdown: f64) -> Result<f64, Error> {
    if max_drawdown == 0.0 {
        Ok(0.0)
    } else {
        let balances_vec: Vec<f64> = balance_series.f64()?.into_no_null_iter().collect();
        let last_balance_index = balances_vec.len() - 1;
        let last_balance = balances_vec[last_balance_index];
        Ok(last_balance / max_drawdown)
    }
}

fn calculate_max_drawdown_and_duration(
    start_series: &Series,
    end_series: &Series,
    drawdown_series: &Series,
) -> Result<(f64, Duration), Error> {
    let start_vec = start_series
        .datetime()?
        .into_no_null_iter()
        .collect::<Vec<_>>();
    let end_vec = end_series
        .datetime()?
        .into_no_null_iter()
        .collect::<Vec<_>>();
    let drawdown_vec = drawdown_series
        .f64()
        .unwrap()
        .into_no_null_iter()
        .collect::<Vec<f64>>();
    if let Some((max_index, _)) = drawdown_vec
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
    {
        let max_drawdown = drawdown_vec[max_index];
        let max_drawdown_duration =
            Duration::milliseconds(end_vec[max_index] - start_vec[max_index]);

        Ok((max_drawdown, max_drawdown_duration))
    } else {
        Ok((0.0, Duration::minutes(0)))
    }
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
#[derive(Clone, Debug)]
pub struct Statistics {
    success_rate: f64,
    risk: f64,
    downside_deviation: f64,
    risk_adjusted_return: f64,
    max_drawdown: f64,
    max_drawdown_duration: Duration,
    sharpe_ratio: f64,
    sortino_ratio: f64,
    calmar_ratio: f64,
}

impl Statistics {
    fn new(
        success_rate: f64,
        risk: f64,
        downside_deviation: f64,
        risk_adjusted_return: f64,
        max_drawdown: f64,
        max_drawdown_duration: Duration,
        sharpe_ratio: f64,
        sortino_ratio: f64,
        calmar_ratio: f64,
    ) -> Self {
        Statistics {
            success_rate,
            risk,
            downside_deviation,
            risk_adjusted_return,
            max_drawdown,
            max_drawdown_duration,
            sharpe_ratio,
            sortino_ratio,
            calmar_ratio,
        }
    }
}

impl Default for Statistics {
    fn default() -> Self {
        Self {
            success_rate: 0.0,
            risk: 0.0,
            downside_deviation: 0.0,
            risk_adjusted_return: 0.0,
            max_drawdown: 0.0,
            max_drawdown_duration: Duration::minutes(0),
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
