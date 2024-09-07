use chrono::NaiveDateTime;
use common::{
    constants::DAY_IN_MS,
    enums::trading_data_update::TradingDataUpdate,
    functions::{
        csv::{get_current_env_log_path, save_csv},
        current_datetime,
        performance::{
            calculate_calmar_ratio, calculate_max_drawdown_and_duration,
            calculate_risk_adjusted_returns, calculate_sharpe_ratio, calculate_sortino_ratio,
            calculate_success_rate,
        },
        round_down_nth_decimal,
    },
    structs::{BehaviorSubject, Statistics, Symbol, TradingSettings},
};
use glow_error::GlowError;
use polars::prelude::*;
use reqwest::Client;
use std::sync::Mutex;
use tokio::{spawn, task::JoinHandle};
use tokio_stream::StreamExt;

#[derive(Clone)]
pub struct Performance {
    benchmark_stats: Arc<Mutex<Statistics>>,
    _http: Client,
    risk_free_returns: f64,
    initial_datetime: NaiveDateTime,
    traded_data_listener: BehaviorSubject<TradingDataUpdate>,
    trading_stats: Arc<Mutex<Statistics>>,
    trading_settings: Arc<Mutex<TradingSettings>>,
}

impl Performance {
    pub fn new(
        trading_settings: &Arc<Mutex<TradingSettings>>,
        traded_data_listener: &BehaviorSubject<TradingDataUpdate>,
    ) -> Self {
        Self {
            benchmark_stats: Arc::new(Mutex::new(Statistics::default())),
            _http: Client::new(),
            risk_free_returns: 0.0,
            initial_datetime: current_datetime(),
            traded_data_listener: traded_data_listener.clone(),
            trading_stats: Arc::new(Mutex::new(Statistics::default())),
            trading_settings: trading_settings.clone(),
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
    fn _set_risk_free_daily_returns(&mut self, returns: f64) {
        self.risk_free_returns = returns;
    }

    fn set_benchmark_stats(&self, benchmark_trading_df: DataFrame) -> Result<(), GlowError> {
        let traded_symbol;
        let anchor_symbol;
        {
            let settings = self.trading_settings.lock().expect("trading settings lock");
            traded_symbol = settings.get_traded_symbol();
            anchor_symbol = settings.get_anchor_symbol();
        }

        let journey_formmated_datetime_start =
            self.initial_datetime.format("%H:%M-%d-%m-%Y").to_string();

        let trading_journey_identifier = format!(
            "{}_{}_{}",
            journey_formmated_datetime_start, anchor_symbol.name, traded_symbol.name
        );

        let path = get_current_env_log_path();
        let file_name = format!("{}_benchmark_data.csv", trading_journey_identifier);
        save_csv(path.clone(), file_name, &benchmark_trading_df, true)?;

        let benchmark_trading_lf = benchmark_trading_df.lazy();
        let (benchmark_data, benchmark_stats) =
            calculate_benchmark_data(benchmark_trading_lf, self.risk_free_returns, traded_symbol)?;
        {
            let mut lock = self.benchmark_stats.lock().unwrap();
            *lock = benchmark_stats;
        };

        let file_name = format!("{}_benchmark_trades.csv", trading_journey_identifier);
        save_csv(path, file_name, &benchmark_data, true)?;

        Ok(())
    }

    fn handle_initial_traded_data(&self, benchmark_trading_df: DataFrame) {
        match self.set_benchmark_stats(benchmark_trading_df) {
            Ok(()) => {}
            Err(error) => {
                println!("set_benchmark_stats error {:?}", error);
            }
        }
    }

    fn update_trading_stats(&self, traded_data: DataFrame) -> Result<(), GlowError> {
        let traded_symbol;
        let anchor_symbol;
        {
            let settings = self.trading_settings.lock().expect("trading settings lock");
            traded_symbol = settings.get_traded_symbol();
            anchor_symbol = settings.get_anchor_symbol();
        }
        let journey_formmated_datetime_start =
            self.initial_datetime.format("%H:%M-%d-%m-%Y").to_string();

        let trading_journey_identifier = format!(
            "{}_{}_{}",
            journey_formmated_datetime_start, anchor_symbol.name, traded_symbol.name
        );
        let trading_journey_start = self.initial_datetime.timestamp_millis();
        let filter_mask = traded_data
            .column("start_time")?
            .gt_eq(trading_journey_start)?;
        let trading_data = traded_data.filter(&filter_mask)?;
        let path = get_current_env_log_path();
        let file_name = format!("{}_trading_data.csv", trading_journey_identifier);
        save_csv(path.clone(), file_name, &trading_data, true)?;
        let (trading_data, trading_stats) = update_trading_data(
            &trading_data,
            self.risk_free_returns,
            traded_symbol,
            Some(self.initial_datetime),
        )?;
        {
            let mut lock = self.trading_stats.lock().unwrap();
            *lock = trading_stats;
        };
        let file_name = format!("{}_trades.csv", trading_journey_identifier);
        save_csv(path, file_name, &trading_data, true)?;
        Ok(())
    }

    fn handle_market_traded_data(&self, traded_data: DataFrame) {
        match self.update_trading_stats(traded_data) {
            Ok(()) => {}
            Err(error) => {
                println!("update_trading_stats error {:?}", error);
            }
        }
    }

    fn init_traded_data_handler(&self) -> JoinHandle<()> {
        let performance = self.clone();
        spawn(async move {
            let mut subscription = performance.traded_data_listener.subscribe();

            while let Some(traded_data) = subscription.next().await {
                match traded_data {
                    TradingDataUpdate::Initial(benchmark_trading_df) => {
                        performance.handle_initial_traded_data(benchmark_trading_df)
                    }
                    TradingDataUpdate::Market(traded_data) => {
                        performance.handle_market_traded_data(traded_data)
                    }
                    _ => {}
                }
            }
        })
    }

    pub fn init(self) {
        self.init_traded_data_handler();
    }
}

// TODO: move this inside performance impl
pub fn calculate_benchmark_data(
    benchmark_trading_data: LazyFrame,
    risk_free_returns: f64,
    traded_symbol: &Symbol,
) -> Result<(DataFrame, Statistics), GlowError> {
    let trades_lf = calculate_trades(benchmark_trading_data)?;

    let mut trading_lf = calculate_trading_sessions(trades_lf, traded_symbol, None)?;
    trading_lf = trading_lf.drop_nulls(None);

    let df = trading_lf.collect()?;

    let benchmark_stats = calculate_trading_stats(&df, risk_free_returns)?;

    println!("\n📋 Benchmark stats \n{}", benchmark_stats);

    Ok((df, benchmark_stats))
}

pub fn update_trading_data(
    trading_data: &DataFrame,
    risk_free_returns: f64,
    traded_symbol: &Symbol,
    log_from_timestamp_on: Option<NaiveDateTime>,
) -> Result<(DataFrame, Statistics), GlowError> {
    let trading_data_lf = trading_data.clone().lazy();
    let trades_lf = calculate_trades(trading_data_lf)?;

    let mut trading_lf =
        calculate_trading_sessions(trades_lf, traded_symbol, log_from_timestamp_on)?;
    trading_lf = trading_lf.drop_nulls(None);

    let df = trading_lf.collect()?;

    let trading_stats = calculate_trading_stats(&df, risk_free_returns)?;

    Ok((df, trading_stats))
}

pub fn calculate_trades(lf: LazyFrame) -> Result<LazyFrame, GlowError> {
    let lf = lf.with_columns([when(
        col("position")
            .shift(1)
            .is_not_null()
            .and(col("position").neq(col("position").shift(1))),
    )
    .then(1)
    .otherwise(0)
    .alias("trade")]);

    Ok(lf)
}

pub fn calculate_trading_sessions(
    lf: LazyFrame,
    traded_symbol: &Symbol,
    log_from_timestamp_on: Option<NaiveDateTime>,
) -> Result<LazyFrame, GlowError> {
    let mut lf = lf.clone();

    if log_from_timestamp_on.is_some() {
        let log_from_timestamp_on = log_from_timestamp_on.unwrap();
        let mut log_from_timestamp_on = log_from_timestamp_on.timestamp_millis();
        log_from_timestamp_on = log_from_timestamp_on - DAY_IN_MS;
        lf = lf.filter(col("start_time").gt_eq(lit(log_from_timestamp_on)));
    }

    lf = lf
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
    let (open_col, high_col, low_col, close_col) = traded_symbol.get_ohlc_cols();

    // let file_name = "trades_lf.csv".to_string();
    // let trades_df = lf.clone().collect()?;
    // save_csv(path, file_name, &trades_df, true)?;
    let returns_output: SpecialEq<Arc<dyn FunctionOutputField>> =
        GetOutput::from_type(DataType::Float64);

    let aggs = [
        col("start_time").first().alias("start"),
        col("start_time").last().alias("end"),
        col(&open_col).first().alias("start_price"),
        col(&close_col).last().alias("end_price"),
        col(&low_col).min().alias("min_price"),
        col(&high_col).max().alias("max_price"),
        col("action").last().alias("close_signal"),
        col("position").first().alias("position"),
        col("returns").last().keep_name(),
        col("returns").max().alias("max_returns"),
        col("returns").min().alias("min_returns"),
        (when(col("returns").last().eq(lit(0)))
            .then(lit(0))
            .otherwise(
                when(col("returns").last().gt(lit(0)))
                    .then(col("returns").last() / col("returns").max())
                    .otherwise(col("returns").last() / col("returns").min()),
            ))
        .alias("returns_seized"),
        col("units").mean().keep_name(),
        col("profit_and_loss").last().keep_name(),
        col("balance").last().keep_name(),
        col("returns").std(0).alias("risk"),
        col("trade_fees").sum().keep_name(),
        col("returns")
            .apply_many(
                |series| {
                    let positions_series: &Series = &series[1];
                    let returns_series: &Series = &series[0];
                    let position = positions_series.head(Some(1)).mean().unwrap_or_default();

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
        .group_by([col("session")])
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
    // let path = "data/test".to_string();
    // let file_name = "trades_lf.csv".to_string();
    // let trades_df = lf.clone().collect()?;
    // save_csv(path, file_name, &trades_df, true)?;

    Ok(lf)
}

pub fn calculate_trading_stats(
    trading_data: &DataFrame,
    risk_free_returns: f64,
) -> Result<Statistics, GlowError> {
    let initial_data_filter_mask = trading_data.column("position")?.not_equal(0)?;
    let df = trading_data.filter(&initial_data_filter_mask)?;

    let start_series = df.column("start")?;
    let end_series = df.column("end")?;
    let returns_series = df.column("returns")?;
    let risk_series = df.column("risk")?;
    let downside_risk_series = df.column("downside_risk")?;
    let drawdown_series = df.column("drawdown")?;
    let balance_series = df.column("balance")?;
    let current_balance = df.column("balance")?.f64()?.tail(Some(1)).to_vec()[0].unwrap();
    let current_balance = round_down_nth_decimal(current_balance, 6);

    let success_rate = calculate_success_rate(returns_series)?;

    let risk = risk_series.mean().unwrap_or_default();
    let downside_deviation = downside_risk_series.mean().unwrap_or_default();

    let (max_drawdown, max_drawdown_duration) =
        calculate_max_drawdown_and_duration(start_series, end_series, drawdown_series)?;
    let risk_adjusted_return = calculate_risk_adjusted_returns(returns_series, risk_series)?;
    let sharpe_ratio = calculate_sharpe_ratio(returns_series, risk_series, risk_free_returns)?;

    let sortino_ratio =
        calculate_sortino_ratio(returns_series, downside_risk_series, risk_free_returns)?;
    let calmar_ratio = calculate_calmar_ratio(balance_series, max_drawdown)?;

    Ok(Statistics::new(
        success_rate,
        current_balance,
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

// #[allow(dead_code)]
// #[derive(Debug, Deserialize)]
// pub struct HttpTreasuryResponseWrapper<T> {
//     data: Vec<T>,
//     meta: HttpTreasuryResponseMeta<T>,
//     links: HttpTreasuryResponseLinks,
// }
// #[allow(dead_code)]
// #[derive(Debug, Deserialize)]
// pub struct TreasuryRateResponse {
//     record_date: String,
//     security_desc: String,
//     avg_interest_rate_amt: String,
// }
// #[allow(dead_code)]
// #[derive(Debug, Deserialize)]
// pub struct HttpTreasuryResponseMeta<T> {
//     count: i32,
//     labels: T,
//     #[serde(rename = "dataTypes")]
//     data_types: T,
//     #[serde(rename = "dataFormats")]
//     data_formats: T,
//     #[serde(rename = "total-count")]
//     total_count: i32,
//     #[serde(rename = "total-pages")]
//     total_pages: i32,
// }

// #[allow(dead_code)]
// #[derive(Debug, Deserialize)]
// pub struct HttpTreasuryResponseLinks {
//     #[serde(rename = "self")]
//     self_: String,
//     first: String,
//     prev: Option<String>,
//     next: Option<String>,
//     last: String,
// }
