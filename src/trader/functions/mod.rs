use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{Duration, NaiveDateTime, Timelike, Utc};
use hmac::{Hmac, Mac};
use polars::prelude::{Duration as PolarsDuration, *};
use reqwest::Client;
use sha2::Sha256;
use std::env::{self};
use std::string::FromUtf8Error;

use super::enums::balance::Balance;
use super::enums::order_action::OrderAction;
use super::enums::trade_status::TradeStatus;
use super::exchanges::binance::models::BinanceHttpKlineResponse;
use super::models::behavior_subject::BehaviorSubject;
use super::models::execution::Execution;
use super::models::tick_data::TickData;
use super::models::trade::Trade;
use super::traits::exchange::Exchange;
use super::{
    constants::{MINUTES_IN_DAY, NANOS_IN_SECOND, SECONDS_IN_MIN},
    errors::Error,
};

/// gives result with full seconds
pub fn current_datetime() -> NaiveDateTime {
    Utc::now().naive_utc().with_nanosecond(0).unwrap()
}

// gives current timestamp in milliseconds
pub fn current_timestamp_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
}

/// Gets the minute start timestamp for the input timestamp
///
/// # Arguments
/// * `use_milliseconds`- will receive / return the timestamp in milliseconds
/// * `timestamp` - Optional for getting timestamp start minute, by using None, the current timestamp will be used.
///
pub fn timestamp_minute_start(use_milliseconds: bool, timestamp: Option<i64>) -> i64 {
    let mut timestamp = match timestamp {
        Some(timestamp) => {
            if use_milliseconds {
                NaiveDateTime::from_timestamp_millis(timestamp)
                    .unwrap()
                    .timestamp()
            } else {
                NaiveDateTime::from_timestamp_millis(timestamp * 1_000)
                    .unwrap()
                    .timestamp()
            }
        }
        None => {
            let timestamp = Utc::now().naive_utc().timestamp();
            if use_milliseconds {
                timestamp * 1_000
            } else {
                timestamp
            }
        }
    };
    let remainder = if use_milliseconds {
        timestamp % 60_000
    } else {
        timestamp % 60
    };
    timestamp -= remainder;
    timestamp
}

/// Gets the minute end timestamp for the input timestamp
///
/// # Arguments
/// * `use_milliseconds`- will receive / return the timestamp in milliseconds
/// * `timestamp` - Optional for getting timestamp start minute, by using None, the current timestamp will be used.
///
pub fn timestamp_minute_end(use_milliseconds: bool, timestamp: Option<i64>) -> i64 {
    let mut timestamp = timestamp_minute_start(use_milliseconds, timestamp);
    if use_milliseconds {
        timestamp += 59_999
    } else {
        timestamp += 59
    }
    timestamp
}

fn current_minute_start() {
    let current_timestamp = current_timestamp_ms();
}

/// Gets Open, High, Low and Close labels for symbol.
///
/// # Arguments
///
/// * `symbol` - Columns symbol.
///
/// # Examples
///
/// ```
/// let result = get_symbol_ohlc_cols("BTC_USDT");
/// assert_eq!(result, ("BTCUSDT_open", "BTCUSDT_high", "BTCUSDT_close", "BTCUSDT_low"));
/// ```
///
/// # Returns
///
/// * `Open symbol` - Open symbol column.
/// * `High symbol` - High symbol column.
/// * `Low symbol` - Low symbol column.
/// * `Close symbol` - Close symbol column.
///

pub fn get_symbol_ohlc_cols(symbol: &String) -> (String, String, String, String) {
    let open_col = format!("{}_open", symbol);
    let high_col = format!("{}_high", symbol);
    let low_col = format!("{}_low", symbol);
    let close_col = format!("{}_close", symbol);
    return (open_col, high_col, low_col, close_col);
}

pub fn get_symbol_open_col(symbol: &String) -> String {
    format!("{}_open", symbol)
}

pub fn get_symbol_close_col(symbol: &String) -> String {
    format!("{}_close", symbol)
}

/// returns open, high, low and close windowed columns names
pub fn get_symbol_window_ohlc_cols(
    symbol: &String,
    window: &String,
) -> (String, String, String, String) {
    let open_col = format!("{}_{}_open", symbol, window);
    let high_col = format!("{}_{}_high", symbol, window);
    let low_col = format!("{}_{}_low", symbol, window);
    let close_col = format!("{}_{}_close", symbol, window);
    return (open_col, high_col, low_col, close_col);
}

pub fn concat_and_clean_lazyframes<L: AsRef<[LazyFrame]>>(
    lfs: L,
    filter_datetime: NaiveDateTime,
) -> Result<LazyFrame, Error> {
    let args = UnionArgs {
        parallel: true,
        rechunk: true,
        to_supertypes: false,
    };
    let result_lf = concat(lfs, args)?;

    let result_lf = result_lf.filter(
        col("start_time")
            .dt()
            .datetime()
            .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
            .gt(filter_datetime.timestamp() * 1000),
    );

    Ok(result_lf)
}

/// Works when all ticks have data the same height
pub fn consolidate_complete_tick_data_into_lf(
    symbols: &Vec<String>,
    tick_data: &Vec<TickData>,
    tick_data_schema: &Schema,
) -> Result<LazyFrame, Error> {
    let mut columns: Vec<Series> = vec![];
    let mut datetimes_vec = vec![];
    let mut empty_ticks: Vec<Option<f64>> = vec![];

    for tick in tick_data {
        if datetimes_vec.contains(&tick.start_time) {
            continue;
        };
        datetimes_vec.push(tick.start_time);
        empty_ticks.push(None::<f64>);
    }
    let mut symbol_timestamp_map = HashMap::new();
    for tick_data in tick_data {
        symbol_timestamp_map.insert(
            (tick_data.symbol.clone(), tick_data.start_time.timestamp()),
            tick_data,
        );
    }

    let date_col = "start_time";
    let mut series = Series::new(date_col, &datetimes_vec);
    series.set_sorted_flag(polars::series::IsSorted::Ascending);
    columns.push(series);
    let unique_symbols: HashSet<String> = symbols.iter().cloned().collect();
    // let unique_symbols: Vec<String> = unique_symbols.iter().cloned().collect();
    let mut opens_map = HashMap::new();
    let mut highs_map = HashMap::new();
    let mut closes_map = HashMap::new();
    let mut lows_map = HashMap::new();
    for symbol in &unique_symbols {
        let (open_col, high_col, low_col, close_col) = get_symbol_ohlc_cols(symbol);
        opens_map.insert(open_col.clone(), vec![] as Vec<Option<f64>>);
        highs_map.insert(high_col.clone(), vec![] as Vec<Option<f64>>);
        closes_map.insert(close_col.clone(), vec![] as Vec<Option<f64>>);
        lows_map.insert(low_col.clone(), vec![] as Vec<Option<f64>>);

        for datetime in &datetimes_vec {
            if let Some(tick) =
                symbol_timestamp_map.get(&(symbol.to_string(), datetime.timestamp()))
            {
                opens_map.get_mut(&open_col).unwrap().push(Some(tick.open));
                highs_map.get_mut(&high_col).unwrap().push(Some(tick.high));
                closes_map
                    .get_mut(&close_col)
                    .unwrap()
                    .push(Some(tick.close));
                lows_map.get_mut(&low_col).unwrap().push(Some(tick.low));
            } else {
                opens_map.get_mut(&open_col).unwrap().push(None::<f64>);
                highs_map.get_mut(&high_col).unwrap().push(None::<f64>);
                closes_map.get_mut(&close_col).unwrap().push(None::<f64>);
                lows_map.get_mut(&low_col).unwrap().push(None::<f64>);
            }
        }
    }

    let opens_cols: HashSet<String> = opens_map.keys().cloned().collect();
    let highs_cols: HashSet<String> = highs_map.keys().cloned().collect();
    let closes_cols: HashSet<String> = closes_map.keys().cloned().collect();
    let lows_cols: HashSet<String> = lows_map.keys().cloned().collect();
    let mut tick_data_cols = HashSet::new();
    tick_data_cols.extend(opens_cols.clone());
    tick_data_cols.extend(highs_cols.clone());
    tick_data_cols.extend(closes_cols.clone());
    tick_data_cols.extend(lows_cols.clone());
    let data_cols: HashSet<String> = tick_data_schema
        .iter()
        .map(|(x, _)| x.to_string())
        .collect();
    let mut non_tick_data_cols: HashSet<&String> = data_cols.difference(&tick_data_cols).collect();
    // removes start_time as it's not considered in tick_data_cols, and duplication of index column messes up resample
    non_tick_data_cols.remove(&"start_time".to_string());
    for (field, data_type) in tick_data_schema.iter() {
        let result = match field {
            start_time_col if start_time_col == "start_time" => continue,
            open_col if opens_cols.contains(&open_col.to_string()) => {
                Series::new(field, &opens_map.get(&field.to_string()).unwrap())
            }
            high_col if highs_cols.contains(&high_col.to_string()) => {
                Series::new(field, &highs_map.get(&field.to_string()).unwrap())
            }
            close_col if closes_cols.contains(&close_col.to_string()) => {
                Series::new(field, &closes_map.get(&field.to_string()).unwrap())
            }
            low_col if lows_cols.contains(&low_col.to_string()) => {
                Series::new(field, &lows_map.get(&field.to_string()).unwrap())
            }
            non_tick_data_col if non_tick_data_cols.contains(&non_tick_data_col.to_string()) => {
                Series::full_null(&field.to_string(), empty_ticks.len(), data_type)
            }
            _ => unreachable!("Unexpected value"),
        };
        columns.push(result);
    }

    let new_df = DataFrame::new(columns)?;
    let new_lf = new_df.lazy();

    Ok(new_lf)
}

pub fn map_tick_data_to_df(
    symbols: &Vec<String>,
    ticks_data: &Vec<TickData>,
    data_schema: &Schema,
    prev_bar: NaiveDateTime,
    bar_length: Duration,
) -> Result<DataFrame, Error> {
    let mut start_timestamp = prev_bar.timestamp();
    let end_timestamp = (prev_bar + bar_length).timestamp();
    let mut columns: Vec<Series> = vec![];
    let mut datetimes_vec = vec![];
    let mut empty_float_ticks = vec![];

    while start_timestamp < end_timestamp {
        datetimes_vec.push(NaiveDateTime::from_timestamp_opt(start_timestamp, 0).unwrap());
        start_timestamp += 1;
        empty_float_ticks.push(None::<f64>);
    }

    let mut symbol_timestamp_map = HashMap::new();
    for tick_data in ticks_data {
        symbol_timestamp_map.insert(
            (tick_data.symbol.clone(), tick_data.start_time.timestamp()),
            tick_data,
        );
    }

    let start_time_col = "start_time";
    let mut series = Series::new(start_time_col, &datetimes_vec);
    series.set_sorted_flag(polars::series::IsSorted::Ascending);
    columns.push(series);

    let unique_symbols: HashSet<String> = symbols.iter().cloned().collect();
    // let unique_symbols: Vec<String> = unique_symbols.iter().cloned().collect();
    let mut opens_map = HashMap::new();
    let mut highs_map = HashMap::new();
    let mut closes_map = HashMap::new();
    let mut lows_map = HashMap::new();
    for symbol in &unique_symbols {
        let (open_col, high_col, low_col, close_col) = get_symbol_ohlc_cols(symbol);
        opens_map.insert(open_col.clone(), vec![] as Vec<Option<f64>>);
        highs_map.insert(high_col.clone(), vec![] as Vec<Option<f64>>);
        closes_map.insert(close_col.clone(), vec![] as Vec<Option<f64>>);
        lows_map.insert(low_col.clone(), vec![] as Vec<Option<f64>>);

        for datetime in &datetimes_vec {
            if let Some(tick) =
                symbol_timestamp_map.get(&(symbol.to_string(), datetime.timestamp()))
            {
                opens_map.get_mut(&open_col).unwrap().push(Some(tick.open));
                highs_map.get_mut(&high_col).unwrap().push(Some(tick.open));
                closes_map
                    .get_mut(&close_col)
                    .unwrap()
                    .push(Some(tick.open));
                lows_map.get_mut(&low_col).unwrap().push(Some(tick.open));
            } else {
                opens_map.get_mut(&open_col).unwrap().push(None::<f64>);
                highs_map.get_mut(&high_col).unwrap().push(None::<f64>);
                closes_map.get_mut(&close_col).unwrap().push(None::<f64>);
                lows_map.get_mut(&low_col).unwrap().push(None::<f64>);
            }
        }
    }

    let opens_cols: HashSet<String> = opens_map.keys().cloned().collect();
    let highs_cols: HashSet<String> = highs_map.keys().cloned().collect();
    let closes_cols: HashSet<String> = closes_map.keys().cloned().collect();
    let lows_cols: HashSet<String> = lows_map.keys().cloned().collect();
    let mut tick_data_cols = HashSet::new();
    tick_data_cols.extend(opens_cols.clone());
    tick_data_cols.extend(highs_cols.clone());
    tick_data_cols.extend(closes_cols.clone());
    tick_data_cols.extend(lows_cols.clone());
    let data_cols: HashSet<String> = data_schema.iter().map(|(x, _)| x.to_string()).collect();
    let mut non_tick_data_cols: HashSet<&String> = data_cols.difference(&tick_data_cols).collect();
    // removes start_time as it's not considered in tick_data_cols, and duplication of index column messes up resample
    non_tick_data_cols.remove(&"start_time".to_string());
    for (field, data_type) in data_schema.iter() {
        let result = match field {
            start_time_col if start_time_col == "start_time" => continue,
            open_col if opens_cols.contains(&open_col.to_string()) => {
                Series::new(field, &opens_map.get(&field.to_string()).unwrap())
            }
            high_col if highs_cols.contains(&high_col.to_string()) => {
                Series::new(field, &highs_map.get(&field.to_string()).unwrap())
            }
            close_col if closes_cols.contains(&close_col.to_string()) => {
                Series::new(field, &closes_map.get(&field.to_string()).unwrap())
            }
            low_col if lows_cols.contains(&low_col.to_string()) => {
                Series::new(field, &lows_map.get(&field.to_string()).unwrap())
            }
            non_tick_data_col if non_tick_data_cols.contains(&non_tick_data_col.to_string()) => {
                Series::full_null(&field.to_string(), empty_float_ticks.len(), data_type)
            }
            _ => unreachable!("Unexpected value"),
        };
        columns.push(result);
    }

    let non_sampled_data_df = DataFrame::new(columns)?;
    let non_sampled_data_lf = non_sampled_data_df.lazy();
    let non_tick_data_cols_ordered: Vec<String> = data_schema
        .clone()
        .iter()
        .filter_map(|(col, _)| {
            if non_tick_data_cols.contains(&col.to_string()) {
                Some(col.to_string())
            } else {
                None
            }
        })
        .collect();

    let resampled_data = resample_tick_data_to_length(
        symbols,
        bar_length,
        non_sampled_data_lf,
        ClosedWindow::Left,
        Some(non_tick_data_cols_ordered),
    )?;

    let resampled_data = resampled_data
        .collect()?
        .fill_null(FillNullStrategy::Forward(None))?;

    Ok(resampled_data)
}

pub fn resample_tick_data_to_length(
    symbols: &Vec<String>,
    length: Duration,
    data_lf: LazyFrame,
    closed_window: ClosedWindow, // TODO: check why benchmark frame doesn't leave last minute tick
    nullable_cols_to_keep: Option<Vec<String>>,
) -> Result<LazyFrame, Error> {
    let length_in_seconds = length.num_seconds();
    let mut agg_expressions = vec![];
    for symbol in symbols {
        let (open_col, high_col, low_col, close_col) = get_symbol_ohlc_cols(symbol);

        let open = col(&open_col).drop_nulls().first().alias(&open_col);
        let high = col(&high_col).max().alias(&high_col);
        let close = col(&close_col).drop_nulls().last().alias(&close_col);
        let low = col(&low_col).min().alias(&low_col);
        agg_expressions.push(open);
        agg_expressions.push(high);
        agg_expressions.push(close);
        agg_expressions.push(low);
    }

    if let Some(nullable_cols) = nullable_cols_to_keep {
        nullable_cols
            .into_iter()
            .for_each(|nullable_col| agg_expressions.push(col(&nullable_col).last().keep_name()))
    }

    let resampled_data = data_lf
        .clone()
        .group_by_dynamic(
            col("start_time"),
            vec![],
            DynamicGroupOptions {
                start_by: StartBy::DataPoint,
                index_column: "start_time".into(),
                every: PolarsDuration::new(NANOS_IN_SECOND * length_in_seconds),
                period: PolarsDuration::new(NANOS_IN_SECOND * length_in_seconds),
                offset: PolarsDuration::new(0),
                truncate: true,
                include_boundaries: false,
                closed_window,
                check_sorted: false,
            },
        )
        .agg(agg_expressions);
    Ok(resampled_data)
}

// pub fn stack

// pub fn parse_ws_kline_into_tick_data(data: WsKlineResponse) -> Result<TickData, Error> {
//     let open = data.k.o.parse::<f64>()?;
//     let close = data.k.c.parse::<f64>()?;
//     let high = data.k.h.parse::<f64>()?;
//     let low = data.k.l.parse::<f64>()?;
//     let date = NaiveDateTime::from_timestamp_opt(data.E / 1000, 0).unwrap();
//     let result = TickData {
//         symbol: data.s,
//         date,
//         open,
//         close,
//         high,
//         low,
//     };
//     Ok(result)
// }

pub async fn fetch_data(
    http: &Client,
    symbol: &String,
    start_timestamp: &i64, // ms
    end_timestamp: &i64,   // ms
    limit: i64,            //Default 500; max 1000.
) -> Result<Vec<BinanceHttpKlineResponse>, Error> {
    if limit > 1000 || limit < 0 {
        panic!("Limit must be less than 1000. Currently is {}", limit);
    }
    let url = format!(
        "https://api3.binance.com/api/v3/klines?symbol={}&interval={}&startTime={}&endTime={}&limit={}",
        symbol, "1m", start_timestamp, end_timestamp, limit
    );
    println!(
        "{:?} | 🦴 Fetching {} data ({} records) for interval between {} and {}",
        current_datetime(),
        symbol,
        limit,
        NaiveDateTime::from_timestamp_millis(*start_timestamp).unwrap(),
        NaiveDateTime::from_timestamp_millis(*end_timestamp).unwrap()
    );
    let result: Vec<BinanceHttpKlineResponse> = http.get(url).send().await?.json().await?;
    Ok(result)
}

pub fn parse_http_kline_into_tick_data(
    symbol: String,
    data: &BinanceHttpKlineResponse,
) -> Result<TickData, Error> {
    let open = data.open.parse::<f64>()?;
    let close = data.close.parse::<f64>()?;
    let high = data.high.parse::<f64>()?;
    let low = data.low.parse::<f64>()?;
    let start_time = NaiveDateTime::from_timestamp_opt(data.timestamp / 1000, 0).unwrap();
    let result = TickData {
        symbol,
        start_time,
        open,
        close,
        high,
        low,
    };
    Ok(result)
}

pub fn timestamp_end_to_daily_timestamp_sec_intervals(
    initial_fetch_offset: i64,
    timestamp_end: i64, //secs
    days_for_analysis: i64,
    limit: i64,
    granularity: i64, // mins
) -> Vec<i64> {
    let mut timestamp_start = timestamp_end - (days_for_analysis * MINUTES_IN_DAY * SECONDS_IN_MIN);
    timestamp_start -= initial_fetch_offset * SECONDS_IN_MIN / granularity;

    let timestamp_step = limit * SECONDS_IN_MIN / granularity;

    stepped_range_inclusive(timestamp_start, timestamp_end, timestamp_step)
}

fn stepped_range_inclusive(start: i64, end: i64, step: i64) -> Vec<i64> {
    let mut next_val = start;
    let last_step = end - (end - start) % step; // Finds the starting point of the last step

    let mut result = Vec::new();

    while next_val <= end {
        result.push(next_val);

        // If the current value is the start of the last step and is not the end,
        // then the next value should be the end value.
        if next_val == last_step && next_val != end {
            next_val = end;
        } else {
            next_val += step;
        }
    }

    result
}

pub fn round_down_nth_decimal(num: f64, n: i32) -> f64 {
    let multiplier = 10.0_f64.powi(n);
    (num * multiplier).floor() / multiplier
}

pub fn count_decimal_places(value: f64) -> i32 {
    let decimal_str = value.to_string();

    if let Some(dot_idx) = decimal_str.find('.') {
        (decimal_str.len() - dot_idx - 1) as i32
    } else {
        0
    }
}

#[allow(dead_code)]
pub fn print_as_df(
    lf: &LazyFrame,
    identifier: String,
    select: Option<Vec<String>>,
    cols: Option<u8>,
    rows: Option<u8>,
) -> Result<(), Error> {
    let mut select_exprs: Vec<Expr> = vec![all()];
    if let Some(strs) = select {
        let exprs: Vec<Expr> = strs.iter().map(|name| col(name)).collect();
        select_exprs = exprs;
    }
    let mut max_rows = "20".to_string();
    if let Some(rows) = rows {
        max_rows = rows.to_string();
    }
    env::set_var("POLARS_FMT_MAX_ROWS", max_rows);
    let mut max_cols = "20".to_string();
    if let Some(cols) = cols {
        max_cols = cols.to_string();
    }
    env::set_var("POLARS_FMT_MAX_COLS", max_cols);

    let df = lf.clone().select(select_exprs).collect()?;
    let hr = "-".to_string().repeat(75);
    println!(
        "{} \n
        {} DF: \n
        {:?} \n
        {}",
        hr, identifier, df, hr
    );

    Ok(())
}

#[allow(dead_code)]
pub fn print_names(lf: &LazyFrame, identifier: String) -> Result<(), Error> {
    println!("PRINT NAMES {}", identifier);
    for name in lf.schema()?.iter_names() {
        println!("{:?}", name);
    }

    Ok(())
}

pub fn calculate_hmac(api_secret: &str, message: &str) -> Result<String, FromUtf8Error> {
    // Create an HMAC-SHA256 object with the provided secret key
    let mut mac =
        Hmac::<Sha256>::new_from_slice(api_secret.as_bytes()).expect("Invalid API secret length");

    // Update the HMAC object with the message
    mac.update(message.as_bytes());

    // Obtain the result of the HMAC computation as an array of bytes
    let result = mac.finalize();
    let signature = result
        .into_bytes()
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect::<String>();

    Ok(signature)
}

pub fn calculate_remainder(dividend: f64, divisor: f64) -> f64 {
    dividend.rem_euclid(divisor)
}

// #[test]
// fn test_floor() {
//     let numb: f64 = 2400.7154131931316;
//     let fdpta: f64 = calculate_remainder(numb, 1.0);

//     let fdpta2 = numb - (2400.7154131931316 as f64).rem_euclid(1.0);
//     print!("FLOOR {}, floor2 {}", fdpta, fdpta2);
// }

pub async fn update_position_data_on_faulty_exchange_ws(
    exchange_socket_error_arc: &Arc<Mutex<Option<i64>>>,
    exchange_listener: &BehaviorSubject<Box<dyn Exchange + Send + Sync>>,
    current_trade_listener: &BehaviorSubject<Option<Trade>>,
    update_balance_listener: &BehaviorSubject<Option<Balance>>,
    update_order_listener: &BehaviorSubject<Option<OrderAction>>,
    update_executions_listener: &BehaviorSubject<Vec<Execution>>,
) -> Result<(), Error> {
    let exchange_binding = exchange_listener.value();
    // get if there was an error
    let mut last_error_ts = None;
    {
        let last_error_guard = exchange_socket_error_arc.lock().unwrap();
        if let Some(last_error) = *last_error_guard {
            last_error_ts = Some(last_error);
        }
    }

    let current_trade = current_trade_listener.value();
    // check for last_error_ts
    if let Some(last_error_ts) = last_error_ts {
        println!(
            "{} | update_position_data_on_faulty_exchange_ws -> last error ts = {}",
            current_timestamp_ms(),
            last_error_ts
        );
        // if it exists, check for current trade
        if let Some(current_trade) = current_trade {
            let current_trade_status = current_trade.status();
            match current_trade_status {
                TradeStatus::PendingCloseOrder | TradeStatus::Closed | TradeStatus::Cancelled => {
                    // in this case, no close order was opened (as when a close order is open, current_trade is updated as soon as it finishes http query)
                    // also, it doesn't make sense to fetch data from finished trades (closed/cancelled)
                }
                _ => {
                    let current_order_uuid = current_trade.get_active_order_uuid()
                        .expect(&format!("update_position_data_on_faulty_exchange_ws -> missing current order uuid. trade = {:?}", &current_trade));
                    let start_timestamp = last_error_ts;
                    let current_minute_end_timestamp = timestamp_minute_end(true, None);

                    let interim_executions = match exchange_binding
                        .fetch_order_executions(
                            current_order_uuid,
                            start_timestamp,
                            current_minute_end_timestamp,
                        )
                        .await
                    {
                        Ok(executions) => executions,
                        Err(error) => {
                            println!("update_position_data_on_faulty_exchange_ws -> executions error {:?}", error);
                            vec![]
                        }
                    };

                    let current_trade_status = current_trade.status();
                    // if executions were updated, order was too, but in case order could be cancelled (OrderStatus::New), we check for order updates
                    if interim_executions.len() > 0 || current_trade_status == TradeStatus::New {
                        // if executions were updated, we emit new executions action
                        if interim_executions.len() > 0 {
                            // let update_action = UpdateAction::Executions(interim_executions);
                            update_executions_listener.next(interim_executions);
                        }
                        let current_order_id = current_trade.get_active_order_id()
                            .expect(&format!("update_position_data_on_faulty_exchange_ws -> missing current order id. trade = {:?}", &current_trade));

                        // get order without executions
                        match exchange_binding
                            .fetch_opened_order(current_order_id.clone(), false)
                            .await
                        {
                            Ok(updated_order) => {
                                let order_action = if updated_order.is_cancel_order() {
                                    OrderAction::Cancel(updated_order)
                                } else {
                                    OrderAction::Update(updated_order)
                                };
                                // let update_action = UpdateAction::Order(Some(order_action));
                                update_order_listener.next(Some(order_action));
                            }
                            Err(error) => {
                                println!("handle_exchange_websocket -> fetch_opened_order failed {:?}. Error = {:?}", current_trade_status, error);
                            }
                        }
                    }
                }
            }
        }
    } else {
        // get open trade and set if any is open
        let trade = exchange_binding.fetch_current_position_trade().await?;
        if trade.is_some() {
            println!(
                "{:?} | A initial trade was found! {:?}",
                current_datetime(),
                trade.clone().unwrap()
            );
        }
        current_trade_listener.next(trade);
    }

    // regardless of error, usdt balance must be updated
    let balance = exchange_binding
        .fetch_current_usdt_balance()
        .await
        .expect("set_current_balance_handle -> get_current_usdt_balance error");
    // let update_action = UpdateAction::Balance(balance);
    update_balance_listener.next(Some(balance));
    Ok(())
}

pub fn closest_multiple_below(of: f64, to: f64) -> f64 {
    // println!("@@@@ closest_multiple_below of {}, to {} ", of, to);
    let quotient = to / of;
    let floored = quotient.floor();
    floored * of
}
