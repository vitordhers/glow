use std::collections::{HashMap, HashSet};

use chrono::{Duration, NaiveDateTime, Timelike, Utc};
use hmac::{Hmac, Mac};
use polars::prelude::{Duration as PolarsDuration, *};
use reqwest::Client;
use sha2::Sha256;
use std::env::{self};
use std::string::FromUtf8Error;

use super::{
    constants::{MINUTES_IN_DAY, NANOS_IN_SECOND, SECONDS_IN_MIN},
    errors::Error,
    models::{HttpKlineResponse, TickData, WsKlineResponse},
};

pub fn current_datetime() -> NaiveDateTime {
    Utc::now().naive_utc().with_nanosecond(0).unwrap()
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
    let result_lf = concat(lfs, true, true)?;

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
        if datetimes_vec.contains(&tick.date) {
            continue;
        };
        datetimes_vec.push(tick.date);
        empty_ticks.push(None::<f64>);
    }

    let date_col = "start_time";
    columns.push(Series::new(date_col, &datetimes_vec));
    let unique_symbols: HashSet<String> = symbols.iter().cloned().collect();
    for symbol in unique_symbols {
        let (open_col, high_col, low_col, close_col) = get_symbol_ohlc_cols(&symbol);

        let mut opens = vec![];
        let mut highs = vec![];
        let mut closes = vec![];
        let mut lows = vec![];

        let symbol_ticks: Vec<TickData> = tick_data
            .iter()
            .filter(|&tick| tick.symbol == symbol.to_string())
            .cloned()
            .collect();

        for tick in symbol_ticks {
            opens.push(tick.open);
            highs.push(tick.high);
            closes.push(tick.close);
            lows.push(tick.low);
        }
        for (field, _) in tick_data_schema.iter() {
            let result = match field {
                _open_col if _open_col == open_col.as_str() => Series::new(field, &opens),
                _high_col if _high_col == high_col.as_str() => Series::new(field, &highs),
                _close_col if _close_col == close_col.as_str() => Series::new(field, &closes),
                _low_col if _low_col == low_col.as_str() => Series::new(field, &lows),
                _ => continue,
            };
            columns.push(result);
        }
    }

    let new_df = DataFrame::new(columns)?;
    let new_lf = new_df.lazy();

    Ok(new_lf)
}

pub fn map_tick_data_to_data_lf(
    symbols: &Vec<String>,
    tick_data_vec: Vec<TickData>,
    data_schema: &Schema,
    current_last_bar: &NaiveDateTime,
    bar_length: &Duration,
) -> Result<DataFrame, Error> {
    // set base df from current_last_bar to current_last_bar + bar_length
    let bar_length_in_seconds = bar_length.num_seconds();
    let end_timestamp = current_last_bar.timestamp() + bar_length_in_seconds;
    let mut current_timestamp = current_last_bar.timestamp();
    let mut columns: Vec<Series> = vec![];
    let mut datetimes_vec = vec![];
    let mut empty_float_ticks = vec![];

    while current_timestamp <= end_timestamp {
        datetimes_vec.push(NaiveDateTime::from_timestamp_opt(current_timestamp, 0).unwrap());
        current_timestamp += 1;
        empty_float_ticks.push(None::<f64>);
    }

    let mut symbol_timestamp_map = HashMap::new();
    for tick_data in tick_data_vec {
        symbol_timestamp_map.insert(
            (tick_data.symbol.clone(), tick_data.date.timestamp()),
            tick_data,
        );
    }

    let date_col = "start_time";
    columns.push(Series::new(date_col, &datetimes_vec));

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

    let df = DataFrame::new(columns)?;

    let data_lf = df.lazy();

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
        data_lf,
        ClosedWindow::Right,
        Some(non_tick_data_cols_ordered),
    )?;

    let resampled_data = resampled_data
        .collect()?
        .fill_null(FillNullStrategy::Forward(None))?;

    Ok(resampled_data)
}

pub fn resample_tick_data_to_length(
    symbols: &Vec<String>,
    length: &Duration,
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
        .groupby_dynamic(
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
            },
        )
        .agg(agg_expressions);
    Ok(resampled_data)
}

// pub fn stack

pub fn parse_ws_kline_into_tick_data(data: WsKlineResponse) -> Result<TickData, Error> {
    let open = data.k.o.parse::<f64>()?;
    let close = data.k.c.parse::<f64>()?;
    let high = data.k.h.parse::<f64>()?;
    let low = data.k.l.parse::<f64>()?;
    let date = NaiveDateTime::from_timestamp_opt(data.E / 1000, 0).unwrap();
    let result = TickData {
        symbol: data.s,
        date,
        open,
        close,
        high,
        low,
    };
    Ok(result)
}

pub fn clear_tick_data_to_last_bar(
    tick_data_vec: Vec<TickData>,
    last_bar: &NaiveDateTime,
) -> Vec<TickData> {
    tick_data_vec
        .iter()
        .filter(|&tick_data| tick_data.date >= *last_bar)
        .cloned()
        .collect()
}

pub async fn fetch_data(
    http: &Client,
    symbol: &String,
    start_timestamp: &i64, // ms
    end_timestamp: &i64,   // ms
    limit: i64,            //Default 500; max 1000.
) -> Result<Vec<HttpKlineResponse>, Error> {
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
    let result: Vec<HttpKlineResponse> = http.get(url).send().await?.json().await?;
    Ok(result)
}

pub fn parse_http_kline_into_tick_data(
    symbol: String,
    data: &HttpKlineResponse,
) -> Result<TickData, Error> {
    let open = data.open.parse::<f64>()?;
    let close = data.close.parse::<f64>()?;
    let high = data.high.parse::<f64>()?;
    let low = data.low.parse::<f64>()?;
    let date = NaiveDateTime::from_timestamp_opt(data.timestamp / 1000, 0).unwrap();
    let result = TickData {
        symbol,
        date,
        open,
        close,
        high,
        low,
    };
    Ok(result)
}

pub fn timestamp_end_to_daily_timestamp_sec_intervals(
    timestamp_end: i64, //secs
    limit: i64,
    granularity: i64, // mins
) -> Vec<i64> {
    let timestamp_start = timestamp_end - (MINUTES_IN_DAY * SECONDS_IN_MIN);
    let step = limit * SECONDS_IN_MIN / granularity;

    (timestamp_start..=timestamp_end)
        .step_by(step as usize)
        .collect()
}

pub fn round_down_nth_decimal(num: f64, n: i32) -> f64 {
    let multiplier = 10.0_f64.powi(n);
    (num * multiplier).floor() / multiplier
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
