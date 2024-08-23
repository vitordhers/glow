use chrono::{Duration, NaiveDateTime, Timelike, Utc};
use glow_error::GlowError;
use hmac::{Hmac, Mac};
use polars::{
    prelude::{Duration as PolarsDuration, *},
    series::IsSorted,
};
use sha2::Sha256;
use std::{
    collections::{HashMap, HashSet},
    env::{self},
    string::FromUtf8Error,
};

pub mod csv;

use crate::{
    constants::{MINUTES_IN_DAY, NANOS_IN_SECOND, SECONDS_IN_MIN},
    r#static::SYMBOLS_MAP,
    structs::{Symbol, TickData},
};

/// gives result with full seconds
pub fn current_datetime() -> NaiveDateTime {
    Utc::now().naive_utc().with_nanosecond(0).unwrap()
}

#[inline]
pub fn current_timestamp() -> i64 {
    Utc::now().with_nanosecond(0).unwrap().timestamp()
}

// gives current timestamp in milliseconds
pub fn current_timestamp_ms() -> i64 {
    Utc::now().with_nanosecond(0).unwrap().timestamp_millis()
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

// TODO: DEPRECATE THIS
// pub fn get_symbol_ohlc_cols(
//     symbol: &str,
// ) -> (&'static str, &'static str, &'static str, &'static str) {
//     let Symbol {
//         name: _,
//         open,
//         high,
//         low,
//         close,
//     } = SYMBOLS_MAP
//         .get(symbol)
//         .expect(&format!("symbol {} to exist", symbol));
//     return (open, high, low, close);
// }

// TODO: deprecate this
pub fn get_symbol_open_col(symbol: &str) -> &'static str {
    let symbol = SYMBOLS_MAP
        .get(symbol)
        .expect(&format!("symbol {} to exist", symbol));
    symbol.open
}

// TODO: deprecate this
pub fn get_symbol_close_col(symbol: &str) -> &'static str {
    let symbol = SYMBOLS_MAP
        .get(symbol)
        .expect(&format!("symbol {} to exist", symbol));
    symbol.close
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
) -> Result<LazyFrame, GlowError> {
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

// TODO: DEPRECATE THIS
fn map_ticks_data_to_df(
    unique_symbols: &Vec<&Symbol>,
    ticks_data: &Vec<TickData>,
    schema_to_comply: &Schema,
) -> Result<DataFrame, GlowError> {
    let mut unique_timestamps = HashSet::new();
    let mut symbol_ts_ticks_map = HashMap::new();
    for tick in ticks_data {
        let ts = tick.start_time.timestamp_millis();
        symbol_ts_ticks_map.insert((tick.symbol, ts), tick);
        unique_timestamps.insert(ts);
    }

    let records_no = unique_timestamps.len();
    let timestamps_col = "start_time";
    let timestamps = unique_timestamps.into_iter().collect::<Vec<i64>>();
    let timestamps_series = Series::new(timestamps_col, &timestamps);
    let mut timestamps_series = timestamps_series.sort(false);
    timestamps_series.set_sorted_flag(IsSorted::Ascending);

    let mut open_cols_map = HashMap::new();
    let mut high_cols_map = HashMap::new();
    let mut close_cols_map = HashMap::new();
    let mut low_cols_map = HashMap::new();

    for symbol in unique_symbols {
        let (o, h, l, c) = symbol.get_ohlc_cols();
        open_cols_map.insert(o, vec![] as Vec<Option<f64>>);
        high_cols_map.insert(h, vec![] as Vec<Option<f64>>);
        close_cols_map.insert(l, vec![] as Vec<Option<f64>>);
        low_cols_map.insert(c, vec![] as Vec<Option<f64>>);

        for timestamp in &timestamps {
            if let Some(tick) = symbol_ts_ticks_map.get(&(symbol.name, *timestamp)) {
                open_cols_map.get_mut(&o).unwrap().push(Some(tick.open));
                high_cols_map.get_mut(&h).unwrap().push(Some(tick.high));
                close_cols_map.get_mut(&c).unwrap().push(Some(tick.close));
                low_cols_map.get_mut(&l).unwrap().push(Some(tick.low));
                continue;
            }
            open_cols_map.get_mut(&o).unwrap().push(None::<f64>);
            high_cols_map.get_mut(&h).unwrap().push(None::<f64>);
            close_cols_map.get_mut(&c).unwrap().push(None::<f64>);
            low_cols_map.get_mut(&l).unwrap().push(None::<f64>);
        }
    }

    let opens_cols: HashSet<&str> = open_cols_map.keys().cloned().collect();
    let highs_cols: HashSet<&str> = high_cols_map.keys().cloned().collect();
    let closes_cols: HashSet<&str> = close_cols_map.keys().cloned().collect();
    let lows_cols: HashSet<&str> = low_cols_map.keys().cloned().collect();
    let mut ticks_data_cols = HashSet::new();

    ticks_data_cols.extend(&opens_cols);
    ticks_data_cols.extend(&highs_cols);
    ticks_data_cols.extend(&closes_cols);
    ticks_data_cols.extend(&lows_cols);

    let schema_cols: HashSet<&str> = schema_to_comply.iter().map(|(x, _)| x.as_str()).collect();
    let mut non_ticks_data_cols: HashSet<&str> =
        schema_cols.difference(&ticks_data_cols).cloned().collect();
    // removes start_time to avoid condition collision
    non_ticks_data_cols.remove(&"start_time");

    let mut df_series: Vec<Series> = vec![];

    for (field, data_type) in schema_to_comply.iter() {
        let result = match field {
            start_time_col if start_time_col == "start_time" => {
                timestamps_series.cast(&DataType::Datetime(TimeUnit::Milliseconds, None))?
            }
            open_col if opens_cols.contains(open_col.as_str()) => {
                Series::new(field, &open_cols_map.get(field.as_str()).unwrap())
            }
            high_col if highs_cols.contains(high_col.as_str()) => {
                Series::new(field, &high_cols_map.get(field.as_str()).unwrap())
            }
            close_col if closes_cols.contains(close_col.as_str()) => {
                Series::new(field, &close_cols_map.get(field.as_str()).unwrap())
            }
            low_col if lows_cols.contains(low_col.as_str()) => {
                Series::new(field, &low_cols_map.get(field.as_str()).unwrap())
            }
            non_ticks_data_col if non_ticks_data_cols.contains(non_ticks_data_col.as_str()) => {
                Series::full_null(field, records_no, data_type)
            }
            _ => unreachable!("Unexpected value"),
        };
        df_series.push(result);
    }

    Ok(DataFrame::new(df_series)?.fill_null(FillNullStrategy::Forward(None))?)
}

pub fn map_and_downsample_ticks_data_to_df2(
    ticks_data: &Vec<TickData>,
    unique_symbols: &Vec<&Symbol>,
    kline_duration: Duration,
    kline_data_schema: &Schema,
    schema_to_comply: Option<&Schema>,
) -> Result<DataFrame, GlowError> {
    assert!(
        unique_symbols.len() > 0,
        "unique symbols must have length > 0"
    );
    let symbols_set: HashSet<&Symbol> = unique_symbols.iter().cloned().collect();
    assert!(
        symbols_set.len() == unique_symbols.len(),
        "symbols must be unique"
    );
    assert!(
        ticks_data.len() % unique_symbols.len() == 0,
        "tick data is asymmetric"
    );

    let non_sampled_ticks_data_df = map_ticks_data_to_kline_df(ticks_data, kline_data_schema)?;
    let non_sampled_ticks_data_lf = non_sampled_ticks_data_df.lazy();

    let resampled_data = downsample_tick_lf_to_kline_duration(
        unique_symbols,
        kline_duration,
        non_sampled_ticks_data_lf,
        ClosedWindow::Left,
        schema_to_comply,
    )?;

    let resampled_data = resampled_data
        .collect()?
        .fill_null(FillNullStrategy::Forward(None))?;

    Ok(resampled_data)
}

fn map_ticks_data_to_kline_df(
    ticks_data: &Vec<TickData>,
    kline_data_schema: &Schema,
) -> Result<DataFrame, GlowError> {
    let mut timestamps = HashSet::new();
    let mut data = HashMap::new();

    for tick in ticks_data {
        timestamps.insert(tick.start_time);

        data.entry(format!("{}_open", tick.symbol))
            .or_insert(Vec::new())
            .push(tick.open);
        data.entry(format!("{}_high", tick.symbol))
            .or_insert(Vec::new())
            .push(tick.high);
        data.entry(format!("{}_low", tick.symbol))
            .or_insert(Vec::new())
            .push(tick.low);
        data.entry(format!("{}_close", tick.symbol))
            .or_insert(Vec::new())
            .push(tick.close);
    }

    let df_series: Vec<Series> = kline_data_schema
        .iter()
        .map(move |(field, _)| {
            if field == "start_time" {
                Series::new(
                    "start_time",
                    timestamps
                        .clone()
                        .into_iter()
                        .collect::<Vec<NaiveDateTime>>(),
                )
            } else {
                Series::new(field, &data.get(field.as_str()).unwrap())
            }
        })
        .collect::<Vec<Series>>();

    Ok(DataFrame::new(df_series)?.fill_null(FillNullStrategy::Forward(None))?)
}

// TODO: deprecate this
pub fn map_and_downsample_ticks_data_to_df(
    schema_to_comply: &Schema,
    kline_duration: Duration,
    ticks_data: &Vec<TickData>,
    unique_symbols: &Vec<&Symbol>,
    maintain_schema: bool,
) -> Result<DataFrame, GlowError> {
    assert!(
        unique_symbols.len() > 0,
        "unique symbols must have length > 0"
    );
    let symbols_set: HashSet<&Symbol> = unique_symbols.iter().cloned().collect();
    assert!(
        symbols_set.len() == unique_symbols.len(),
        "symbols must be unique"
    );
    assert!(
        ticks_data.len() % unique_symbols.len() == 0,
        "tick data is asymmetric"
    );

    let non_sampled_ticks_data_df =
        map_ticks_data_to_df(unique_symbols, ticks_data, schema_to_comply)?;
    let non_sampled_ticks_data_lf = non_sampled_ticks_data_df.lazy();

    let resampled_data = downsample_tick_lf_to_kline_duration(
        unique_symbols,
        kline_duration,
        non_sampled_ticks_data_lf,
        ClosedWindow::Left,
        if maintain_schema {
            Some(schema_to_comply)
        } else {
            None
        },
    )?;

    let resampled_data = resampled_data
        .collect()?
        .fill_null(FillNullStrategy::Forward(None))?;

    Ok(resampled_data)
}

// TODO: write code for diferentiating upsample / downsample
// upsample = increase frequency. i.e. minutes -> seconds. methods: ffill/ interpolation
// downsample = decrease frequency. i.e. seconds -> minutes. methods: aggregation
// currently working only for downsampling
pub fn downsample_tick_lf_to_kline_duration(
    unique_symbols: &Vec<&Symbol>,
    kline_duration: Duration,
    tick_lf: LazyFrame,
    closed_window: ClosedWindow, // TODO: check why benchmark frame doesn't leave last minute tick -- CHECK if this is still happening
    schema_to_comply: Option<&Schema>,
) -> Result<LazyFrame, GlowError> {
    let duration_in_secs = kline_duration.num_seconds();
    let mut agg_expressions = vec![];
    let mut tick_data_cols = HashSet::new();
    for symbol in unique_symbols {
        let (o, h, l, c) = symbol.get_ohlc_cols();
        if schema_to_comply.is_some() {
            tick_data_cols.insert(o);
            tick_data_cols.insert(h);
            tick_data_cols.insert(l);
            tick_data_cols.insert(c);
        }
        // TODO: check if we can use keep_name() instead of alias
        let first_open = col(o).drop_nulls().first().alias(&o);
        let max_high = col(h).max().alias(&h);
        let min_low = col(l).min().alias(&l);
        let last_close = col(c).drop_nulls().last().alias(&c);
        agg_expressions.push(first_open);
        agg_expressions.push(max_high);
        agg_expressions.push(min_low);
        agg_expressions.push(last_close);
    }

    if let Some(schema) = schema_to_comply {
        schema.iter().for_each(|(col_name, _)| {
            if col_name == "start_time" || tick_data_cols.contains(col_name.as_str()) {
                return;
            }
            // TODO: adjust downsample/upsample method
            // TODO: check if this can be replaced by NULL
            agg_expressions.push(col(&col_name).last().keep_name())
        })
    }

    let resampled_data = tick_lf
        .group_by_dynamic(
            col("start_time"),
            vec![],
            DynamicGroupOptions {
                start_by: StartBy::DataPoint,
                index_column: "start_time".into(),
                every: PolarsDuration::new(NANOS_IN_SECOND * duration_in_secs),
                period: PolarsDuration::new(NANOS_IN_SECOND * duration_in_secs),
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
// former timestamp_end_to_daily_timestamp_sec_intervals
pub fn get_fetch_timestamps_interval(
    start_timestamp: i64, // seconds
    end_timestamp: i64,   // seconds
    kline_duration: Duration,
    max_limit: i64,
) -> Vec<i64> {
    let step_size = max_limit * SECONDS_IN_MIN / kline_duration.num_minutes();
    stepped_range_inclusive(start_timestamp, end_timestamp, step_size)
}

fn stepped_range_inclusive(start: i64, end: i64, step_size: i64) -> Vec<i64> {
    let mut next_val = start;
    let last_step = end - (end - start) % step_size; // Finds the starting point of the last step

    let mut result = Vec::new();

    while next_val <= end {
        result.push(next_val);

        // If the current value is the start of the last step and is not the end,
        // then the next value should be the end value.
        if next_val == last_step && next_val != end {
            next_val = end;
        } else {
            next_val += step_size;
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
) -> Result<(), GlowError> {
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

pub fn closest_multiple_below(of: f64, to: f64) -> f64 {
    let quotient = to / of;
    let floored = quotient.floor();
    floored * of
}
