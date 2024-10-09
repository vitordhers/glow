use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use glow_error::{assert_or_error, GlowError};
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
pub mod performance;

use crate::{
    constants::{NANOS_IN_SECOND, SECONDS_IN_MIN},
    enums::signal_category::SignalCategory,
    r#static::SYMBOLS_MAP,
    structs::{Symbol, TickData},
};

/// gives result with full seconds
pub fn current_datetime() -> NaiveDateTime {
    Utc::now().naive_utc().with_nanosecond(0).unwrap()
}

pub fn current_datetime_minute_start() -> NaiveDateTime {
    Utc::now()
        .naive_utc()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap()
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
        diagonal: true,
        from_partitioned_ds: false,
    };
    let result_lf = concat(lfs, args)?;

    let result_lf = result_lf.filter(
        col("start_time")
            .dt()
            .datetime()
            .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
            .gt(filter_datetime.and_utc().timestamp() * 1000),
    );

    Ok(result_lf)
}

pub fn map_df_to_kline_data(df: &DataFrame, symbol: &Symbol) -> Result<Vec<TickData>, GlowError> {
    let timestamps = &df.column("start_time")?.timestamp(TimeUnit::Milliseconds)?;
    let opens = &df.column(&symbol.open)?.f64()?;
    let highs = &df.column(&symbol.high)?.f64()?;
    let lows = &df.column(&symbol.low)?.f64()?;
    let closes = &df.column(&symbol.close)?.f64()?;

    let mut ticks_data = vec![];

    for (i, ts) in timestamps.into_iter().enumerate() {
        let tick_data = TickData::new_from_string(
            &symbol.name,
            NaiveDateTime::from_timestamp_millis(ts.unwrap()).unwrap(),
            opens.get(i).unwrap(),
            highs.get(i).unwrap(),
            closes.get(i).unwrap(),
            lows.get(i).unwrap(),
        );
        ticks_data.push(tick_data);
    }

    Ok(ticks_data)
}

pub fn map_ticks_data_to_df(ticks_data: &Vec<TickData>) -> Result<DataFrame, GlowError> {
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

    let timestamp_series = Series::new(
        "start_time".into(),
        timestamps.into_iter().collect::<Vec<NaiveDateTime>>(),
    );
    let sort_options = SortOptions::default();
    let sort_options = sort_options.with_order_descending(false);
    let mut timestamp_series = timestamp_series.sort(sort_options)?;
    timestamp_series.set_sorted_flag(IsSorted::Ascending);

    let series_init = vec![timestamp_series];

    let df = DataFrame::new(data.into_iter().fold(
        series_init,
        |mut series, (col_name, col_data)| {
            series.push(Series::new(col_name.into(), col_data));
            series
        },
    ))?;
    let sort_options = SortMultipleOptions::default();
    let sort_options = sort_options.with_order_descending(false);
    let sort_options = sort_options.with_maintain_order(false);
    let df = df.sort(["start_time"], sort_options)?;

    Ok(df)
}

pub fn coerce_df_to_schema(df: DataFrame, schema: &Schema) -> Result<DataFrame, GlowError> {
    let current_schema = df.schema();
    // println!("THIS IS SCHEMA {:?}", current_schema);
    let data_size = df.height();
    // let mut result_df = DataFrame::from(schema);
    let mut series = vec![];
    // let start_time_series = df.column("start_time")?;
    // series.push;
    for (col, dtype) in schema.clone().into_iter() {
        let is_already_in_df = current_schema.contains(&col);
        if is_already_in_df {
            let column_series = df.column(&col)?.clone();
            series.push(column_series);
            continue;
        }
        let null_series = Series::full_null(col.into(), data_size, &dtype);
        series.push(null_series);
        // let _ = result_df.replace(&col, null_series);
    }
    let result_df = DataFrame::new(series)?;
    // println!("RESULT DF {:?}", result_df);
    Ok(result_df)
}

pub fn filter_df_timestamps_to_lf(
    df: DataFrame,
    start_datetime: NaiveDateTime,
    end_datetime: NaiveDateTime,
) -> Result<LazyFrame, GlowError> {
    let lf = df.lazy();
    let lf = lf.filter(
        col("start_time")
            .gt_eq(lit(start_datetime.timestamp_millis()))
            .and(col("start_time").lt_eq(lit(end_datetime.timestamp_millis()))),
    );

    Ok(lf)
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
        let first_open = col(o).drop_nulls().first().alias(o);
        let max_high = col(h).max().alias(h);
        let min_low = col(l).min().alias(l);
        let last_close = col(c).drop_nulls().last().alias(c);
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
            agg_expressions.push(
                col(col_name.clone())
                    .last()
                    .alias(col_name.clone())
                    .name()
                    .keep(),
            )
        })
    }

    let duration_string = format!("{}s", duration_in_secs);
    let resampled_data = tick_lf
        .group_by_dynamic(
            col("start_time"),
            vec![],
            DynamicGroupOptions {
                start_by: StartBy::DataPoint,
                index_column: "start_time".into(),
                label: Label::Left,
                every: PolarsDuration::parse(duration_string.as_str()),
                period: PolarsDuration::parse(duration_string.as_str()),
                offset: PolarsDuration::parse("0s"),
                include_boundaries: false,
                closed_window,
            },
        )
        .agg(agg_expressions);

    Ok(resampled_data)
}

pub fn get_days_between(
    start_datetime: NaiveDateTime,
    end_datetime: NaiveDateTime,
) -> Result<Vec<NaiveDate>, GlowError> {
    assert_or_error!(start_datetime <= end_datetime);

    let num_days = (end_datetime - start_datetime).num_days();
    let days = (0..=num_days)
        .map(|i| start_datetime.date() + Duration::days(i))
        .collect::<Vec<NaiveDate>>();
    Ok(days)
}

pub fn get_date_start_and_end_timestamps(date: NaiveDate) -> [(i64, i64); 2] {
    println!("get_date_start_and_end_timestamps {:?}", &date);
    let first_start_time = NaiveTime::from_hms_milli_opt(0, 0, 0, 0).unwrap();
    let first_start_datetime = NaiveDateTime::new(date, first_start_time);

    let first_end_time = NaiveTime::from_hms_milli_opt(11, 59, 0, 0).unwrap();
    let first_end_datetime = NaiveDateTime::new(date, first_end_time);

    let second_start_time = NaiveTime::from_hms_milli_opt(12, 0, 0, 0).unwrap();
    let second_start_datetime = NaiveDateTime::new(date, second_start_time);

    let second_end_time = NaiveTime::from_hms_milli_opt(23, 59, 0, 0).unwrap();
    let second_end_datetime = NaiveDateTime::new(date, second_end_time);

    [
        (
            first_start_datetime.and_utc().timestamp_millis(),
            first_end_datetime.and_utc().timestamp_millis(),
        ),
        (
            second_start_datetime.and_utc().timestamp_millis(),
            second_end_datetime.and_utc().timestamp_millis(),
        ),
    ]
}

// former timestamp_end_to_daily_timestamp_sec_intervals
pub fn get_fetch_timestamps_interval(
    start_timestamp_in_secs: i64,
    end_timestamp_in_secs: i64,
    kline_duration: Duration,
    max_limit: i64,
) -> Vec<i64> {
    let step_size = max_limit * SECONDS_IN_MIN / kline_duration.num_minutes();
    stepped_range_inclusive(start_timestamp_in_secs, end_timestamp_in_secs, step_size)
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

pub fn get_trading_columns_values(
    df: &DataFrame,
) -> Result<
    (
        Vec<Option<i64>>,
        Vec<Option<f64>>,
        Vec<Option<f64>>,
        Vec<Option<f64>>,
        Vec<Option<f64>>,
        Vec<Option<f64>>,
        Vec<Option<i32>>,
        Vec<Option<&str>>,
    ),
    GlowError,
> {
    let series_binding = df.columns([
        "start_time",
        "trade_fees",
        "units",
        "profit_and_loss",
        "returns",
        "balance",
        "position",
        "action",
    ])?;

    let mut series = series_binding.iter();
    let start_times: Vec<Option<i64>> = series.next().unwrap().datetime()?.into_iter().collect();
    let trades_fees: Vec<Option<f64>> = series.next().unwrap().f64()?.into_iter().collect();
    let units: Vec<Option<f64>> = series.next().unwrap().f64()?.into_iter().collect();
    let pnl: Vec<Option<f64>> = series.next().unwrap().f64()?.into_iter().collect();
    let returns: Vec<Option<f64>> = series.next().unwrap().f64()?.into_iter().collect();
    let balances: Vec<Option<f64>> = series.next().unwrap().f64()?.into_iter().collect();
    let positions: Vec<Option<i32>> = series.next().unwrap().i32()?.into_iter().collect();
    let actions: Vec<Option<&str>> = series.next().unwrap().str()?.into_iter().collect();
    Ok((
        start_times,
        trades_fees,
        units,
        pnl,
        returns,
        balances,
        positions,
        actions,
    ))
}

pub fn check_last_index_for_signal(
    trading_data_df: &DataFrame,
    signal_category: SignalCategory,
) -> Result<bool, GlowError> {
    let trading_data_schema = trading_data_df.schema();
    let signal_column = signal_category.get_column();
    if !trading_data_schema.contains(signal_column) {
        return Ok(false);
    }

    let last_index = trading_data_df.height() - 1;
    let column = trading_data_df
        .column(signal_column)?
        .i32()
        .unwrap()
        .into_no_null_iter()
        .collect::<Vec<i32>>();
    let value = column.get(last_index).unwrap();
    return Ok(value == &1);
}

pub fn get_signal_col_values(
    df: &DataFrame,
    signal_category: SignalCategory,
) -> Result<Vec<i32>, GlowError> {
    let schema = df.schema();
    let signal_column = signal_category.get_column();
    let df_height = df.height();
    if !schema.contains(signal_column) {
        return Ok(vec![0; df_height]);
    }

    let result = df
        .column(signal_column)?
        .i32()
        .unwrap()
        .into_no_null_iter()
        .collect::<Vec<i32>>();
    Ok(result)
}

pub fn get_price_columns(
    df: &DataFrame,
    symbol: &Symbol,
) -> Result<(Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>), GlowError> {
    let (open_col, high_col, low_col, close_col) = symbol.get_ohlc_cols();
    let opens = df
        .column(&open_col)
        .unwrap()
        .f64()
        .unwrap()
        .into_no_null_iter()
        .collect::<Vec<f64>>();
    let highs = df
        .column(&high_col)
        .unwrap()
        .f64()
        .unwrap()
        .into_no_null_iter()
        .collect::<Vec<f64>>();
    let lows = df
        .column(&low_col)
        .unwrap()
        .f64()
        .unwrap()
        .into_no_null_iter()
        .collect::<Vec<f64>>();
    let closes = df
        .column(&close_col)
        .unwrap()
        .f64()
        .unwrap()
        .into_no_null_iter()
        .collect::<Vec<f64>>();

    Ok((opens, highs, lows, closes))
}

pub fn get_price_columns_f32(
    df: &DataFrame,
    symbol: &Symbol,
) -> Result<(Vec<f32>, Vec<f32>, Vec<f32>, Vec<f32>), GlowError> {
    let (open_col, high_col, low_col, close_col) = symbol.get_ohlc_cols();
    let opens = df
        .column(&open_col)
        .unwrap()
        .f64()
        .unwrap()
        .into_no_null_iter()
        .map(|o| o as f32)
        .collect::<Vec<f32>>();
    let highs = df
        .column(&high_col)
        .unwrap()
        .f64()
        .unwrap()
        .into_no_null_iter()
        .map(|h| h as f32)
        .collect::<Vec<f32>>();
    let lows = df
        .column(&low_col)
        .unwrap()
        .f64()
        .unwrap()
        .into_no_null_iter()
        .map(|l| l as f32)
        .collect::<Vec<f32>>();
    let closes = df
        .column(&close_col)
        .unwrap()
        .f64()
        .unwrap()
        .into_no_null_iter()
        .map(|c| c as f32)
        .collect::<Vec<f32>>();

    Ok((opens, highs, lows, closes))
}

pub fn calculate_remainder(dividend: f64, divisor: f64) -> f64 {
    dividend.rem_euclid(divisor)
}

pub fn closest_multiple_below(of: f64, to: f64) -> f64 {
    let quotient = to / of;
    let floored = quotient.floor();
    floored * of
}
