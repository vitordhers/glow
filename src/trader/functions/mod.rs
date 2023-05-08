use std::time::Duration as TimeDuration;

use chrono::{Duration, NaiveDateTime, Timelike, Utc};
use polars::prelude::{Duration as PolarsDuration, *};
use reqwest::Client;
use tokio::time::{sleep};

use super::{
    constants::{MINUTES_IN_DAY, NANOS_IN_SECOND, SECONDS_IN_MIN},
    errors::Error,
    models::{HttpKlineResponse, TickData, WsKlineResponse},
};

pub fn current_datetime() -> NaiveDateTime {
    Utc::now().naive_utc().with_nanosecond(0).unwrap()
}

pub fn get_symbol_ohlc_cols(symbol: &String) -> (String, String, String, String) {
    let open_col = format!("{}_open", symbol);
    let high_col = format!("{}_high", symbol);
    let close_col = format!("{}_close", symbol);
    let low_col = format!("{}_low", symbol);
    return (open_col, high_col, close_col, low_col);
}

pub fn get_symbol_window_ohlc_cols(
    symbol: &String,
    window: &String,
) -> (String, String, String, String) {
    let open_col = format!("{}_{}_open", symbol, window);
    let high_col = format!("{}_{}_high", symbol, window);
    let close_col = format!("{}_{}_close", symbol, window);
    let low_col = format!("{}_{}_low", symbol, window);
    return (open_col, high_col, close_col, low_col);
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

pub fn consolidate_tick_data_into_lf(
    symbols: &[String; 2],
    tick_data: &Vec<TickData>,
    tick_data_schema: &Schema,
) -> Result<LazyFrame, Error> {
    let mut dfs = vec![];
    for symbol in symbols {
        let mut columns: Vec<Series> = vec![];

        let date_col = "start_time";
        let (open_col, high_col, close_col, low_col) = get_symbol_ohlc_cols(symbol);
        let mut dates = vec![];
        let mut opens = vec![];
        let mut highs = vec![];
        let mut closes = vec![];
        let mut lows = vec![];

        let mut empty_ticks: Vec<Option<f64>> = vec![];

        let symbol_ticks: Vec<TickData> = tick_data
            .iter()
            .filter(|&tick| tick.symbol == symbol.to_string())
            .cloned()
            .collect();
        // println!("SYMBOL TICKS {:?}", symbol_ticks);
        for tick in symbol_ticks {
            dates.push(tick.date);
            opens.push(tick.open);
            highs.push(tick.high);
            closes.push(tick.close);
            lows.push(tick.low);
            empty_ticks.push(None::<f64>);
        }
        for (field, _) in tick_data_schema.iter() {
            let result = match field {
                _date_col if _date_col == date_col => Series::new(field, &dates),
                _open_col if _open_col == open_col.as_str() => Series::new(field, &opens),
                _high_col if _high_col == high_col.as_str() => Series::new(field, &highs),
                _close_col if _close_col == close_col.as_str() => Series::new(field, &closes),
                _low_col if _low_col == low_col.as_str() => Series::new(field, &lows),
                _ => Series::new(field, &empty_ticks),
            };
            columns.push(result);
        }
        let df = DataFrame::new(columns)?;
        dfs.push(df);
    }
    let new_df = concat(
        dfs.iter()
            .map(|df| df.clone().lazy())
            .collect::<Vec<LazyFrame>>(),
        true,
        true,
    )?;

    Ok(new_df)
}

pub fn resample_tick_data_secs_to_min(
    symbols: &[String; 2],
    tick_data: &LazyFrame,
    tick_data_schema: &Schema,
    current_last_bar: &NaiveDateTime,
    bar_length: &Duration,
) -> Result<LazyFrame, Error> {
    // set base df from current_last_bar to current_last_bar + bar_length
    let bar_length_in_seconds = bar_length.num_seconds();
    let end_timestamp = current_last_bar.timestamp() + bar_length_in_seconds;
    let mut current_timestamp = current_last_bar.timestamp();
    let mut interval_ticks = vec![];
    let mut empty_float_ticks = vec![];

    while current_timestamp <= end_timestamp {
        interval_ticks.push(NaiveDateTime::from_timestamp_opt(current_timestamp, 0).unwrap());
        current_timestamp += 1;
        empty_float_ticks.push(None::<f64>);
    }

    let columns_names = tick_data_schema.iter_names();
    let mut base_df_series: Vec<Series> = vec![];
    for column in columns_names {
        let series = match column {
            date_col if date_col == "start_time" => Series::new(column, &interval_ticks),
            _ => Series::new(column, &empty_float_ticks.clone()),
        };
        base_df_series.push(series);
    }

    let base_df = DataFrame::new(base_df_series).unwrap();

    let mut agg_expressions = vec![];
    for symbol in symbols {
        let (open_col, high_col, close_col, low_col) = get_symbol_ohlc_cols(symbol);

        let open = when(col(&close_col).shift(1).is_null())
            .then(col(&open_col).drop_nulls().first())
            .otherwise(col(&close_col).shift(1))
            .alias(&open_col);
        let high = col(&high_col).max().alias(&high_col);
        let close = col(&close_col).drop_nulls().last().alias(&close_col);
        let low = col(&low_col).min().alias(&low_col);
        agg_expressions.push(open);
        agg_expressions.push(high);
        agg_expressions.push(close);
        agg_expressions.push(low);
    }
    let resampled_data = concat(&[tick_data.clone(), base_df.lazy()], true, true)?;
    let resampled_data =
        resample_tick_data_to_min(symbols, bar_length, &resampled_data, ClosedWindow::Right)?;

    let resampled_data = resampled_data
        .collect()?
        .fill_null(FillNullStrategy::Forward(None))?
        .lazy();

    Ok(resampled_data)
}

pub fn resample_tick_data_to_min(
    symbols: &[String; 2],
    bar_length: &Duration,
    tick_data: &LazyFrame,
    closed_window: ClosedWindow,
) -> Result<LazyFrame, Error> {
    let bar_length_in_seconds = bar_length.num_seconds();
    let mut agg_expressions = vec![];
    for symbol in symbols {
        let (open_col, high_col, close_col, low_col) = get_symbol_ohlc_cols(symbol);

        let open = col(&open_col).drop_nulls().first().alias(&open_col);
        let high = col(&high_col).max().alias(&high_col);
        let close = col(&close_col).drop_nulls().last().alias(&close_col);
        let low = col(&low_col).min().alias(&low_col);
        agg_expressions.push(open);
        agg_expressions.push(high);
        agg_expressions.push(close);
        agg_expressions.push(low);
    }
    let resampled_data = tick_data
        .clone()
        .sort(
            "start_time",
            SortOptions {
                descending: false,
                nulls_last: false,
                multithreaded: true,
            },
        )
        .groupby_dynamic(
            vec![],
            DynamicGroupOptions {
                start_by: StartBy::DataPoint,
                index_column: "start_time".into(),
                every: PolarsDuration::new(NANOS_IN_SECOND * bar_length_in_seconds),
                period: PolarsDuration::new(NANOS_IN_SECOND * bar_length_in_seconds),
                offset: PolarsDuration::new(0),
                truncate: true,
                include_boundaries: false,
                closed_window,
            },
        )
        .agg(agg_expressions);
    Ok(resampled_data)
}

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
    tick_data_payload: Vec<TickData>,
    last_bar: &NaiveDateTime,
) -> Vec<TickData> {
    tick_data_payload
        .iter()
        .filter(|&tick_data| tick_data.date >= *last_bar)
        .cloned()
        .collect()
}

pub async fn get_historical_tick_data(
    http: &Client,
    symbols: &[String; 2],
    last_bar: &NaiveDateTime,
    limit: i64,
    fetch_offset: TimeDuration,
) -> Result<Vec<TickData>, Error> {
    let timestamp_intervals =
        timestamp_end_to_daily_timestamp_sec_intervals(last_bar.timestamp(), limit, 1);
    let mut tick_data = vec![];
    for (i, value) in timestamp_intervals.iter().enumerate() {
        if i == 0 {
            continue;
        }
        let start = &timestamp_intervals[i - 1] * 1000;
        let end = &timestamp_intervals[i] * 1000;

        if value == timestamp_intervals.last().unwrap() {
            sleep(fetch_offset).await;
            // let start = Instant::now();
            // let sleep_duration = Duration::seconds(fetch_offset);
            // while Instant::now().duration_since(start) < fetch_offset {
            //     println!("Sleeping...");
            //     sleep(Duration::from_secs(1)).await;
            // }
        }
        for symbol in symbols {
            let fetched_klines = fetch_data(http, symbol, &start, &end, limit).await?;
            fetched_klines.iter().for_each(|kline| {
                let tick = parse_http_kline_into_tick_data(symbol.to_string(), kline).unwrap();
                tick_data.push(tick);
            });
        }
    }
    Ok(tick_data)
}

async fn fetch_data(
    http: &Client,
    symbol: &String,
    start_time_timestamp: &i64, // ms
    end_time_timestamp: &i64,   // ms
    limit: i64,                 //Default 500; max 1000.
) -> Result<Vec<HttpKlineResponse>, Error> {
    let url = format!(
        "https://api3.binance.com/api/v3/klines?symbol={}&interval={}&startTime={}&endTime={}&limit={}",
        symbol, "1m", start_time_timestamp, end_time_timestamp, limit
    );
    println!(
        "{:?} | Fetching {} data ({} records) for interval between {} and {}",
        current_datetime(),
        symbol,
        limit,
        NaiveDateTime::from_timestamp_millis(*start_time_timestamp).unwrap(),
        NaiveDateTime::from_timestamp_millis(*end_time_timestamp).unwrap()
    );
    let result: Vec<HttpKlineResponse> = http.get(url).send().await?.json().await?;
    Ok(result)
}

fn parse_http_kline_into_tick_data(
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

fn timestamp_end_to_daily_timestamp_sec_intervals(
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
