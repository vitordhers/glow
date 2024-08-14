// use crate::trader::functions::timestamp_end_to_daily_timestamp_sec_intervals;
// use crate::trader::functions::{
//     consolidate_complete_tick_data_into_lf, fetch_data, parse_http_kline_into_tick_data,
//     resample_tick_data_to_length,
// };
// use crate::trader::{constants::SECONDS_IN_MIN, errors::Error};
// use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
// use polars::prelude::*;
// use reqwest::Client;

// use crate::trader::{constants::DAY_IN_MS, functions::get_symbol_ohlc_cols};
// use std::marker::Send;
// use traits::{optimizable_indicator::OptimizableIndicator, optimizable_signal::OptimizableSignal};

fn main() {
    // use super::{traits::optimizable_indicator::OptimizableIndicatorWrapper, *};

    // async fn optimize_strategy(
    //     initial_fetch_offset: i64,
    //     date_str: &String,
    //     symbols: Vec<String>,
    //     optimizable_pre_indicators: Vec<OptimizableIndicatorWrapper>,
    //     optimizable_indicators: Vec<OptimizableIndicatorWrapper>,
    //     optimizable_signals: Vec<Box<(dyn OptimizableSignal + Send + Sync + 'static)>>,
    // ) -> Result<(), Error> {
    //     let end_timestamp_ms = get_ms_timestamp_end(date_str).expect("get_ms_timestamp_end error");
    //     let tick_data_schema = build_tick_data_schema(&symbols);

    //     let tick_data_lf = fetch_historical_data(
    //         initial_fetch_offset,
    //         end_timestamp_ms,
    //         &symbols,
    //         &tick_data_schema,
    //     )
    //     .await?;

    //     let optimizable_data =
    //         set_combined_data(tick_data_lf, &optimizable_indicators, &optimizable_signals)?;

    //     // get indicator combinations args
    //     // get signal combinations args
    //     // get trading settings combinations args

    //     todo!()
    // }

    // fn get_ms_timestamp_end(date_str: &String) -> Result<i64, Error> {
    //     let date_parts: Vec<&str> = date_str.split('/').collect();
    //     let day = date_parts.iter().next().unwrap().parse::<u32>()?;
    //     let month = date_parts.iter().next().unwrap().parse::<u32>()?;
    //     let year = date_parts.iter().next().unwrap().parse::<i32>()?;

    //     let date = NaiveDate::from_ymd_opt(year, month, day);
    //     if date.is_none() {
    //         panic!("invalid date");
    //     }
    //     let date = date.unwrap();
    //     let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();

    //     let datetime = NaiveDateTime::new(date, time);

    //     let now = Utc::now();
    //     let today_start_datetime = now
    //         .with_hour(0)
    //         .expect("Invalid hour")
    //         .with_minute(0)
    //         .expect("Invalid minute")
    //         .with_second(0)
    //         .expect("Invalid second")
    //         .with_nanosecond(0)
    //         .expect("Invalid nanoseconds");

    //     let today_start_datetime = today_start_datetime.timestamp_millis();
    //     let start_timestamp_ms = datetime.timestamp_millis();

    //     if start_timestamp_ms >= today_start_datetime {
    //         panic!("Today can't be optimized since we don't have all klines available yet");
    //     }

    //     Ok(start_timestamp_ms + DAY_IN_MS)
    // }

    // fn build_tick_data_schema(symbols: &Vec<String>) -> Schema {
    //     let mut schema_fields: Vec<Field> = vec![];

    //     schema_fields.push(Field::new(
    //         "start_time",
    //         DataType::Datetime(TimeUnit::Milliseconds, None),
    //     ));

    //     for symbol in symbols {
    //         let (open_col, high_col, low_col, close_col) = get_symbol_ohlc_cols(&symbol);

    //         schema_fields.push(Field::new(open_col.as_str(), DataType::Float64));
    //         schema_fields.push(Field::new(high_col.as_str(), DataType::Float64));
    //         schema_fields.push(Field::new(close_col.as_str(), DataType::Float64));
    //         schema_fields.push(Field::new(low_col.as_str(), DataType::Float64));
    //     }

    //     Schema::from_iter(schema_fields.into_iter())
    // }

    // async fn fetch_historical_data(
    //     initial_fetch_offset: i64,
    //     timestamp_end: i64,
    //     symbols: &Vec<String>,
    //     tick_data_schema: &Schema,
    // ) -> Result<LazyFrame, Error> {
    //     let limit = 720; // TODO: limit hardcoded

    //     let timestamp_intervals =
    //         timestamp_end_to_daily_timestamp_sec_intervals(timestamp_end, limit, 1); // TODO: granularity hardcoded
    //     let mut tick_data = vec![];

    //     let duration = Duration::minutes(1);

    //     for (i, value) in timestamp_intervals.iter().enumerate() {
    //         let start: i64;
    //         let current_limit: i64;
    //         if i == 0 {
    //             // if first iteration, grab time offset by initial_fetch_offset
    //             start = (value - initial_fetch_offset * SECONDS_IN_MIN) * 1000;
    //             current_limit = initial_fetch_offset;
    //         } else {
    //             current_limit = limit;
    //             start = &timestamp_intervals[i - 1] * 1000;
    //         }

    //         let mut end = (&timestamp_intervals[i] * 1000) - 1;

    //         if value == timestamp_intervals.last().unwrap() {
    //             end -= duration.num_seconds() * 1000;
    //         }

    //         let http = Client::new();

    //         for symbol in symbols {
    //             let fetched_klines = fetch_data(&http, symbol, &start, &end, current_limit).await?;
    //             fetched_klines.iter().for_each(|kline| {
    //                 let tick = parse_http_kline_into_tick_data(symbol.to_string(), kline).unwrap();
    //                 tick_data.push(tick);
    //             });
    //         }
    //     }

    //     let new_tick_data_lf =
    //         consolidate_complete_tick_data_into_lf(&symbols, &tick_data, tick_data_schema)?;

    //     let tick_data_lf = resample_tick_data_to_length(
    //         &symbols,
    //         duration,
    //         new_tick_data_lf,
    //         ClosedWindow::Left,
    //         None,
    //     )?;

    //     Ok(tick_data_lf)
    // }

    // fn set_combined_data(
    //     tick_data: LazyFrame,
    //     optimizable_indicators: &Vec<OptimizableIndicatorWrapper>,
    //     optimizable_signals: &Vec<Box<(dyn OptimizableSignal + Send + Sync + 'static)>>,
    // ) -> Result<LazyFrame, Error> {
    //     let mut lf = set_combined_indicators_data(&tick_data, optimizable_indicators)?;
    //     // lf = set_combined_signals_data(&lf, optimizable_signals)?;
    //     Ok(lf)
    // }

    // fn set_combined_indicators_data(
    //     data: &LazyFrame,
    //     optimizable_indicators: &Vec<OptimizableIndicatorWrapper>,
    // ) -> Result<LazyFrame, Error> {
    //     let mut data = data.to_owned();

    //     // for pre_indicator in optimizable_indicators {
    //     //     let lf = pre_indicator.set_combined_indicator_columns(data.clone())?;
    //     //     data = data.left_join(lf, "start_time", "start_time");
    //     // }

    //     Ok(data)
    // }

    // fn set_combined_signals_data(
    //     data: &LazyFrame,
    //     optimizable_signals: &Vec<Box<(dyn OptimizableSignal + Send + Sync + 'static)>>,
    // ) -> Result<LazyFrame, Error> {
    //     let mut data = data.to_owned();
    //     for signal in optimizable_signals {
    //         let lf = signal.set_combined_signal_columns(&data)?;
    //         data = data.left_join(lf, "start_time", "start_time");
    //     }

    //     Ok(data)
    // }
}

// #[test]
// fn test_iproduct() {
//     use itertools::iproduct;

//     let vec1 = vec![1, 2, 3];
//     let vec2 = vec![4, 5, 6];
//     let vec3 = vec![7, 8, 9];
//     for (x, y, z) in iproduct!(vec1.iter(), vec2.iter(), vec3.iter()) {
//         println!("Combination: {} + {} + {}", x, y, z);
//     }
// }

// #[test]
// fn test_combinations() {
//     use itertools::Itertools;
//     let mut combinations = (3..=30).combinations(3);

//     let mut len = 0;

//     while let Some(comb) = combinations.next() {
//         println!("@@@ {:?}", comb);
//         len += 1;
//     }

//     println!("len @@ {}", len)
// }
