use crate::functions::select_from_list;
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use common::constants::{DATE_INPUT_REGEX, TIME_INPUT_REGEX};
use common::{functions::current_datetime, traits::exchange::TraderHelper};
use exchanges::enums::TraderExchangeWrapper;
use glow_error::{assert_or_error, GlowError};
use regex::Regex;
use std::io::stdin;
use strategy::Strategy;

/// check for validation schema in order to understand better: https://docs.google.com/spreadsheets/d/1VlRswrunwbYIkmvHdg2MGt7TmkpdkS0fGhGMqRyEgiY/edit?gid=0#gid=0
pub fn change_benchmark_datetimes(
    benchmark_start: NaiveDateTime,
    benchmark_end: NaiveDateTime,
    current_exchange: TraderExchangeWrapper,
    current_strategy: Strategy,
) -> Result<Option<(NaiveDateTime, NaiveDateTime)>> {
    let benchmark_datetimes_options = vec![
        String::from("Change Benchmark start datetime"),
        String::from("Change Benchmark end datetime"),
        String::from("Go back"),
    ];
    let back_index = benchmark_datetimes_options.len() - 1;
    let selection = select_from_list(
        "Select a Date to change",
        &benchmark_datetimes_options,
        Some(back_index),
    );

    let settings = current_exchange.get_trading_settings();
    let benchmark_min_days_duration = Duration::days(settings.bechmark_min_days as i64);
    let kline_duration = settings.granularity.get_chrono_duration();
    let minimum_klines_duration = Duration::minutes(
        (current_strategy.get_minimum_klines_for_benchmarking() as i64)
            * kline_duration.num_minutes(),
    );
    let traded_contract = current_exchange.get_traded_contract();
    let current_datetime = current_datetime();
    let minimum_benchmark_duration = minimum_klines_duration + benchmark_min_days_duration;

    match selection {
        0 => {
            let start_after_datetime = traded_contract.available_since + minimum_benchmark_duration;
            let start_after_date = start_after_datetime.date();
            let start_before_datetime = benchmark_end - minimum_benchmark_duration;
            let start_before_date = start_before_datetime.date();
            let updated_benchmark_start_date = get_validated_date_input(
                "Start date: ",
                Some(start_before_date),
                Some(start_after_date),
                benchmark_start.date(),
            );
            let mut start_before_time = None;
            let mut start_after_time = None;
            if updated_benchmark_start_date == start_after_date {
                start_after_time = Some(start_after_datetime.time());
            } else if updated_benchmark_start_date == start_before_date {
                start_before_time = Some(start_before_datetime.time())
            }
            let updated_benchmark_start_time = get_validated_time_input(
                start_before_time,
                start_after_time,
                benchmark_start.time(),
            );
            let updated_benchmark_start_datetime =
                NaiveDateTime::new(updated_benchmark_start_date, updated_benchmark_start_time);

            assert_or_error!(updated_benchmark_start_datetime < benchmark_end);
            assert_or_error!(
                (updated_benchmark_start_datetime + minimum_benchmark_duration) <= benchmark_end
            );

            Ok(Some((updated_benchmark_start_datetime, benchmark_end)))
        }
        1 => {
            let end_before_datetime = current_datetime;
            let end_before_date = end_before_datetime.date();
            let end_after_datetime = benchmark_start + minimum_benchmark_duration;
            let end_after_date = end_after_datetime.date();
            let updated_benchmark_end_date = get_validated_date_input(
                "End date: ",
                Some(end_before_date),
                Some(end_after_date),
                benchmark_end.date(),
            );
            let mut end_before_time = None;
            let mut end_after_time = None;
            if updated_benchmark_end_date == end_after_date {
                end_after_time = Some(end_after_datetime.time());
            } else if updated_benchmark_end_date == end_before_date {
                end_before_time = Some(end_before_datetime.time())
            }
            let updated_benchmark_end_time =
                get_validated_time_input(end_before_time, end_after_time, benchmark_end.time());

            let updated_benchmark_end_datetime =
                NaiveDateTime::new(updated_benchmark_end_date, updated_benchmark_end_time);

            assert_or_none!(benchmark_start < updated_benchmark_end_datetime);
            assert_or_none!(
                (benchmark_start + minimum_benchmark_duration) <= updated_benchmark_end_datetime
            );

            Ok(Some((benchmark_start, updated_benchmark_end_datetime)))
        }
        _ => Ok(None),
    }
}

// TODO: implement breaking when ESC is pressed
fn get_validated_date_input(
    title: &str,
    before: Option<NaiveDate>,
    after: Option<NaiveDate>,
    current_date: NaiveDate,
) -> NaiveDate {
    let format = "%d-%m-%Y";
    loop {
        let validation_str = if let (Some(before), Some(after)) = (before, after) {
            format!(
                ", before {} and after {},",
                before.format(format).to_string(),
                after.format(format).to_string()
            )
        } else if let (Some(before), None) = (before, after) {
            format!(", before {},", before.format(format).to_string())
        } else if let (None, Some(after)) = (before, after) {
            format!(", after {}", after.format(format).to_string())
        } else {
            String::from("")
        };
        let current = current_date.format(format).to_string();
        println!(
            "{} insert a date in format dd-mm-yyyy{} to proceed. Current = {}",
            title, validation_str, &current
        );

        let mut input = current;

        let date_regex = Regex::new(DATE_INPUT_REGEX).unwrap();

        stdin()
            .read_line(&mut input)
            .expect("Insert a valid number!");

        if !date_regex.is_match(&input.trim()) {
            println!("Invalid date! Use format dd-mm-yyyy");
            continue;
        }

        let date_parts: Vec<&str> = input.trim().split('-').collect();

        if date_parts.len() != 3 {
            println!("Invalid date! Use format dd-mm-yyyy");
            continue;
        }

        // Extract day, month, and year as integers
        let day = date_parts[0].parse().unwrap();
        let month = date_parts[1].parse().unwrap();
        let year = date_parts[2].parse().unwrap();
        let date_result = NaiveDate::from_ymd_opt(year, month, day);
        if let None = date_result {
            println!("Invalid date! Use format dd-mm-yyyy");
            continue;
        }
        let parsed_date = date_result.unwrap();

        let validated_date: Option<NaiveDate> = if let (Some(before), Some(after)) = (before, after)
        {
            if parsed_date < before || parsed_date > after {
                println!(
                    "Date must be before {} and after {}",
                    before.format(format).to_string(),
                    after.format(format).to_string()
                );
                None
            } else {
                Some(parsed_date)
            }
        } else if let (Some(before), None) = (before, after) {
            if parsed_date < before {
                println!("Date must be before {}", before.format(format).to_string());
                None
            } else {
                Some(parsed_date)
            }
        } else if let (None, Some(after)) = (before, after) {
            if parsed_date > after {
                println!("Date must be after {}", after.format(format).to_string());
                None
            } else {
                Some(parsed_date)
            }
        } else {
            Some(parsed_date)
        };

        if let None = validated_date {
            continue;
        }

        return validated_date.unwrap();
    }
}

fn get_validated_time_input(
    before: Option<NaiveTime>,
    after: Option<NaiveTime>,
    current_time: NaiveTime,
) -> NaiveTime {
    let format = "%Hh%Mm";
    loop {
        let validation_str = if let (Some(before), Some(after)) = (before, after) {
            format!(
                ", before {} and after {},",
                before.format(format).to_string(),
                after.format(format).to_string()
            )
        } else if let (Some(before), None) = (before, after) {
            format!(", before {},", before.format(format).to_string())
        } else if let (None, Some(after)) = (before, after) {
            format!(", after {}", after.format(format).to_string())
        } else {
            String::from("")
        };

        let current = current_time.format(format).to_string();

        println!(
            "Insert a time in format hh:mm{} to proceed. Current = {}",
            validation_str, &current
        );

        let mut input = current;

        let time_regex = Regex::new(TIME_INPUT_REGEX).unwrap();

        stdin()
            .read_line(&mut input)
            .expect("Insert a valid number!");

        if !time_regex.is_match(&input.trim()) {
            println!("Invalid time! Use format hh:mm");
            continue;
        }

        let time_parts: Vec<&str> = input.trim().split(':').collect();

        if time_parts.len() != 2 {
            println!("Invalid time! Use format hh:mm");
            continue;
        }

        // Extract hours, minutes, and seconds as integers
        let hours = time_parts[0].parse().unwrap();
        let minutes = time_parts[1].parse().unwrap();

        let time_result = NaiveTime::from_hms_opt(hours, minutes, 0);
        if let None = time_result {
            println!("Invalid date! Use format hh:mm");
            continue;
        }
        let parsed_time = time_result.unwrap();

        let validated_time = if let (Some(before), Some(after)) = (before, after) {
            if parsed_time < before || parsed_time > after {
                println!(
                    "Date must be before {} and after {}",
                    before.format(format).to_string(),
                    after.format(format).to_string()
                );
                None
            } else {
                Some(parsed_time)
            }
        } else if let (Some(before), None) = (before, after) {
            if parsed_time < before {
                println!("Date must be before {}", before.format(format).to_string());
                None
            } else {
                Some(parsed_time)
            }
        } else if let (None, Some(after)) = (before, after) {
            if parsed_time > after {
                println!("Date must be after {}", after.format(format).to_string());
                None
            } else {
                Some(parsed_time)
            }
        } else {
            Some(parsed_time)
        };

        if let None = validated_time {
            continue;
        }

        return validated_time.unwrap();
    }
}
