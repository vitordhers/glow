// use std::{
//     collections::HashMap,
//     sync::{Arc, Mutex},
// };
//
// use chrono::{Duration, Local, NaiveDate,  NaiveTime, Utc};
// use dotenv::dotenv;
// use polars::prelude::DataFrame;
// use regex::Regex;
// use std::io;
// use std::io::{stdin, stdout, Read, Write};
// use std::marker::Send;
//
#[tokio::main]
async fn main() {
    // env_logger::init();
    // dotenv().ok();

    // let is_test_mode = true;

    // // let trend_col = String::from("bullish_market");
    // use std::env::{self};
    // let max_rows = "40".to_string();
    // env::set_var("POLARS_FMT_MAX_ROWS", max_rows);

    // // ARGS
    // let windows = vec![1]; // 5, 15, 30 // 5, 9, 12

    // let btcusdt_contract: Contract = Contract::new(
    //     String::from("BTCUSDT"),
    //     0.0001,
    //     Duration::hours(8),
    //     None,
    //     0.00005,
    //     100.0,
    //     0.001,
    //     100.0,
    // );

    // let agixusdt_contract: Contract = Contract::new(
    //     String::from("AGIXUSDT"),
    //     0.000073,
    //     Duration::hours(8),
    //     None,
    //     0.00005,
    //     300000.0,
    //     1.0,
    //     25.0,
    // );

    // let arbusdt_contract: Contract = Contract::new(
    //     String::from("ARBUSDT"),
    //     0.0001,
    //     Duration::hours(8),
    //     None,
    //     0.0001,
    //     180000.0,
    //     0.1,
    //     50.0,
    // );

    // let ethusdt_contract: Contract = Contract::new(
    //     String::from("ETHUSDT"),
    //     0.0001,
    //     Duration::hours(8),
    //     None,
    //     0.01,
    //     1500.0,
    //     0.01,
    //     50.0,
    // );

    // let linkusdt_contract: Contract = Contract::new(
    //     String::from("LINKUSDT"),
    //     0.000047,
    //     Duration::hours(8),
    //     None,
    //     0.001,
    //     36750.0,
    //     0.1,
    //     50.0,
    // );

    // let mut contracts = HashMap::new();
    // contracts.insert(btcusdt_contract.symbol.clone(), btcusdt_contract.clone());
    // contracts.insert(agixusdt_contract.symbol.clone(), agixusdt_contract.clone());
    // contracts.insert(arbusdt_contract.symbol.clone(), arbusdt_contract.clone());
    // contracts.insert(linkusdt_contract.symbol.clone(), linkusdt_contract.clone());
    // contracts.insert(ethusdt_contract.symbol.clone(), ethusdt_contract.clone());

    // let start_datetime;

    // if !is_test_mode {
    //     start_datetime = current_datetime();
    // } else {
    //     println!("Glow test suite - v0.01.");
    //     println!("Ao Nosso Senhor Jesus Cristo, toda honra e toda glória 🙏");
    //     println!("em memória de Pedro Borges de Medeiros");

    //     let start_date: NaiveDate;

    //     loop {
    //         println!("Insira uma data no formato dd/mm/yyyy, anterior ao dia de hoje, para realizar o teste.");

    //         let mut input = String::new();

    //         let date_pattern = r"^(0?[1-9]|[12][0-9]|3[01])[\/\-](0?[1-9]|1[012])[\/\-]\d{4}$";

    //         let date_regex = Regex::new(date_pattern).unwrap();

    //         io::stdin()
    //             .read_line(&mut input)
    //             .expect("Insira um número válido!");

    //         if date_regex.is_match(&input.trim()) {
    //             let date_parts: Vec<&str> = input.trim().split('/').collect();

    //             if date_parts.len() == 3 {
    //                 // Extract day, month, and year as integers
    //                 let day = date_parts[0].parse().unwrap();
    //                 let month = date_parts[1].parse().unwrap();
    //                 let year = date_parts[2].parse().unwrap();

    //                 let mut parsed_date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
    //                 parsed_date = parsed_date + Duration::days(1);

    //                 let current_date = Local::now().date_naive();

    //                 if parsed_date < current_date {
    //                     start_date = parsed_date;
    //                     break;
    //                 } else {
    //                     println!(
    //                         "Data inválida! Insira uma data anterior ao dia de hoje {:?}.",
    //                         current_date
    //                     );
    //                     continue;
    //                 }
    //             } else {
    //                 println!("Data inválida! Use o formato dd/mm/yyyy.");
    //                 continue;
    //             }
    //         } else {
    //             println!("Data inválida! Use o formato dd/mm/yyyy");
    //             continue;
    //         }
    //     }

    //     let start_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();

    //     start_datetime = NaiveDateTime::new(start_date, start_time);
    // }

    // let seconds_to_next_full_minute = 60 - start_datetime.timestamp() % 60;
    // let trading_initial_datetime = start_datetime + Duration::seconds(seconds_to_next_full_minute);

    // let anchor_contract = linkusdt_contract.clone();
    // let traded_contract = linkusdt_contract.clone();

    // let anchor_symbol = anchor_contract.symbol.clone();
    // let traded_symbol = traded_contract.symbol.clone();

    // // let stochastic_indicator = StochasticIndicator {
    // //     name: "StochasticIndicator".into(),
    // //     windows: windows.clone(),
    // //     anchor_symbol: anchor_symbol.clone(),
    // //     starting_datetime: trading_initial_datetime,
    // //     k_window: 14,
    // //     k_smooth: 2,
    // //     d_smooth: 2,
    // // };

    // let cloud_length = 20;

    // let fast_slow_mas_indicator = FastSlowRelativeMovingAverages {
    //     name: "FastSlowRelativeMovingAverages".to_string(),
    //     contract: traded_contract,
    //     fast_period: 20,
    //     slow_period: 200,
    //     cloud_length,
    //     momentum_diff_span: 15,
    // };

    // let pre_indicators: Vec<IndicatorWrapper> = vec![];

    // let indicators: Vec<IndicatorWrapper> = vec![fast_slow_mas_indicator.into()];

    // let multiple_stochastic_with_threshold_short_signal =
    //     MultipleStochasticWithThresholdShortSignal {
    //         windows: windows.clone(),
    //         anchor_symbol: anchor_symbol.clone(),
    //         cloud_length,
    //     };

    // let multiple_stochastic_with_threshold_long_signal =
    //     MultipleStochasticWithThresholdLongSignal {
    //         windows: windows.clone(),
    //         anchor_symbol: anchor_symbol.clone(),
    //         cloud_length,
    //     };

    // let multiple_stochastic_with_threshold_close_short_signal =
    //     MultipleStochasticWithThresholdCloseShortSignal {
    //         anchor_symbol: anchor_symbol.clone(),
    //         windows: windows.clone(),
    //         cloud_length,
    //     };

    // let multiple_stochastic_with_threshold_close_long_signal =
    //     MultipleStochasticWithThresholdCloseLongSignal {
    //         anchor_symbol: anchor_symbol.clone(),
    //         windows: windows.clone(),
    //         cloud_length,
    //     };

    // let signals: Vec<SignalWrapper> = vec![
    //     multiple_stochastic_with_threshold_short_signal.into(),
    //     multiple_stochastic_with_threshold_long_signal.into(),
    //     multiple_stochastic_with_threshold_close_short_signal.into(),
    //     multiple_stochastic_with_threshold_close_long_signal.into(),
    // ];

    // let initial_leverage = Leverage::Isolated(5);

    // let position_lock_modifier = PositionLock::Nil;

    // let mut price_level_modifiers = HashMap::new();
    // let stop_loss: PriceLevel = PriceLevel::StopLoss(0.25);
    // let take_profit = PriceLevel::TakeProfit(0.5);

    // // let trailing_stop_loss = TrailingStopLoss::Percent(0.5, 0.05);
    // // let trailing_modifier = PriceLevel::TrailingStopLoss(trailing_stop_loss);

    // price_level_modifiers.insert(stop_loss.get_hash_key(), stop_loss);
    // price_level_modifiers.insert(take_profit.get_hash_key(), take_profit);
    // // price_level_modifiers.insert("tsl".to_string(), trailing_modifier);

    // let trading_settings_arc = Arc::new(Mutex::new(TradingSettings::new(
    //     OrderType::Market,
    //     OrderType::Market,
    //     initial_leverage.clone(),
    //     position_lock_modifier,
    //     price_level_modifiers,
    //     0.95, // CHANGE ALLOCATION,
    //     false,
    // )));

    // let bybit_exchange = BybitExchange::new(
    //     "Bybit".to_string(),
    //     contracts,
    //     anchor_symbol.clone(),
    //     traded_symbol.clone(),
    //     &trading_settings_arc,
    //     0.0002,
    //     0.00055,
    // );

    // let exchange = Box::new(bybit_exchange);

    // let exchange_listener: BehaviorSubject<Box<dyn Exchange + Send + Sync>> =
    //     BehaviorSubject::new(exchange);

    // let current_trade_listener: BehaviorSubject<Option<Trade>> = BehaviorSubject::new(None);
    // let timestamp = Utc::now().timestamp() * 1000;
    // let current_balance_listener = BehaviorSubject::new(Balance::new(timestamp, 0.0, 0.0));

    // let bar_length = 60;
    // let log_level = LogLevel::All;
    // let benchmark_balance = 100.00;

    // let performance = Performance::new(exchange_listener.clone(), trading_initial_datetime);
    // let performance_arc = Arc::new(Mutex::new(performance));

    // let signal_listener = BehaviorSubject::new(None);
    // let trading_data_listener = BehaviorSubject::new(DataFrame::default());

    // let strategy = Strategy::new(
    //     "Stepped Stochastic Strategy".to_string(),
    //     pre_indicators,
    //     indicators.clone(),
    //     signals.clone(),
    //     benchmark_balance,
    //     performance_arc.clone(),
    //     trading_settings_arc.clone(),
    //     exchange_listener.clone(),
    //     current_trade_listener.clone(),
    //     signal_listener.clone(),
    //     current_balance_listener.clone(),
    // );

    // let strategy_arc = Arc::new(Mutex::new(strategy));
    // let exchange_socket_error_arc = Arc::new(Mutex::new(None));

    // let trading_data_update_listener = BehaviorSubject::new(TradingDataUpdate::Nil);
    // let update_balance_listener: BehaviorSubject<Option<Balance>> = BehaviorSubject::new(None);
    // let update_order_listener: BehaviorSubject<Option<OrderAction>> = BehaviorSubject::new(None);
    // let update_executions_listener: BehaviorSubject<Vec<Execution>> = BehaviorSubject::new(vec![]);

    // let data_feed = DataFeed::new(
    //     bar_length,
    //     log_level,
    //     &exchange_socket_error_arc,
    //     &trading_data_update_listener,
    //     &exchange_listener,
    //     &trading_data_listener,
    //     &update_balance_listener,
    //     &update_order_listener,
    //     &update_executions_listener,
    //     &current_trade_listener,
    //     start_datetime,
    //     indicators,
    //     signals,
    //     is_test_mode,
    // );

    // let leverage_listener = BehaviorSubject::new(initial_leverage);
    // let trader = Trader::new(
    //     data_feed,
    //     &strategy_arc,
    //     &performance_arc,
    //     &trading_settings_arc,
    //     &exchange_socket_error_arc,
    //     &exchange_listener,
    //     &current_balance_listener,
    //     &update_balance_listener,
    //     &update_order_listener,
    //     &update_executions_listener,
    //     &signal_listener,
    //     &trading_data_listener,
    //     &trading_data_update_listener,
    //     &current_trade_listener,
    //     &leverage_listener,
    //     &log_level,
    //     is_test_mode,
    // );

    // let _ = trader.init().await;
}
