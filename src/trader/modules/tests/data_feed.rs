use super::performance::Performance;
use super::strategy::Strategy;
use crate::shared::csv::save_csv;
use crate::trader::constants::{SECONDS_IN_DAY, SECONDS_IN_MIN};
use crate::trader::enums::balance::Balance;
use crate::trader::enums::modifiers::leverage::Leverage;
use crate::trader::enums::modifiers::position_lock::PositionLock;
use crate::trader::enums::modifiers::price_level::PriceLevel;
use crate::trader::enums::order_type::OrderType;

use crate::trader::indicators::{
    ExponentialMovingAverageIndicator, IndicatorWrapper, StochasticIndicator,
    StochasticThresholdIndicator,
};
use crate::trader::models::contract::Contract;
use crate::trader::models::trading_settings::TradingSettings;
use std::io::Write;
use tokio::time::{interval, interval_at, sleep, sleep_until, Instant, Interval};


use crate::trader::signals::{
    MultipleStochasticWithThresholdCloseLongSignal,
    MultipleStochasticWithThresholdCloseShortSignal, MultipleStochasticWithThresholdLongSignal,
    MultipleStochasticWithThresholdShortSignal, SignalWrapper,
};

#[tokio::test]
async fn test_benchmark() -> Result<(), Error> {
    use dotenv::dotenv;

    env_logger::init();
    dotenv().ok();
    let anchor_symbol = String::from("BTCUSDT");
    let trend_col = String::from("bullish_market");

    // ARGS
    let windows = vec![3, 5, 15]; // 5, 15, 30 // 5, 9, 12
    let upper_threshold = 70;
    let lower_threshold = 30;
    let close_window_index = 2;

    let btcusdt_contract: Contract = Contract::new(
        String::from("BTCUSDT"),
        0.0001,
        ChronoDuration::hours(8),
        None,
        0.00005,
        100.0,
        0.001,
        100.0,
    );

    let agixusdt_contract: Contract = Contract::new(
        String::from("AGIXUSDT"),
        0.000073,
        ChronoDuration::hours(8),
        None,
        0.00005,
        300000.0,
        1.0,
        25.0,
    );

    let arbusdt_contract: Contract = Contract::new(
        String::from("ARBUSDT"),
        0.0001,
        ChronoDuration::hours(8),
        None,
        0.0001,
        180000.0,
        0.1,
        50.0,
    );

    let linlusdt_contract: Contract = Contract::new(
        String::from("LINKUSDT"),
        0.000047,
        ChronoDuration::hours(8),
        None,
        0.001,
        36750.0,
        0.1,
        50.0,
    );

    let mut contracts = HashMap::new();
    contracts.insert(btcusdt_contract.symbol.clone(), btcusdt_contract);
    contracts.insert(agixusdt_contract.symbol.clone(), agixusdt_contract);
    contracts.insert(arbusdt_contract.symbol.clone(), arbusdt_contract);
    contracts.insert(linlusdt_contract.symbol.clone(), linlusdt_contract);

    let stochastic_indicator = StochasticIndicator {
        name: "StochasticIndicator".into(),
        windows: windows.clone(),
        anchor_symbol: anchor_symbol.clone(),
    };

    let stochastic_threshold_indicator = StochasticThresholdIndicator {
        name: "StochasticThresholdIndicator".into(),
        upper_threshold,
        lower_threshold,
        trend_col: trend_col.clone(),
    };

    let ewm_preindicator = ExponentialMovingAverageIndicator {
        name: "ExponentialMovingAverageIndicator".into(),
        anchor_symbol: anchor_symbol.clone(),
        long_span: 50,
        short_span: 21,
        trend_col: trend_col.clone(),
    };

    let pre_indicators: Vec<IndicatorWrapper> = vec![ewm_preindicator.into()];

    let indicators: Vec<IndicatorWrapper> = vec![
        stochastic_indicator.into(),
        stochastic_threshold_indicator.into(),
    ];

    let multiple_stochastic_with_threshold_short_signal =
        MultipleStochasticWithThresholdShortSignal {
            windows: windows.clone(),
            anchor_symbol: anchor_symbol.clone(),
        };

    let multiple_stochastic_with_threshold_long_signal =
        MultipleStochasticWithThresholdLongSignal {
            windows: windows.clone(),
            anchor_symbol: anchor_symbol.clone(),
        };

    let multiple_stochastic_with_threshold_close_short_signal =
        MultipleStochasticWithThresholdCloseShortSignal {
            anchor_symbol: anchor_symbol.clone(),
            windows: windows.clone(),
            upper_threshold,
            lower_threshold,
            close_window_index,
        };

    let multiple_stochastic_with_threshold_close_long_signal =
        MultipleStochasticWithThresholdCloseLongSignal {
            anchor_symbol: anchor_symbol.clone(),
            windows: windows.clone(),
            upper_threshold,
            lower_threshold,
            close_window_index,
        };

    let signals: Vec<SignalWrapper> = vec![
        multiple_stochastic_with_threshold_short_signal.into(),
        multiple_stochastic_with_threshold_long_signal.into(),
        multiple_stochastic_with_threshold_close_short_signal.into(),
        multiple_stochastic_with_threshold_close_long_signal.into(),
    ];

    let bybit_exchange = BybitExchange::new(
        "Bybit".to_string(),
        contracts,
        "BTCUSDT".to_string(),
        "AGIXUSDT".to_string(),
        0.0002,
        0.00055,
    );

    let exchange = Box::new(bybit_exchange);

    let exchange_listener: BehaviorSubject<Box<dyn Exchange + Send + Sync>> =
        BehaviorSubject::new(exchange);

    let current_trade_listener: BehaviorSubject<Option<Trade>> = BehaviorSubject::new(None);
    let timestamp = Utc::now().timestamp() * 1000;
    let current_balance_listener = BehaviorSubject::new(Balance::new(timestamp, 0.0, 0.0));

    let benchmark_balance = 100.00;

    let performance = Performance::new(exchange_listener.clone());
    let performance_arc = Arc::new(Mutex::new(performance));

    let signal_listener = BehaviorSubject::new(None);
    let initial_leverage = Leverage::Isolated(25);

    let position_lock_modifier = PositionLock::Fee;

    let mut price_level_modifiers = HashMap::new();
    let stop_loss: PriceLevel = PriceLevel::StopLoss(0.40);
    let take_profit = PriceLevel::TakeProfit(1.0);
    price_level_modifiers.insert(stop_loss.get_hash_key(), stop_loss);
    price_level_modifiers.insert(take_profit.get_hash_key(), take_profit);

    let trading_settings_arc = Arc::new(Mutex::new(TradingSettings::new(
        OrderType::Limit,
        OrderType::Market,
        initial_leverage.clone(),
        position_lock_modifier,
        price_level_modifiers,
        0.95, // CHANGE ALLOCATION
    )));

    let strategy = Strategy::new(
        "Stepped Stochastic Strategy".into(),
        pre_indicators,
        indicators,
        signals,
        benchmark_balance,
        performance_arc.clone(),
        trading_settings_arc.clone(),
        exchange_listener.clone(),
        current_trade_listener.clone(),
        signal_listener.clone(),
        current_balance_listener.clone(),
    );

    let http = Client::new();
    let limit = 720; // TODO: limit hardcoded
    let initial_fetch_offset = 225;
    let bar_length = Duration::from_secs(60);
    let current_datetime = NaiveDateTime::from_timestamp_millis(1694603160000).unwrap();
    let seconds_to_next_full_minute = 60 - current_datetime.timestamp() % 60;
    let last_bar = current_datetime + ChronoDuration::seconds(seconds_to_next_full_minute);

    let timestamp_end = 1694689560;
    let timestamp_intervals =
        timestamp_end_to_daily_timestamp_sec_intervals(timestamp_end, limit, 1); // TODO: granularity hardcoded
    let mut tick_data = vec![];
    let symbols = vec!["BTCUSDT".to_string(), "AGIXUSDT".to_string()];

    for (i, value) in timestamp_intervals.iter().enumerate() {
        let start: i64;
        let current_limit: i64;
        if i == 0 {
            // if first iteration, grab time offset by initial_fetch_offset
            start = (value - initial_fetch_offset * SECONDS_IN_MIN) * 1000;
            current_limit = initial_fetch_offset;
        } else {
            current_limit = limit;
            start = &timestamp_intervals[i - 1] * 1000;
        }

        let mut end = (&timestamp_intervals[i] * 1000) - 1;

        if value == timestamp_intervals.last().unwrap() {
            end -= bar_length.as_secs() as i64 * 1000;
            // println!(
            //     "{:?} | ⌛ Waiting {:?} secs until last ({}) klines data is available",
            //     current_datetime(),
            //     self.initial_fetch_delay.as_secs(),
            //     NaiveDateTime::from_timestamp_millis(end)
            //         .unwrap()
            //         .format("%Y-%m-%d %H:%M")
            //         .to_string()
            // );
            // sleep(self.initial_fetch_delay).await;
            // let start = Instant::now();
            // let sleep_duration = Duration::seconds(fetch_offset);
            // while Instant::now().duration_since(start) < fetch_offset {
            //     println!("Sleeping...");
            //     sleep(Duration::from_secs(1)).await;
            // }
        }
        for symbol in &symbols.clone() {
            let fetched_klines = fetch_data(&http, symbol, &start, &end, current_limit).await?;
            fetched_klines.iter().for_each(|kline| {
                let tick = parse_http_kline_into_tick_data(symbol.to_string(), kline).unwrap();
                tick_data.push(tick);
            });
        }
    }

    let mut schema_fields: Vec<Field> = vec![];

    schema_fields.push(Field::new(
        "start_time",
        DataType::Datetime(TimeUnit::Milliseconds, None),
    ));

    for symbol in symbols.clone() {
        let (open_col, high_col, low_col, close_col) = get_symbol_ohlc_cols(&symbol);

        schema_fields.push(Field::new(open_col.as_str(), DataType::Float64));
        schema_fields.push(Field::new(high_col.as_str(), DataType::Float64));
        schema_fields.push(Field::new(close_col.as_str(), DataType::Float64));
        schema_fields.push(Field::new(low_col.as_str(), DataType::Float64));
    }
    let tick_data_schema = Schema::from_iter(schema_fields.into_iter());

    let new_tick_data_lf =
        consolidate_complete_tick_data_into_lf(&symbols, &tick_data, &tick_data_schema)?;

    let chrono_duration = ChronoDuration::from_std(bar_length)
        .expect("benchmark test -> couldn't convert std duration to chrono duration");
    let tick_data_lf = resample_tick_data_to_length(
        &symbols,
        chrono_duration,
        new_tick_data_lf,
        ClosedWindow::Right,
        None,
    )?;

    let mut initial_trading_data_lf = strategy
        .set_benchmark(tick_data_lf, last_bar)
        .expect("benchmark test -> strategy.set_benchmark.unwrap");

    // let mut performance_guard = performance_arc
    //     .lock()
    //     .expect("benchmark test -> performance_arc.unwrap");
    // let _ = performance_guard.set_benchmark(&initial_trading_data_lf);
    initial_trading_data_lf = initial_trading_data_lf.cache();
    let initial_trading_data_df = initial_trading_data_lf
        .collect()
        .expect("benchmark test -> performance_guard.set_benchmark.unwrap");

    let path = "data/test".to_string();
    let file_name = "benchmark_test.csv".to_string();
    save_csv(path.clone(), file_name, &initial_trading_data_df, true)
        .expect("benchmark test save_csv unrwap");

    Ok(())
}

// #[test]
// fn test_better_logs() {
//     println!("PREVIOUS VALUE");
//     for i in 0..10 {
//         print!("\rValue: {}", i);
//         std::io::stdout().flush().unwrap();

//         thread::sleep(Duration::from_secs(1));
//     }
//     println!();
// }

// #[tokio::test]
// async fn test_websocket_reconnection() {
//     use dotenv::dotenv;
// use tokio_tungstenite::tungstenite::Error as TungsteniteError;
//     dotenv().ok();

//     let bar_length = 60;

//     let bar_length = Duration::from_secs(bar_length);
//     let current_datetime = current_datetime();
//     let seconds_to_next_full_minute = 60 - current_datetime.timestamp() % 60;
//     let last_bar = current_datetime + ChronoDuration::seconds(seconds_to_next_full_minute);

//     let symbols = vec!["BTCUSDT".to_string(), "AGIXUSDT".to_string()];

//     // handle_websocket_reconnect(bar_length, last_bar, symbols)
//     //     .await
//     //     .expect("test error");
// }

// async fn handle_websocket(
//     websocket_stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
// ) -> Result<(), TungsteniteError> {
//     loop {
//         tokio::select! {
//             // Handle incoming WebSocket message
//             Some(Ok(message)) = websocket_stream.try_next() => {
//                 match message {
//                     Message::Text(text) => {
//                         println!("Received message: {}", text);
//                     }
//                     _ => {}
//                 }
//             }
//             // Handle WebSocket errors
//             // Err(err) = websocket_stream.try_next() => {
//             //     match err {
//             //     _ => {

//             //     }
//             //     }
//             // }
//         }
//     }
//     Ok(())
// }

// #[tokio::test]
// async fn test_sleep() {
//     println!("WAIT 3 SECS");
//     select! {
//         _ = sleep(Duration::from_secs(3)) => {
//          println!("FETCH FROM LAST NOT CLOSED MINUTE");
//         }
//     }
// }
