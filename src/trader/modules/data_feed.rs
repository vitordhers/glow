use super::super::enums::log_level::LogLevel;
use super::super::errors::Error;
use super::super::functions::{
    consolidate_complete_tick_data_into_lf, current_datetime, fetch_data, get_symbol_ohlc_cols,
    parse_http_kline_into_tick_data, resample_tick_data_to_length,
    timestamp_end_to_daily_timestamp_sec_intervals,
};
use super::strategy::Strategy;
use crate::trader::constants::{SECONDS_IN_DAY, SECONDS_IN_MIN};
use crate::trader::enums::balance::Balance;
use crate::trader::enums::order_action::OrderAction;
use crate::trader::enums::processer_action::ProcesserAction;
use crate::trader::enums::trading_data_update::TradingDataUpdate;
use crate::trader::exchanges::binance::enums::{BinanceRequestMethod, BinanceWsResponse};
use crate::trader::exchanges::binance::models::BinanceWsRequest;
use crate::trader::functions::{
    current_timestamp_ms, map_tick_data_to_df, update_position_data_on_faulty_exchange_ws,
};
use crate::trader::models::behavior_subject::BehaviorSubject;
use crate::trader::models::execution::Execution;
use crate::trader::models::tick_data::{LogKlines, TickData};
use crate::trader::models::trade::Trade;
use crate::trader::traits::exchange::Exchange;
use chrono::{Duration as ChronoDuration, NaiveDateTime, Timelike, Utc};
use futures_util::SinkExt;
use log::*;
use polars::prelude::*;
use reqwest::Client;
use std::collections::HashMap;
use std::env::{self};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{interval, interval_at, sleep, sleep_until, Instant, Interval};
use tokio::{select, spawn};
use tokio_stream::StreamExt;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

const RECONNECT_INTERVAL: u64 = 2;

#[derive(Clone)]
pub struct DataFeed {
    /// bar length is in seconds
    pub bar_length: Duration,
    pub last_bar: NaiveDateTime,
    pub tick_data_schema: Schema,
    pub log_level: LogLevel,
    pub http: Client,
    pub initial_fetch_delay: Duration,
    // offsets number of minutes from indicators, so that we can have information for the full previous day
    pub initial_fetch_offset: i64,
    pub exchange_socket_error_arc: Arc<Mutex<Option<i64>>>,
    pub exchange_listener: BehaviorSubject<Box<dyn Exchange + Send + Sync>>,
    pub trading_data_update_listener: BehaviorSubject<TradingDataUpdate>,
    pub trading_data_listener: BehaviorSubject<DataFrame>,
    pub update_balance_listener: BehaviorSubject<Option<Balance>>,
    pub update_order_listener: BehaviorSubject<Option<OrderAction>>,
    pub update_executions_listener: BehaviorSubject<Vec<Execution>>,
    pub current_trade_listener: BehaviorSubject<Option<Trade>>,
}

impl DataFeed {
    pub fn new(
        bar_length: u64,
        initial_fetch_offset: i64,
        log_level: LogLevel,
        exchange_socket_error_arc: &Arc<Mutex<Option<i64>>>,
        trading_data_update_listener: &BehaviorSubject<TradingDataUpdate>,
        exchange_listener: &BehaviorSubject<Box<dyn Exchange + Send + Sync>>,
        trading_data_listener: &BehaviorSubject<DataFrame>,
        update_balance_listener: &BehaviorSubject<Option<Balance>>,
        update_order_listener: &BehaviorSubject<Option<OrderAction>>,
        update_executions_listener: &BehaviorSubject<Vec<Execution>>,
        current_trade_listener: &BehaviorSubject<Option<Trade>>,
    ) -> DataFeed {
        let mut schema_fields: Vec<Field> = vec![];

        schema_fields.push(Field::new(
            "start_time",
            DataType::Datetime(TimeUnit::Milliseconds, None),
        ));
        let exchange_binding = exchange_listener.value();
        let symbols = exchange_binding.get_current_symbols();
        for symbol in symbols {
            let (open_col, high_col, low_col, close_col) = get_symbol_ohlc_cols(&symbol);

            schema_fields.push(Field::new(open_col.as_str(), DataType::Float64));
            schema_fields.push(Field::new(high_col.as_str(), DataType::Float64));
            schema_fields.push(Field::new(close_col.as_str(), DataType::Float64));
            schema_fields.push(Field::new(low_col.as_str(), DataType::Float64));
        }
        let tick_data_schema = Schema::from_iter(schema_fields.into_iter());

        let bar_length = Duration::from_secs(bar_length);
        let current_datetime = current_datetime();
        let seconds_to_next_full_minute = 60 - current_datetime.timestamp() % 60;
        let last_bar = current_datetime + ChronoDuration::seconds(seconds_to_next_full_minute);
        println!(
            "{} | Initializing DataFeed -> {} seconds to next full minute, initial last bar {}",
            current_datetime, seconds_to_next_full_minute, last_bar
        );

        DataFeed {
            bar_length,
            last_bar,
            log_level,
            http: Client::new(),
            initial_fetch_delay: Duration::from_secs(seconds_to_next_full_minute as u64),
            initial_fetch_offset,
            tick_data_schema,
            exchange_socket_error_arc: exchange_socket_error_arc.clone(),
            exchange_listener: exchange_listener.clone(),
            trading_data_update_listener: trading_data_update_listener.clone(),
            trading_data_listener: trading_data_listener.clone(),
            update_balance_listener: update_balance_listener.clone(),
            update_order_listener: update_order_listener.clone(),
            update_executions_listener: update_executions_listener.clone(),
            current_trade_listener: current_trade_listener.clone(),
        }
    }

    pub async fn init(&mut self) -> Result<(), Error> {
        let exchange_socket_error_arc = self.exchange_socket_error_arc.clone();
        let current_trade_listener = self.current_trade_listener.clone();
        let exchange_listener = self.exchange_listener.clone();
        let update_balance_listener = self.update_balance_listener.clone();
        let update_order_listener = self.update_order_listener.clone();
        let update_executions_listener = self.update_executions_listener.clone();

        let exchange_ws_handle = spawn(async move {
            handle_exchange_websocket_reconnect(
                exchange_socket_error_arc,
                current_trade_listener,
                exchange_listener,
                update_balance_listener,
                update_order_listener,
                update_executions_listener,
            )
            .await
        });

        let http = self.http.clone();
        let symbols = self.get_current_symbols();
        let trading_data_listener = self.trading_data_listener.clone();
        let last_error_ts_arc = Arc::new(Mutex::new(None));
        let bar_length = self.bar_length.clone();
        let trading_data_update_listener: BehaviorSubject<TradingDataUpdate> =
            self.trading_data_update_listener.clone();
        let last_bar = self.last_bar;

        let binance_ws_handle = spawn(async move {
            handle_market_websocket_reconnect(
                &http,
                last_bar,
                bar_length,
                symbols,
                trading_data_listener,
                last_error_ts_arc,
                trading_data_update_listener,
            )
            .await
        });

        let data_feed_clone = self.clone();
        let benchmark_handle = spawn(async move {
            data_feed_clone.fetch_benchmark_data().await;
        });

        let _ = benchmark_handle.await;
        let _ = binance_ws_handle.await;
        let _ = exchange_ws_handle.await;

        Ok(())
    }

    pub async fn fetch_benchmark_data(&self) {
        match self.fetch_historical_data().await {
            Ok(initial_tick_data_lf) => {
                let trading_update = TradingDataUpdate::BenchmarkData {
                    initial_tick_data_lf,
                    initial_last_bar: self.last_bar,
                };
                self.trading_data_update_listener.next(trading_update);
            }
            Err(e) => error!("get historical data error {:?}", e),
        }
    }

    fn get_current_symbols(&self) -> Vec<String> {
        let exchange_ref = self.exchange_listener.value();
        exchange_ref.get_current_symbols()
    }

    async fn fetch_historical_data(&self) -> Result<LazyFrame, Error> {
        let limit = 720; // TODO: limit hardcoded

        let timestamp_end = self.last_bar.timestamp();
        let timestamp_intervals =
            timestamp_end_to_daily_timestamp_sec_intervals(timestamp_end, limit, 1); // TODO: granularity hardcoded
        let mut tick_data = vec![];
        let symbols = self.get_current_symbols();

        for (i, value) in timestamp_intervals.iter().enumerate() {
            let start: i64;
            let current_limit: i64;
            if i == 0 {
                // if first iteration, grab time offset by initial_fetch_offset
                start = (value - self.initial_fetch_offset * SECONDS_IN_MIN) * 1000;
                current_limit = self.initial_fetch_offset;
            } else {
                current_limit = limit;
                start = &timestamp_intervals[i - 1] * 1000;
            }

            let mut end = (&timestamp_intervals[i] * 1000) - 1;

            if value == timestamp_intervals.last().unwrap() {
                end -= self.bar_length.as_secs() as i64 * 1000;
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
            for symbol in &symbols {
                let fetched_klines =
                    fetch_data(&self.http, symbol, &start, &end, current_limit).await?;
                fetched_klines.iter().for_each(|kline| {
                    let tick = parse_http_kline_into_tick_data(symbol.to_string(), kline).unwrap();
                    tick_data.push(tick);
                });
            }
        }

        let new_tick_data_lf =
            consolidate_complete_tick_data_into_lf(&symbols, &tick_data, &self.tick_data_schema)?;

        let chrono_duration = ChronoDuration::from_std(self.bar_length)
            .expect("fetch_historical_data -> couldn't convert std duration to chrono duration");
        let tick_data_lf = resample_tick_data_to_length(
            &symbols,
            chrono_duration,
            new_tick_data_lf,
            ClosedWindow::Left,
            None,
        )?;

        Ok(tick_data_lf)
    }
}

async fn handle_exchange_websocket_reconnect(
    exchange_socket_error_arc: Arc<Mutex<Option<i64>>>,
    current_trade_listener: BehaviorSubject<Option<Trade>>,
    exchange_listener: BehaviorSubject<Box<dyn Exchange + Send + Sync>>,
    update_balance_listener: BehaviorSubject<Option<Balance>>,
    update_order_listener: BehaviorSubject<Option<OrderAction>>,
    update_executions_listener: BehaviorSubject<Vec<Execution>>,
) -> Result<(), Error> {
    loop {
        match connect_exchange_websocket(&exchange_listener).await {
            Ok(ws_stream) => {
                if let Err(err) = handle_exchange_websocket(
                    ws_stream,
                    &exchange_socket_error_arc,
                    &current_trade_listener,
                    &exchange_listener,
                    &update_balance_listener,
                    &update_order_listener,
                    &update_executions_listener,
                )
                .await
                {
                    let mut last_error_guard = exchange_socket_error_arc
                        .lock()
                        .expect("handle_websocket -> last_error_guard unwrap");
                    let timestamp = Utc::now().timestamp() * 1_000;
                    *last_error_guard = Some(timestamp);

                    eprintln!(
                        "{:?} | Exchange websocket connection error: {:?}. Retrying...",
                        current_timestamp_ms(),
                        err
                    );
                }
            }
            Err(error) => {
                {
                    let mut last_error_guard = exchange_socket_error_arc
                        .lock()
                        .expect("handle_websocket -> last_error_guard unwrap");
                    let timestamp = Utc::now().timestamp() * 1_000;
                    *last_error_guard = Some(timestamp);
                }
                eprintln!(
                    "Exchange WebSocket connection failed. Got error {:?} Retrying in {} seconds...",
                    error,
                    RECONNECT_INTERVAL
                );
                sleep(Duration::from_secs(RECONNECT_INTERVAL)).await;
            }
        }
    }
}

async fn connect_exchange_websocket(
    exchange_listener: &BehaviorSubject<Box<dyn Exchange + Send + Sync>>,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
    let exchange = exchange_listener.value();
    let url = exchange.get_ws_url()?;
    loop {
        match connect_async(url.clone()).await {
            Ok((ws_stream, _)) => return Ok(ws_stream),
            Err(_) => {
                eprintln!(
                    "Exchange WebSocket connection failed. Retrying in {} seconds...",
                    RECONNECT_INTERVAL
                );
                sleep(Duration::from_secs(RECONNECT_INTERVAL)).await;
            }
        }
    }
}

async fn handle_exchange_websocket(
    mut wss: WebSocketStream<MaybeTlsStream<TcpStream>>,
    last_error_ts_arc: &Arc<Mutex<Option<i64>>>,
    current_trade_listener: &BehaviorSubject<Option<Trade>>,
    exchange_listener: &BehaviorSubject<Box<dyn Exchange + Send + Sync>>,
    update_balance_listener: &BehaviorSubject<Option<Balance>>,
    update_order_listener: &BehaviorSubject<Option<OrderAction>>,
    update_executions_listener: &BehaviorSubject<Vec<Execution>>,
) -> Result<(), Error> {
    let binding = exchange_listener.value();
    binding.auth_ws(&mut wss).await?;
    binding.subscribe_ws(&mut wss).await?;

    let processer = binding.get_processer();
    let ping_interval = binding.get_ws_ping_interval();
    let message = binding.get_ws_ping_message();
    let exchange_wss_ping_interval_and_message = (ping_interval, message);

    let mut heartbeat_interval: Interval = interval(Duration::from_secs(
        exchange_wss_ping_interval_and_message.0,
    ));

    let mut sleep_deadline = Instant::now();
    let mut timeout_executed = false;

    loop {
        tokio::select! {
            ws_message = wss.next() => {
                    let message = ws_message.unwrap()?;

                    match message {
                        Message::Text(json) => {
                            let json = json.to_string();

                            let action = processer.process_ws_message(&json);

                            match action {
                                ProcesserAction::Nil => {},
                                ProcesserAction::Auth { success } => {
                                    println!("{:?} | Exchange websocket authentication success: {}", current_datetime(), success);
                                },
                                ProcesserAction::Execution { executions } => {
                                    // let update_action = UpdateAction::Executions(executions);
                                    // update_action_listener.next(update_action);
                                    update_executions_listener.next(executions)
                                },
                                ProcesserAction::UpdateOrder { updated_order } => {
                                    let order_action = OrderAction::Update(updated_order);
                                    // let update_action = UpdateAction::Order(Some(order_action));
                                    // update_action_listener.next(update_action);
                                    update_order_listener.next(Some(order_action))
                                },
                                ProcesserAction::CancelOrder { cancelled_order } => {
                                    let order_action = OrderAction::Cancel(cancelled_order);
                                    // let update_action = UpdateAction::Order(Some(order_action));
                                    // update_action_listener.next(update_action);
                                    update_order_listener.next(Some(order_action))
                                },
                                ProcesserAction::StopOrder {stop_order} => {
                                    let order_action = OrderAction::Stop(stop_order);
                                    update_order_listener.next(Some(order_action))
                                },
                                ProcesserAction::Balance { balance } => {
                                    // let update_action = UpdateAction::Balance(balance);
                                    // update_action_listener.next(update_action);
                                    update_balance_listener.next(Some(balance))
                                }
                            }
                        },
                        Message::Ping(_) => {
                            println!("received ping request");
                            wss.send(Message::Pong(vec![])).await?
                        },
                        _ => {

                        }
                    }
                },
                _ = sleep_until(sleep_deadline) => {
                    sleep_deadline = Instant::now() + Duration::from_secs(SECONDS_IN_DAY as u64);
                    if timeout_executed {
                        continue;
                    }

                    let _ = update_position_data_on_faulty_exchange_ws(
                        &last_error_ts_arc, &exchange_listener,
                        &current_trade_listener,
                        &update_balance_listener,
                        &update_order_listener,
                        &update_executions_listener
                    ).await;

                    {
                        let mut last_error_guard = last_error_ts_arc.lock().unwrap();
                        *last_error_guard = None;
                    }
                    timeout_executed = true;
                }
            _ = heartbeat_interval.tick() => {
                if let Some(ping_message) = exchange_wss_ping_interval_and_message.1.clone() {
                    let _send = wss.send(ping_message).await?;
                }
            }
        }
    }
}

async fn handle_market_websocket_reconnect(
    http: &Client,
    last_bar: NaiveDateTime,
    bar_length: Duration,
    symbols: Vec<String>,
    trading_data_listener: BehaviorSubject<DataFrame>,
    last_error_ts_arc: Arc<Mutex<Option<i64>>>,
    trading_data_update_listener: BehaviorSubject<TradingDataUpdate>,
) -> Result<(), Error> {
    loop {
        match connect_market_websocket().await {
            Ok(ws_stream) => {
                if let Err(err) = handle_market_websocket(
                    http,
                    ws_stream,
                    last_bar,
                    bar_length,
                    &symbols,
                    &trading_data_listener,
                    last_error_ts_arc.clone(),
                    trading_data_update_listener.clone(),
                )
                .await
                {
                    let mut last_error_guard = last_error_ts_arc
                        .lock()
                        .expect("handle_market_websocket_reconnect -> last_error_guard unwrap");
                    let timestamp = Utc::now().timestamp();
                    *last_error_guard = Some(timestamp);
                    eprintln!("Market Websocket connection error: {:?}. Retrying...", err);
                }
            }
            Err(_) => {
                {
                    let mut last_error_guard = last_error_ts_arc
                        .lock()
                        .expect("handle_market_websocket_reconnect -> last_error_guard unwrap");
                    let timestamp = Utc::now().timestamp();
                    *last_error_guard = Some(timestamp);
                }
                eprintln!(
                    "Market Websocket connection failed. Retrying in {} seconds...",
                    RECONNECT_INTERVAL
                );
                sleep(Duration::from_secs(RECONNECT_INTERVAL)).await;
            }
        }
    }
}

async fn connect_market_websocket() -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
    let binance_ws_base_url = env::var("BINANCE_WS_BASE_URL")?;
    let url = Url::parse(&format!("{}/ws/bookTicker", binance_ws_base_url))?; // ws url
    loop {
        match connect_async(url.clone()).await {
            Ok((ws_stream, _)) => return Ok(ws_stream),
            Err(_) => {
                eprintln!(
                    "WebSocket connection failed. Retrying in {} seconds...",
                    RECONNECT_INTERVAL
                );
                sleep(Duration::from_secs(RECONNECT_INTERVAL)).await;
            }
        }
    }
}

/// A single connection to stream.binance.com is only valid for 24 hours; expect to be disconnected at the 24 hour mark
async fn handle_market_websocket(
    http: &Client,
    mut wss: WebSocketStream<MaybeTlsStream<TcpStream>>,
    last_bar: NaiveDateTime,
    bar_length: Duration,
    symbols: &Vec<String>,
    trading_data_listener: &BehaviorSubject<DataFrame>,
    last_error_ts_arc: Arc<Mutex<Option<i64>>>,
    trading_data_update_listener: BehaviorSubject<TradingDataUpdate>,
) -> Result<(), Error> {
    let mut ticker_params: Vec<String> = vec![];

    for symbol in symbols {
        let kline_param = format!("{}@kline_1s", symbol.to_lowercase());
        ticker_params.push(kline_param);
    }

    let subscribe_request = BinanceWsRequest {
        method: BinanceRequestMethod::SUBSCRIBE,
        params: ticker_params,
        id: 1,
    };

    let subscribe_json_str = serde_json::to_string(&subscribe_request)
        .expect(&format!("JSON ({:?}) parsing error", subscribe_request));

    let subscription_message = Message::Text(subscribe_json_str);
    wss.send(subscription_message).await?;

    let datetime_now = current_datetime();
    let current_timestamp = datetime_now.timestamp();
    let seconds_to_next_full_minute = 60 - (current_timestamp % 60);

    let initial_timeout = Duration::from_secs(seconds_to_next_full_minute as u64);

    let mut last_bar = last_bar;

    let discard_klines_limit = last_bar - ChronoDuration::nanoseconds(1);

    let mut current_klines_last_minute = last_bar.time().minute();

    let bar_length_in_secs = bar_length.as_secs() as i64;
    let chrono_bar_length = ChronoDuration::from_std(bar_length)
        .expect("handle_market_websocket -> error converting std duration to chrono duration");

    let mut staging_ticks = HashMap::new();

    let ticks_data_to_process: BehaviorSubject<Vec<TickData>> = BehaviorSubject::new(vec![]);
    let mut ticks_data_subscription = ticks_data_to_process.subscribe();
    let mut query_deadline = Instant::now() + initial_timeout;
    let mut timeout_executed = false;
    let trading_data_binding = trading_data_listener.value();
    let mut trading_data_schema = trading_data_binding.schema();

    loop {
        select! {
            message_result = wss.try_next() => {
                match message_result {
                    Ok(message_opt) => {
                        if let Some(message) = message_opt {
                            match message {
                                Message::Text(json) => {
                                    let response: BinanceWsResponse =
                                        serde_json::from_str::<BinanceWsResponse>(&json).unwrap_or_default();

                                    match response {
                                        BinanceWsResponse::Kline(kline_response) => {
                                            let tick_data: TickData = kline_response.into();

                                            let tick_time = tick_data.start_time.time();
                                            let tick_minute = tick_time.minute();
                                            let tick_second = tick_time.second();

                                            if tick_minute == current_klines_last_minute {
                                                staging_ticks.entry(tick_second).or_insert(Vec::new()).push(tick_data.clone());
                                            } else {
                                                ticks_data_to_process.next(staging_ticks.values().cloned().into_iter().flat_map(|vec| vec.into_iter()).collect());
                                                staging_ticks.clear();
                                                staging_ticks.insert(tick_second, vec![tick_data.clone()]);
                                                current_klines_last_minute = tick_minute;
                                            }

                                            let second_staging_ticks = staging_ticks.get(&tick_second).unwrap();

                                            if second_staging_ticks.len() == 2 {
                                                print!("{}", LogKlines(second_staging_ticks.to_vec()));
                                            }
                                        },
                                        _ => {}
                                    }
                                },
                                Message::Ping(_) => {
                                    wss.send(Message::Pong(vec![])).await?
                                },
                                _ => {

                                }
                            }

                        }
                    },
                    Err(error) => {
                        let mut last_error_guard = last_error_ts_arc
                        .lock()
                        .expect("handle_websocket -> last_error_guard unwrap");
                        let timestamp = Utc::now().timestamp();
                        *last_error_guard = Some(timestamp);
                        eprintln!("WebSocket message error: {:?}", error);
                    }
                }
            },
            ticks_data = ticks_data_subscription.next() => {
                if ticks_data.is_none() {
                    continue;
                }

                let mut ticks_data = ticks_data.unwrap();
                if ticks_data.len() <= 0 || ticks_data.iter().filter(|tick| tick.start_time > discard_klines_limit).collect::<Vec<_>>().len() <= 0 {
                    continue;
                }

                ticks_data.sort_by(|a, b| a.start_time.cmp(&b.start_time));

                last_bar += chrono_bar_length;

                let prev_bar = last_bar - chrono_bar_length;
                let last_period_tick_data_df_opt = process_tick_data_df(&ticks_data, prev_bar, chrono_bar_length, &symbols, &trading_data_schema)?;
                if let Some(last_period_tick_data) = last_period_tick_data_df_opt {
                    let trading_data_update = TradingDataUpdate::MarketData{ last_period_tick_data};
                    trading_data_update_listener.next(trading_data_update);
                }
            },
            // _ = ticks_process_interval.tick() => {
            //     if staging_ticks.len() == 0 {
            //         continue;
            //     }
            //     // let last_start_time = staging_ticks.iter().last().unwrap().start_time;
            //     ticks_data_to_process.next(staging_ticks.clone());
            //     staging_ticks.clear();
            //     last_bar = last_bar + ChronoDuration::from_std(bar_length).unwrap();

            // },
            _ = sleep_until(query_deadline) => {
                query_deadline = Instant::now() + Duration::from_secs(SECONDS_IN_DAY as u64);
                if timeout_executed {
                    continue;
                }

                let trading_data_binding = trading_data_listener.value();
                trading_data_schema = trading_data_binding.schema();

                let mut ticks_data = Vec::new();

                let remainer_seconds_from_previous_minute = current_timestamp % 60;
                let mut start_ms = (current_timestamp - remainer_seconds_from_previous_minute) * 1000;
                let end_ms = start_ms + (bar_length_in_secs * 1000);
                {
                    let last_error_guard = last_error_ts_arc.lock().expect("handle_websocket -> last_error_guard unwrap");
                    if let Some(last_error_ts) = &*last_error_guard {
                        let remainder_seconds_to_next_minute = last_error_ts % 60;
                        start_ms = (last_error_ts - remainder_seconds_to_next_minute) * 1000;
                    }
                }
                let current_limit = (end_ms - start_ms) / (bar_length_in_secs * 1000);
                for symbol in symbols {
                    let fetched_klines =
                        fetch_data(http, symbol, &start_ms, &end_ms, current_limit).await?;
                    fetched_klines.iter().for_each(|kline| {
                        let tick = parse_http_kline_into_tick_data(symbol.to_string(), kline).unwrap();
                        ticks_data.push(tick);
                    });
                }
                let schema = trading_data_listener.value().schema();
                let new_ticks_data =
                    consolidate_complete_tick_data_into_lf(symbols, &ticks_data, &schema)?.collect()?;

                let trading_data_update = TradingDataUpdate::MarketData{ last_period_tick_data: new_ticks_data};
                trading_data_update_listener.next(trading_data_update);
                {
                    let mut last_error_guard = last_error_ts_arc.lock().unwrap();
                    *last_error_guard = None;
                }

                timeout_executed = true
            },
            // _ = heartbeat_interval.tick() => {
            //     println!("pong message must be sent");
            //     wss.send(item)
            // }

        }
    }

    // Ok(())
}

pub fn process_tick_data_df(
    ticks_data: &Vec<TickData>,
    prev_bar: NaiveDateTime,
    bar_length: ChronoDuration,
    symbols: &Vec<String>,
    trading_data_schema: &Schema,
) -> Result<Option<DataFrame>, Error> {
    let last_period_tick_data_df = map_tick_data_to_df(
        symbols,
        &ticks_data,
        &trading_data_schema,
        prev_bar,
        bar_length,
    )?;
    Ok(Some(last_period_tick_data_df))
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
