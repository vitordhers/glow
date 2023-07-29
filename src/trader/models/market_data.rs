use super::super::enums::log_level::LogLevel;
use super::super::enums::request_method::RequestMethod;
use super::super::errors::Error;
use super::super::functions::{
    consolidate_complete_tick_data_into_lf, current_datetime, fetch_data, get_symbol_ohlc_cols,
    parse_http_kline_into_tick_data, parse_ws_kline_into_tick_data, resample_tick_data_to_length,
    timestamp_end_to_daily_timestamp_sec_intervals,
};
use super::behavior_subject::BehaviorSubject;
use super::exchange::{Exchange, WsProcesser};
use super::strategy::Strategy;
use super::{Request, WsKlineResponse};
use crate::trader::constants::SECONDS_IN_MIN;
use crate::trader::models::exchange::ProcesserAction;
use crate::trader::Order;
use chrono::{Duration, NaiveDateTime};
use futures_util::{SinkExt, StreamExt};
use log::*;
use polars::prelude::*;
use reqwest::Client;
use std::env::{self};
use std::sync::{Arc, Mutex};
use std::time::Duration as TimeDuration;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

#[derive(Clone)]
pub struct MarketDataFeed {
    exchange: BehaviorSubject<Box<dyn Exchange + Send + Sync>>,
    bar_length: Duration,
    last_bar: NaiveDateTime,
    tick_data_schema: Schema,
    log_level: LogLevel,
    http: Client,
    initial_fetch_delay: TimeDuration,
    initial_fetch_offset: i64,
    strategy_arc: Arc<Mutex<Strategy>>,
    current_balance: BehaviorSubject<f64>,
    staging_order: BehaviorSubject<Order>,
    // offsets number of minutes from indicators, so that we can have information for the full previous day
}

// #[derive(Clone)]
// pub enum MarketDataFeedDTE {
//     Nil,
//     SetBenchmark(LazyFrame, NaiveDateTime, String),
// }

// impl Default for MarketDataFeedDTE {
//     fn default() -> Self {
//         MarketDataFeedDTE::Nil
//     }
// }

impl MarketDataFeed {
    pub fn new(
        exchange: BehaviorSubject<Box<dyn Exchange + Send + Sync>>,
        bar_length: i64,
        initial_fetch_offset: i64,
        log_level: LogLevel,
        strategy_arc: Arc<Mutex<Strategy>>,
        current_balance: BehaviorSubject<f64>,
        staging_order: BehaviorSubject<Order>,
    ) -> MarketDataFeed {
        let mut schema_fields: Vec<Field> = vec![];

        schema_fields.push(Field::new(
            "start_time",
            DataType::Datetime(TimeUnit::Milliseconds, None),
        ));
        let biding = exchange.value();
        let symbols = biding.get_current_symbols();
        for symbol in symbols {
            let (open_col, high_col, low_col, close_col) = get_symbol_ohlc_cols(&symbol);

            schema_fields.push(Field::new(open_col.as_str(), DataType::Float64));
            schema_fields.push(Field::new(high_col.as_str(), DataType::Float64));
            schema_fields.push(Field::new(close_col.as_str(), DataType::Float64));
            schema_fields.push(Field::new(low_col.as_str(), DataType::Float64));
        }
        let tick_data_schema = Schema::from_iter(schema_fields.into_iter());

        let bar_length = Duration::seconds(bar_length);
        let current_datetime = current_datetime();
        let seconds_to_next_full_minute = 60 - current_datetime.timestamp() % 60;
        let last_bar = current_datetime + Duration::seconds(seconds_to_next_full_minute);
        println!(
            "{} | Initializing MarketDataFeed -> {} seconds to next full minute, initial last bar {}",
            current_datetime, seconds_to_next_full_minute, last_bar
        );

        MarketDataFeed {
            exchange,
            bar_length,
            last_bar,
            log_level,
            http: Client::new(),
            initial_fetch_delay: TimeDuration::from_secs(seconds_to_next_full_minute as u64),
            initial_fetch_offset,
            tick_data_schema,
            strategy_arc,
            current_balance,
            staging_order,
        }
    }

    pub async fn init_binance_ws(&mut self) -> Result<(), Error> {
        let mut binance_wss = self
            .connect_to_binance_ws()
            .await
            .expect("Binance ws stream connection error");
        self.listen_to_binance_klines(&mut binance_wss)
            .await
            .expect("Binance ws listen to klines failed");

        let mut exchange_wss = self
            .connect_to_exchange_ws()
            .await
            .expect("Exchange ws stream connection error");

        self.listen_websocket(&mut binance_wss, &mut exchange_wss)
            .await
    }

    pub async fn fetch_benchmark_data(&self) {
        match self.fetch_historical_data().await {
            Ok(lf) => {
                let mut strategy_guard = self.strategy_arc.lock().unwrap();
                let _ = strategy_guard.set_benchmark(lf, &self.last_bar);
            }
            Err(e) => error!("get historical data error {:?}", e),
        }
    }

    fn get_current_symbols(&self) -> Vec<String> {
        let exchange_ref = self.exchange.value();
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

            let end = (&timestamp_intervals[i] * 1000) - 1;

            if value == timestamp_intervals.last().unwrap() {
                println!(
                    "{:?} | ⌛ Waiting {:?} secs until last ({}) klines data is available",
                    current_datetime(),
                    self.initial_fetch_delay.as_secs(),
                    NaiveDateTime::from_timestamp_millis(end)
                        .unwrap()
                        .format("%Y-%m-%d %H:%M")
                        .to_string()
                );
                sleep(self.initial_fetch_delay).await;
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

        let tick_data_lf = resample_tick_data_to_length(
            &symbols,
            &self.bar_length,
            new_tick_data_lf,
            ClosedWindow::Left,
            None,
        )?;

        Ok(tick_data_lf)
    }

    /// A single connection to stream.binance.com is only valid for 24 hours; expect to be disconnected at the 24 hour mark
    pub async fn connect_to_binance_ws(
        &self,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
        let binance_ws_base_url = env::var("BINANCE_WS_BASE_URL")?;

        if self.log_level > LogLevel::Trades {
            info!("Connecting to Binance WS {}", binance_ws_base_url);
        }

        let url = Url::parse(&format!("{}/ws/bookTicker", binance_ws_base_url))?; // ws url

        let (ws_stream, response) = connect_async(url).await?;
        println!("BINANCE WS RESPONSE {:?}", response);

        Ok(ws_stream)
    }

    pub async fn listen_to_binance_klines(
        &self,
        wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<(), Error> {
        let mut ticker_params: Vec<String> = vec![];
        let symbols = self.get_current_symbols();
        for symbol in symbols {
            let kline_param = format!("{}@kline_1m", symbol.to_lowercase());
            ticker_params.push(kline_param);
        }

        let subscribe_request = Request {
            method: RequestMethod::SUBSCRIBE,
            params: ticker_params,
            id: 1,
        };

        let subscribe_json_str = serde_json::to_string(&subscribe_request)
            .expect(&format!("JSON ({:?}) parsing error", subscribe_request));

        let subscription_message = Message::Text(subscribe_json_str);
        wss.send(subscription_message).await?;

        Ok(())
    }

    async fn connect_to_exchange_ws(
        &self,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
        let binding = self.exchange.value();

        let wss = binding.connect_to_ws().await?;
        Ok(wss)
    }

    pub async fn listen_websocket(
        &mut self,
        binance_wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        exchange_wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<(), Error> {
        // let socket: &mut WebSocketStream<MaybeTlsStream<TcpStream>> = self.socket.as_mut().unwrap();
        println!(
            "{:?} | 🔌 Started to listen to Binance websocket events",
            current_datetime()
        );
        loop {
            tokio::select! {
                ws_message = binance_wss.next() => {
                    let message = ws_message.unwrap()?;
                    let message_str = message.to_string();
                    if !message_str.contains("\"e\":\"kline\"") {
                        continue;
                    }
                    let kline_response: WsKlineResponse = serde_json::from_str(&message_str)?;
                    let tick_data = parse_ws_kline_into_tick_data(kline_response)?;
                    // let last_tick_datetime = tick_data.date;
                    let diff = self.last_bar - tick_data.date;
                    // println!("tickbar diff = {}", diff.num_seconds());

                    if diff.num_seconds() > 0 {
                        println!("{:?} | {:?}", tick_data.date, tick_data);
                        continue;
                    }

                    println!("{:?} | ✅ {:?}", tick_data.date, tick_data);
                    let mut guard = self.strategy_arc.lock().unwrap();
                    let symbols = self.get_current_symbols();

                    self.last_bar = guard.process_tick_data(tick_data, &self.last_bar, self.bar_length, &symbols)?;

                },
                ws_message = exchange_wss.next() =>{
                    let message = ws_message.unwrap()?;
                    let ws_json_response = message.to_string();
                    let binding = self.exchange.value();
                    let processer = binding.get_processer();

                    let action = processer.process_ws_message(ws_json_response);

                    match action {
                        ProcesserAction::Nil => {},
                        ProcesserAction::Auth { success } => {
                            println!("AUTH ACTION SUCCESS: {}", success);
                        },
                        ProcesserAction::UpdateOrder {order} =>{
                            self.staging_order.next(order);
                        }
                        ProcesserAction::UpdateBalance {balance_available} =>{
                            self.current_balance.next(balance_available);
                        }
                    }

                }
            }
        }
        // Ok(())
    }
}

// impl Emitter<MarketDataFeedDTE> for MarketDataFeed {
//     fn send(&self, dte: MarketDataFeedDTE) -> Result<(), SendError<MarketDataFeedDTE>> {
//         let result = self.sender.send(dte)?;
//         Ok(result)
//     }
// }
