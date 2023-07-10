use super::super::enums::log_level::LogLevel;
use super::super::enums::request_method::RequestMethod;
use super::super::errors::Error;
use super::super::functions::{
    clear_tick_data_to_last_bar, consolidate_tick_data_into_lf, current_datetime, fetch_data,
    get_symbol_ohlc_cols, parse_http_kline_into_tick_data, parse_ws_kline_into_tick_data,
    resample_tick_data_secs_to_min, resample_tick_data_to_min,
    timestamp_end_to_daily_timestamp_sec_intervals,
};
use super::contract::Contract;
use super::performance::Performance;
use super::strategy::Strategy;
use super::{Request, WsKlineResponse};
use crate::trader::constants::SECONDS_IN_MIN;
use chrono::{Duration, NaiveDateTime};
use futures_util::{SinkExt, StreamExt};
use log::*;
use polars::prelude::*;
use reqwest::Client;
use std::env::{self};
use std::time::Duration as TimeDuration;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;
// use tokio::sync::watch::error::SendError;
// use tokio::sync::watch::Sender;

#[derive(Clone)]
pub struct MarketDataFeed {
    contracts: [Contract; 2],
    bar_length: Duration,
    last_bar: NaiveDateTime,
    tick_data_schema: Schema,
    log_level: LogLevel,
    http: Client,
    initial_fetch_delay: TimeDuration,
    initial_fetch_offset: i64, // offsets number of minutes from indicators, so that we can have information for the full previous day
                               // socket: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
                               // pub sender: Sender<MarketDataFeedDTE>,
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
        contracts: [Contract; 2],
        bar_length: i64,
        initial_fetch_offset: i64,
        log_level: LogLevel,
        // sender: Sender<MarketDataFeedDTE>,
    ) -> MarketDataFeed {
        let mut schema_fields: Vec<Field> = vec![];

        schema_fields.push(Field::new(
            "start_time",
            DataType::Datetime(TimeUnit::Milliseconds, None),
        ));

        for contract in &contracts {
            let (open_col, high_col, low_col, close_col) = get_symbol_ohlc_cols(&contract.symbol);

            schema_fields.push(Field::new(open_col.as_str(), DataType::Float64));
            schema_fields.push(Field::new(high_col.as_str(), DataType::Float64));
            schema_fields.push(Field::new(close_col.as_str(), DataType::Float64));
            schema_fields.push(Field::new(low_col.as_str(), DataType::Float64));
        }
        let tick_data_schema = Schema::from_iter(schema_fields.into_iter());

        let bar_length = Duration::seconds(bar_length);
        let current_datetime: NaiveDateTime = current_datetime();
        let seconds_to_next_full_minute = 60 - current_datetime.timestamp() % 60;
        let last_bar = current_datetime + Duration::seconds(seconds_to_next_full_minute);
        println!(
            "{} | Initializing MarketDataFeed -> {} seconds to next full minute, initial last bar {}",
            current_datetime, seconds_to_next_full_minute, last_bar
        );

        MarketDataFeed {
            contracts,
            bar_length,
            last_bar,
            log_level,
            http: Client::new(),
            initial_fetch_delay: TimeDuration::from_secs(seconds_to_next_full_minute as u64),
            initial_fetch_offset,
            tick_data_schema,
            // sender,
        }
    }

    fn symbols(&self) -> [String; 2] {
        self.contracts.clone().map(|c| c.symbol)
    }

    pub async fn init(
        &mut self,
        strategy: &mut Strategy,
        performance: &mut Performance,
        initial_balance: f64,
    ) {
        let connection = self.connect().await;
        match connection {
            Err(e) => {
                error!("web socket stream connection error {:?}", e);
            }
            Ok(mut ws) => match self.listen_to_klines_live_data(&mut ws).await {
                Ok(()) => {
                    let _events = self.listen_websocket(&mut ws, strategy).await;
                }
                Err(e) => error!("Klines error {:?}", e),
            },
        }
    }

    pub async fn fetch_benchmark_data(
        &self,
        strategy: &mut Strategy,
        performance: &mut Performance,
        initial_balance: f64,
    ) {
        match self.fetch_historical_data().await {
            Ok(lf) => {
                // let dte = MarketDataFeedDTE::SetBenchmark(
                //     lf.clone(),
                //     self.last_bar.clone(),
                //     self.symbols[1].clone(),
                // );
                // let _ = self.send(dte);
                let _ = strategy.set_benchmark(performance, lf, &self.last_bar, &self.contracts[1]);
            }
            Err(e) => error!("get historical data error {:?}", e),
        }
    }

    async fn fetch_historical_data(&self) -> Result<LazyFrame, Error> {
        let limit = 720; // TODO: limit hardcoded

        let timestamp_end = self.last_bar.timestamp();
        let timestamp_intervals =
            timestamp_end_to_daily_timestamp_sec_intervals(timestamp_end, limit, 1); // TODO: granularity hardcoded
        let mut tick_data = vec![];

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

            let end = &timestamp_intervals[i] * 1000;

            if value == timestamp_intervals.last().unwrap() {
                sleep(self.initial_fetch_delay).await;
                // let start = Instant::now();
                // let sleep_duration = Duration::seconds(fetch_offset);
                // while Instant::now().duration_since(start) < fetch_offset {
                //     println!("Sleeping...");
                //     sleep(Duration::from_secs(1)).await;
                // }
            }
            for symbol in &self.symbols() {
                println!(
                    "fetching {} data, start= {}, end= {}, limit= {}",
                    symbol, start, end, current_limit
                );
                let fetched_klines =
                    fetch_data(&self.http, symbol, &start, &end, current_limit).await?;
                fetched_klines.iter().for_each(|kline| {
                    let tick = parse_http_kline_into_tick_data(symbol.to_string(), kline).unwrap();
                    tick_data.push(tick);
                });
            }
        }

        let new_tick_data_lf =
            consolidate_tick_data_into_lf(&self.symbols(), &tick_data, &self.tick_data_schema)?;

        let tick_data_lf =
            resample_tick_data_to_min(&self.symbols(), &self.bar_length, new_tick_data_lf)?;

        Ok(tick_data_lf)
    }

    /// A single connection to stream.binance.com is only valid for 24 hours; expect to be disconnected at the 24 hour mark
    pub async fn connect(&mut self) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
        let binance_ws_base_url = env::var("BINANCE_WS_BASE_URL")?;

        if self.log_level > LogLevel::Trades {
            info!("Connecting to Binance WS {}", binance_ws_base_url);
        }

        let url = Url::parse(&format!("{}/ws/bookTicker", binance_ws_base_url))?; // ws url

        let (ws_stream, _) = connect_async(url).await?;
        // self.socket = Some(ws_stream);

        Ok(ws_stream)
    }

    pub async fn listen_to_klines_live_data(
        &mut self,
        wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<(), Error> {
        let mut ticker_params: Vec<String> = vec![];
        let symbols = self.symbols();
        if symbols[0] != symbols[1] {
            for symbol in symbols {
                let kline_param = format!("{}@kline_1m", symbol.to_lowercase());
                ticker_params.push(kline_param);
            }
        } else {
            ticker_params.push(format!("{}@kline_1m", symbols[0].to_lowercase()));
        }

        let request = Request {
            method: RequestMethod::SUBSCRIBE,
            params: ticker_params,
            id: 1,
        };

        let json_str_result = serde_json::to_string(&request);

        let json_str = match json_str_result {
            Ok(json_str) => json_str,
            Err(error) => {
                println!("JSON ({:?}) parsing error: {:?}", request, error);
                String::from("{}")
            }
        };

        let subscription_message = Message::Text(json_str);
        let result = wss.send(subscription_message).await?;

        Ok(result)
    }

    pub async fn listen_websocket(
        &mut self,
        wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        strategy: &mut Strategy,
    ) -> Result<(), Error> {
        // let socket: &mut WebSocketStream<MaybeTlsStream<TcpStream>> = self.socket.as_mut().unwrap();
        println!("Listen events start {:?}", current_datetime());
        let mut tick_data_payload = vec![];
        loop {
            tokio::select! {
                ws_message = wss.next() => {
                    let message = ws_message.unwrap()?;
                    let message_str = message.to_string();
                    if !message_str.contains("\"e\":\"kline\"") {
                        continue;
                    }
                    let kline_response: WsKlineResponse = serde_json::from_str(&message_str)?;
                    let tick_data = parse_ws_kline_into_tick_data(kline_response)?;
                    let last_tick_datetime = tick_data.date;
                    let diff = self.last_bar - tick_data.date;
                    // println!("tickbar diff = {}", diff.num_seconds());
                    if diff.num_seconds() > 0 {
                        println!("{:?} | {:?}", tick_data.date, tick_data);
                        continue;
                    }

                    println!("{:?} | + {:?}", tick_data.date, tick_data);

                    tick_data_payload.push(tick_data);

                    if last_tick_datetime - self.last_bar >= self.bar_length {
                        let new_tick_data_lf = consolidate_tick_data_into_lf(&self.symbols(), &tick_data_payload, &self.tick_data_schema)?;
                        let new_tick_data_lf = resample_tick_data_secs_to_min(
                            &self.symbols(),
                            &new_tick_data_lf,
                            &self.tick_data_schema,
                            &self.last_bar,
                            &self.bar_length,
                        )?;

                        let filter_datetime = self.last_bar - Duration::days(1);
                        // tick_data_lf = concat_and_clean_lazyframes([tick_data_lf, new_tick_data_lf], filter_datetime)?;
                        self.last_bar = last_tick_datetime;
                        tick_data_payload = clear_tick_data_to_last_bar(tick_data_payload, &self.last_bar);
                        // TODO: this needs to be done in ws instead
                        // tick_data_payload = clear_tick_data_to_last_bar(tick_data_payload, &previous_bar);
                        // tick_data_payload.clear();
                    }
                },
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
