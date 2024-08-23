use super::{
    dtos::{http::response::BinanceHttpKlineResponse, ws::outgoing::WsOutgoingMessage},
    enums::OutgoingWsMessageMethod,
};
use crate::{
    binance::{enums::IncomingWsMessage, functions::from_tick_to_tick_data},
    config::WS_RECONNECT_INTERVAL_IN_SECS,
};
use chrono::{Datelike, Duration as ChronoDuration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use common::{
    constants::{SECONDS_IN_DAY, SECONDS_IN_MIN},
    enums::trading_data_update::TradingDataUpdate,
    functions::{
        current_datetime, current_timestamp, get_fetch_timestamps_interval,
        map_and_downsample_ticks_data_to_df,
    },
    structs::{BehaviorSubject, LogKlines, Symbol, SymbolsPair, TickData},
    traits::exchange::DataProviderExchange,
};
use futures_util::SinkExt;
use glow_error::{assert_or_error, GlowError};
use polars::prelude::{IntoLazy, LazyFrame, Schema};
use reqwest::Client;
use serde_json::{from_str, to_string};
use std::{
    collections::{HashMap, HashSet},
    env::var as env_var,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpStream,
    select, spawn,
    time::{sleep, sleep_until, Instant},
};
use tokio_stream::StreamExt;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

#[derive(Clone)]
pub struct BinanceDataProvider {
    http: Client,
    kline_duration: Duration,
    last_ws_error_ts: Arc<Mutex<Option<i64>>>,
    minimum_klines_for_benchmarking: u32,
    staged_ticks: HashMap<u32, Vec<TickData>>, // TODO: change to array to avoid heap allocation
    symbols: (&'static str, &'static str),
    unique_symbols: Vec<&'static Symbol>,
    ticks_to_commit: BehaviorSubject<Vec<TickData>>, // TODO: change to array to avoid heap allocation
    trading_data_update_listener: BehaviorSubject<TradingDataUpdate>,
}

/// A single connection to stream.binance.com is only valid for 24 hours; expect to be disconnected at the 24 hour mark
impl BinanceDataProvider {
    pub fn new(
        kline_duration: Duration,
        last_ws_error_ts: &Arc<Mutex<Option<i64>>>,
        minimum_klines_for_benchmarking: u32,
        symbols_pair: SymbolsPair,
        trading_data_update_listener: &BehaviorSubject<TradingDataUpdate>,
    ) -> Self {
        // let wss = Self::connect_websocket().await.expect("wss to be provided");
        let symbols = &symbols_pair.get_tuple();
        let unique_symbols = symbols_pair.get_unique_symbols();

        Self {
            http: Client::new(),
            // kline_data_schema,
            kline_duration,
            last_ws_error_ts: last_ws_error_ts.clone(),
            minimum_klines_for_benchmarking,
            staged_ticks: HashMap::new(),
            symbols: *symbols,
            ticks_to_commit: BehaviorSubject::new(vec![]),
            // trading_data_schema,
            trading_data_update_listener: trading_data_update_listener.clone(),
            unique_symbols,
        }
    }

    async fn fetch_benchmark_available_data(
        http: &Client,
        kline_data_schema: Schema,
        unique_symbols: &Vec<&Symbol>,
        minimum_klines_for_benchmarking: u32,
        kline_duration_in_secs: i64,
        benchmark_end_ts: i64,   // seconds
        benchmark_start_ts: i64, // seconds
    ) -> Result<LazyFrame, GlowError> {
        let max_limit: i64 = 1000; // TODO: limit hardcoded
        let granularity_in_mins = 1; // TODO: granularity hardcoded
        let timestamp_intervals = get_fetch_timestamps_interval(
            minimum_klines_for_benchmarking as i64,
            granularity_in_mins,
            benchmark_start_ts,
            benchmark_end_ts,
            max_limit,
            Some(1),
        );

        println!("timestamp interval {:?}", timestamp_intervals);

        let mut tick_data = vec![];

        for (i, value) in timestamp_intervals.iter().enumerate() {
            let start_ts: i64;
            if i == 0 {
                // skip i == 0, as &timestamp_intervals[i - 1] doesn't exist
                continue;
            }

            start_ts = &timestamp_intervals[i - 1] * 1000;

            let mut end_ts = &timestamp_intervals[i] * 1000;

            let current_limit =
                granularity_in_mins * (((end_ts - start_ts) / 1000) / SECONDS_IN_MIN);

            end_ts -= 1;

            if value == timestamp_intervals.last().unwrap() {
                end_ts -= kline_duration_in_secs * 1000;
            }

            for symbol in unique_symbols {
                let fetched_klines =
                    Self::fetch_data(http, symbol.name, start_ts, end_ts, current_limit).await?;
                tick_data.extend(fetched_klines);
            }
        }

        let tick_data_df = map_and_downsample_ticks_data_to_df(
            &kline_data_schema,
            ChronoDuration::seconds(kline_duration_in_secs),
            &tick_data,
            unique_symbols,
            false,
        )?;
        // println!("{:?}", tick_data_df);
        let tick_data_lf = tick_data_df.lazy();

        Ok(tick_data_lf)
    }

    async fn fetch_data(
        http: &Client,
        symbol: &'static str,
        start_timestamp_ms: i64, // ms
        end_timestamp_ms: i64,   // ms
        limit: i64,              //Default 500; max 1000.
    ) -> Result<Vec<TickData>, GlowError> {
        assert!(limit <= 1000, "Limit must be equal or less than 1000");
        assert!(limit > 0, "Limit must be greater than 0");

        let url = format!(
            "https://api3.binance.com/api/v3/klines?symbol={}&interval={}&startTime={}&endTime={}&limit={}",
            symbol, "1m", start_timestamp_ms, end_timestamp_ms, limit
        );

        println!(
            "{:?} | 🦴 Fetching {} data ({} records) for interval between {} and {}",
            current_datetime(),
            symbol,
            limit,
            NaiveDateTime::from_timestamp_millis(start_timestamp_ms).unwrap(),
            NaiveDateTime::from_timestamp_millis(end_timestamp_ms).unwrap()
        );

        let result: Vec<BinanceHttpKlineResponse> = http.get(url).send().await?.json().await?;
        let result = result
            .into_iter()
            .map(move |data| {
                let open = data.open.parse::<f64>().unwrap();
                let close = data.close.parse::<f64>().unwrap();
                let high = data.high.parse::<f64>().unwrap();
                let low = data.low.parse::<f64>().unwrap();
                let start_time =
                    NaiveDateTime::from_timestamp_opt(data.timestamp / 1000, 0).unwrap();
                TickData::new_from_string(symbol, start_time, open, high, close, low)
            })
            .collect();
        Ok(result)
    }
}

impl DataProviderExchange for BinanceDataProvider {
    async fn subscribe_to_tick_stream(
        &mut self,
        wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<(), GlowError> {
        let ticker_params: Vec<String> = self
            .unique_symbols
            .clone()
            .into_iter()
            .map(|s| s.name.to_string())
            .collect();

        let subscribe_message = WsOutgoingMessage {
            method: OutgoingWsMessageMethod::Subscribe,
            params: ticker_params,
            id: 1,
        };

        let subscribe_json_str = to_string(&subscribe_message)
            .expect(&format!("JSON ({:?}) parsing error", subscribe_message));

        let subscription_message = Message::Text(subscribe_json_str);
        wss.send(subscription_message)
            .await
            .map_err(|err| GlowError::from(err))
    }

    async fn listen_ticks(
        &mut self,
        mut wss: WebSocketStream<MaybeTlsStream<TcpStream>>,
        benchmark_end: NaiveDateTime,
        trading_data_schema: &Schema,
    ) -> Result<(), GlowError> {
        self.subscribe_to_tick_stream(&mut wss).await?;

        // let mut current_staged_kline_start = self.benchmark_end;
        let discard_ticks_before = benchmark_end - ChronoDuration::nanoseconds(1);
        let mut current_staged_kline_minute = benchmark_end.time().minute();

        let chrono_kline_duration = ChronoDuration::from_std(self.kline_duration)
            .expect("init -> error converting std duration to chrono duration");

        // let mut staging_ticks: HashMap<u32, Vec<TickData>> = HashMap::new();
        // let ticks_data_to_process: BehaviorSubject<Vec<TickData>> = BehaviorSubject::new(vec![]);
        let mut ticks_to_commit_subscription = self.ticks_to_commit.subscribe();

        let unique_symbols_len = self.unique_symbols.len();

        loop {
            select! {
                message = wss.try_next() => {
                    // TODO: clean this code
                    if let Err(error) = message {
                        let mut last_error_guard = self.last_ws_error_ts
                        .lock()
                        .expect("handle_websocket -> last_error_guard unwrap");
                        let error_timestamp = current_timestamp();
                        *last_error_guard = Some(error_timestamp);
                        eprintln!("WebSocket message error: {:?}", error);
                        return Err(GlowError::from(error));
                    }
                    let message = message?;
                    if let None = message {
                        continue;
                    }
                    let message = message.unwrap();
                    match message {
                        Message::Text(json) => {
                            let incoming_msg = from_str::<IncomingWsMessage>(&json).unwrap_or_default();
                            match incoming_msg {
                                IncomingWsMessage::Tick(tick) => {
                                    let tick_data = from_tick_to_tick_data(tick, &self.symbols);

                                    let tick_time = tick_data.start_time.time();
                                    let tick_minute = tick_time.minute();
                                    let tick_second = tick_time.second();
                                    // we assume that if the received tick minute is the same as the current staged kline
                                    // the tick still belongs to the kline
                                    if tick_minute == current_staged_kline_minute {
                                        self.staged_ticks.entry(tick_second).or_insert(Vec::new()).push(tick_data.clone());
                                    } else {
                                        // otherwise, all ticks regarding the staged kline were already provided
                                        // and the ticks must be committed as kline data

                                        // commit ticks to kline data
                                        self.ticks_to_commit.next(self.staged_ticks.values().cloned().into_iter().flat_map(|vec| vec.into_iter()).collect());

                                        // clear staged ticks
                                        self.staged_ticks.clear();

                                        // insert the new tick data at respective map second
                                        self.staged_ticks.insert(tick_second, vec![tick_data.clone()]);
                                        // and update current committed kline minute
                                        current_staged_kline_minute = tick_minute;
                                    }

                                    let second_staged_ticks = self.staged_ticks.get(&tick_second).unwrap();
                                    if second_staged_ticks.len() == unique_symbols_len {
                                        print!("{}", LogKlines(second_staged_ticks.to_vec()));
                                    }
                                },
                                fallback => {
                                    println!("fallback incoming msg from binance data provider {:?}", fallback);
                                }
                            }
                        },
                        Message::Ping(_) => {
                           wss.send(Message::Pong(vec![])).await?
                        },
                        fallback => {
                            println!("fallback msg from binance data provider {:?}", fallback);
                        }
                    }
                },
                // TODO: separate this if convenient
                committed_ticks = ticks_to_commit_subscription.next() => {
                    if committed_ticks.is_none() {
                        continue;
                    }

                    let mut committed_ticks = committed_ticks.unwrap();

                    if committed_ticks.len() <= 0 || committed_ticks.iter().filter(|tick| tick.start_time > discard_ticks_before).collect::<Vec<_>>().len() <= 0 {
                        continue;
                    }

                    committed_ticks.sort_by(|a, b| a.start_time.cmp(&b.start_time));

                    // let kline_start = current_staged_kline_start;
                    // current_staged_kline_start += chrono_kline_duration;


                    let commited_kline_df = map_and_downsample_ticks_data_to_df(
                        trading_data_schema,
                        chrono_kline_duration,
                        &committed_ticks,
                        &self.unique_symbols,
                        true
                    )?;


                    let trading_data_update = TradingDataUpdate::MarketData{ last_period_tick_data: commited_kline_df};
                    self.trading_data_update_listener.next(trading_data_update);

                },
            }
        }
    }

    async fn handle_http_klines_fetch(
        &self,
        benchmark_start_ts: i64,
        benchmark_end_ts: i64,
        kline_data_schema: &Schema,
        trading_data_schema: &Schema,
    ) -> Result<(), GlowError> {
        let kline_duration_in_secs = self.kline_duration.as_secs() as i64;

        let http = self.http.clone();
        let minimum_klines_for_benchmarking = self.minimum_klines_for_benchmarking;
        let unique_symbols = self.unique_symbols.clone();
        let kline_data_schema = kline_data_schema.clone();

        let fetch_data_handle = spawn(async move {
            let _ = Self::fetch_benchmark_available_data(
                &http,
                kline_data_schema,
                &unique_symbols,
                minimum_klines_for_benchmarking,
                kline_duration_in_secs,
                benchmark_end_ts,
                benchmark_start_ts,
            )
            .await;
        });

        let _ = fetch_data_handle.await;

        let current_timestamp = current_timestamp();
        let is_last_kline_available = current_timestamp > benchmark_end_ts;

        if is_last_kline_available {
            return Ok(());
        }

        let http = self.http.clone();
        let unique_symbols = self.unique_symbols.clone();
        let kline_duration = self.kline_duration.clone();
        let trading_data_update_listener = self.trading_data_update_listener.clone();
        let trading_data_schema = trading_data_schema.clone();

        let last_kline_available_handle = spawn(async move {
            let seconds_until_benchmark_end = benchmark_end_ts - current_timestamp;
            let duration_until_benchmark_end =
                Duration::from_secs(seconds_until_benchmark_end as u64);
            let benchmark_end_available_at = Instant::now() + duration_until_benchmark_end;

            sleep_until(benchmark_end_available_at).await;

            let mut kline_data = Vec::new();

            // {
            //     let last_error_guard = self
            //         .last_ws_error_ts
            //         .lock()
            //         .expect("handle_websocket -> last_error_guard unwrap");
            //     if let Some(last_error_ts) = &*last_error_guard {
            //         let remainder_seconds_to_next_minute = last_error_ts % 60;
            //         start_ms = (last_error_ts - remainder_seconds_to_next_minute) * 1000;
            //     }
            // }
            let remaining_seconds_from_current_ts = current_timestamp % 60;
            let start_ms = (current_timestamp - remaining_seconds_from_current_ts) * 1000;
            let end_ms = start_ms + (kline_duration_in_secs * 1000);

            let current_limit = (end_ms - start_ms) / (kline_duration_in_secs * 1000);

            for symbol in unique_symbols.clone() {
                let symbol_kline_data =
                    Self::fetch_data(&http, symbol.name, start_ms, end_ms, current_limit)
                        .await
                        .expect("fetch data to work");
                kline_data.extend(symbol_kline_data);
            }

            let commited_kline_df = map_and_downsample_ticks_data_to_df(
                &trading_data_schema,
                ChronoDuration::from_std(kline_duration)
                    .expect("init -> error converting std duration to chrono duration"),
                &kline_data,
                &unique_symbols,
                true,
            )
            .expect("map_and_downsample_ticks_data_to_df data to work");

            let trading_data_update = TradingDataUpdate::MarketData {
                last_period_tick_data: commited_kline_df,
            };
            trading_data_update_listener.next(trading_data_update);
            // {
            //     let mut last_error_guard = self.last_ws_error_ts.lock().unwrap();
            //     *last_error_guard = None;
            // }
        });

        last_kline_available_handle.await;

        Ok(())
    }

    async fn init(
        &mut self,
        benchmark_end: Option<NaiveDateTime>,
        benchmark_start: Option<NaiveDateTime>,
        kline_data_schema: Schema,
        run_benchmark_only: bool,
        trading_data_schema: Schema,
    ) -> Result<(), GlowError> {
        let (benchmark_start, benchmark_end) =
            fallback_benchmark_datetimes(benchmark_start, benchmark_end)?;

        let _ = self
            .handle_http_klines_fetch(
                benchmark_start.timestamp(),
                benchmark_end.timestamp(),
                &kline_data_schema,
                &trading_data_schema,
            )
            .await?;

        if run_benchmark_only {
            return Ok(());
        }

        let binance_ws_base_url = env_var("BINANCE_WS_BASE_URL")?;
        let url = Url::parse(&format!("{}/ws/bookTicker", binance_ws_base_url))?; // ws url

        loop {
            match connect_async(url.clone()).await {
                Ok((wss, resp)) => {
                    eprintln!(
                        "Data provider connection stablished. \n Response: {:?}",
                        resp
                    );
                    if let Err(err) = self
                        .listen_ticks(wss, benchmark_end, &trading_data_schema)
                        .await
                    {
                        let mut last_error_guard = self
                            .last_ws_error_ts
                            .lock()
                            .expect("init -> last_error_guard unwrap");
                        let error_timestamp = current_timestamp();
                        *last_error_guard = Some(error_timestamp);
                        eprintln!("Market Websocket connection error: {:?}. Retrying...", err);
                    }
                }
                Err(error) => {
                    {
                        let mut last_error_guard = self
                            .last_ws_error_ts
                            .lock()
                            .expect("init -> last_error_guard unwrap");
                        let error_timestamp = current_timestamp();
                        *last_error_guard = Some(error_timestamp);
                    }
                    eprintln!(
                        "Market Websocket connection failed. \n
                        Error: {} \n
                        Retrying in {} seconds...",
                        error.to_string(),
                        WS_RECONNECT_INTERVAL_IN_SECS
                    );
                    sleep(Duration::from_secs(WS_RECONNECT_INTERVAL_IN_SECS)).await;
                }
            }
        }
    }
}

fn fallback_benchmark_datetimes(
    benchmark_end: Option<NaiveDateTime>,
    benchmark_start: Option<NaiveDateTime>,
) -> Result<(NaiveDateTime, NaiveDateTime), GlowError> {
    let benchmark_end = benchmark_end.unwrap_or_else(|| {
        let current_datetime = current_datetime();
        let date = NaiveDate::from_ymd_opt(
            current_datetime.year(),
            current_datetime.month(),
            current_datetime.day(),
        )
        .unwrap();
        let time =
            NaiveTime::from_hms_opt(current_datetime.hour(), current_datetime.minute(), 0).unwrap();
        NaiveDateTime::new(date, time)
    });

    let benchmark_start = benchmark_start.unwrap_or(current_datetime());

    assert_or_error!(benchmark_end > benchmark_start_ts);

    Ok((benchmark_start_ts, benchmark_end_ts))
}
