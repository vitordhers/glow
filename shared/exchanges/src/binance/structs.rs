use super::{
    dtos::{http::response::BinanceHttpKlineResponse, ws::outgoing::WsOutgoingMessage},
    enums::OutgoingWsMessageMethod,
};
use crate::{
    binance::{enums::IncomingWsMessage, functions::from_tick_to_tick_data},
    config::WS_RECONNECT_INTERVAL_IN_SECS,
};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use common::{
    enums::trading_data_update::TradingDataUpdate,
    functions::{
        coerce_df_to_schema,
        csv::{load_interval_tick_dataframe, save_kline_df_to_csv},
        current_datetime, current_timestamp, current_timestamp_ms,
        downsample_tick_lf_to_kline_duration, filter_df_timestamps_to_lf,
        get_date_start_and_end_timestamps, map_ticks_data_to_df, timestamp_minute_end,
        timestamp_minute_start,
    },
    structs::{BehaviorSubject, LogKlines, SymbolsPair, TickData, TradingSettings},
    traits::exchange::DataProviderExchange,
};
use futures_util::SinkExt;
use glow_error::{assert_or_error, GlowError};
use polars::{
    frame::DataFrame,
    prelude::{IntoLazy, Schema},
    time::ClosedWindow,
};
use reqwest::Client;
use serde_json::{from_str, to_string};
use std::{
    collections::HashMap,
    env::var as env_var,
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};
use strategy::Strategy;
use tokio::{
    net::TcpStream,
    spawn,
    time::{sleep, sleep_until, Instant},
};
use tokio_stream::StreamExt;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

#[derive(Clone)]
pub struct BinanceDataProvider {
    fetch_leeway: StdDuration,
    http: Client,
    kline_duration: Duration,
    last_ws_error_ts: Arc<Mutex<Option<i64>>>,
    minimum_klines_for_benchmarking: u32,
    staged_ticks: HashMap<u32, Vec<TickData>>, // TODO: change to array to avoid heap allocation
    symbols: SymbolsPair,
    ticks_to_commit: BehaviorSubject<Vec<TickData>>, // TODO: change to array to avoid heap allocation
    klines_data_update_emitter: BehaviorSubject<TradingDataUpdate>,
}

/// A single connection to stream.binance.com is only valid for 24 hours; expect to be disconnected at the 24 hour mark
impl BinanceDataProvider {
    pub fn new(trading_settings: &TradingSettings, strategy: &Strategy) -> Self {
        let symbols = trading_settings.symbols_pair;
        let kline_duration = trading_settings.granularity.get_chrono_duration();
        let last_ws_error_ts = Arc::new(Mutex::new(None));
        let minimum_klines_for_benchmarking = strategy.get_minimum_klines_for_calculation();
        let klines_data_update_emitter = BehaviorSubject::new(TradingDataUpdate::default());
        Self {
            fetch_leeway: StdDuration::from_secs(5),
            http: Client::new(),
            // kline_data_schema,
            kline_duration,
            last_ws_error_ts,
            minimum_klines_for_benchmarking,
            staged_ticks: HashMap::new(),
            symbols,
            ticks_to_commit: BehaviorSubject::new(vec![]),
            // trading_data_schema,
            klines_data_update_emitter,
        }
    }

    pub fn patch_settings(&mut self, trading_settings: &TradingSettings) {
        self.symbols = trading_settings.symbols_pair;
        self.kline_duration = trading_settings.granularity.get_chrono_duration();
    }

    pub fn patch_strategy(&mut self, strategy: &Strategy) {
        self.minimum_klines_for_benchmarking = strategy.get_minimum_klines_for_calculation();
    }

    async fn load_or_fetch_kline_data(
        &self,
        trading_data_schema: &Schema,
        start_datetime: NaiveDateTime,
        end_datetime: NaiveDateTime,
        tick_duration: Duration,
    ) -> Result<DataFrame, GlowError> {
        let mut kline_df = DataFrame::from(trading_data_schema);
        for symbol in &self.symbols.get_unique_symbols() {
            let (loaded_data_df, not_loaded_dates) =
                load_interval_tick_dataframe(start_datetime, end_datetime, &symbol, "binance")?;

            let mut result_df =
                loaded_data_df.unwrap_or_else(|| DataFrame::from(trading_data_schema));

            if result_df.schema().len() != trading_data_schema.len() {
                result_df = coerce_df_to_schema(result_df, trading_data_schema)?;
            }

            for (i, date) in not_loaded_dates.into_iter().enumerate() {
                let datetimes = get_date_start_and_end_timestamps(date);
                let mut day_ticks_data = vec![];
                if i > 0 {
                    // avoid spamming API
                    sleep(StdDuration::from_secs(1)).await;
                }
                for (start_timestamp_ms, end_timestamp_ms) in datetimes {
                    let fetched_ticks = self
                        .fetch_tick_data(symbol.name, start_timestamp_ms, end_timestamp_ms, 720)
                        .await?;
                    day_ticks_data.extend(fetched_ticks);
                }
                let fetched_data_df = map_ticks_data_to_df(&day_ticks_data)?;

                let total_klines = fetched_data_df.height() as i64;
                let daily_klines = Duration::days(1).num_seconds() / tick_duration.num_seconds();

                if total_klines == daily_klines {
                    let _ = save_kline_df_to_csv(&fetched_data_df, date, "binance", &symbol.name)?;
                }
                let fetched_data_df = coerce_df_to_schema(fetched_data_df, &trading_data_schema)?;
                match &result_df.vstack(&fetched_data_df) {
                    Ok(stacked_df) => {
                        result_df = stacked_df.sort(["start_time"], false, false)?;
                    }
                    Err(error) => {
                        println!("STACK ERROR 1 {:?}", error);
                        println!("RESULT DF SCHEMA {:?}", &result_df.schema());
                        println!("FETCHED DF SCHEMA 2{:?}", &fetched_data_df.schema());
                    }
                }
            }
            result_df = coerce_df_to_schema(result_df, &trading_data_schema)?;
            match kline_df.vstack(&result_df) {
                Ok(stacked_df) => {
                    kline_df = stacked_df;
                }
                Err(error) => {
                    println!("STACK ERROR 2 {:?}", error);
                }
            };
        }

        let mut kline_df = kline_df.sort(["start_time"], false, false)?;

        kline_df.align_chunks();
        let kline_lf = filter_df_timestamps_to_lf(kline_df, start_datetime, end_datetime)?;
        let kline_lf = downsample_tick_lf_to_kline_duration(
            &self.symbols.get_unique_symbols(),
            self.kline_duration,
            kline_lf,
            ClosedWindow::Left,
            Some(trading_data_schema),
        )?;

        match kline_lf.collect() {
            Ok(kline_df) => Ok(kline_df),
            Err(error) => {
                println!("COLLECT ERROR {:?}", error);
                Err(error.into())
            }
        }
    }

    async fn fetch_tick_data(
        &self,
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

        let result: Vec<BinanceHttpKlineResponse> = self.http.get(url).send().await?.json().await?;
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

    async fn fetch_data_after_waiting(
        &self,
        wait_until: Instant,
        start_timestamp_ms: i64,
        end_timestamp_ms: i64,
        trading_data_schema: &Schema,
    ) -> Result<DataFrame, GlowError> {
        sleep_until(wait_until).await;

        let mut ticks_data = Vec::new();
        let kline_duration_in_secs = self.kline_duration.num_seconds();
        let current_limit =
            (end_timestamp_ms - start_timestamp_ms) / (kline_duration_in_secs * 1000);
        for symbol in &self.symbols.get_unique_symbols() {
            let symbol_kline_data = self
                .fetch_tick_data(
                    symbol.name,
                    start_timestamp_ms,
                    end_timestamp_ms,
                    current_limit,
                )
                .await
                .expect("fetch data to work");
            ticks_data.extend(symbol_kline_data);
        }

        let kline_data_df = map_ticks_data_to_df(&ticks_data)?;
        let kline_data_df = coerce_df_to_schema(kline_data_df, trading_data_schema)?;

        Ok(kline_data_df)
    }

    async fn handle_initial_klines_fetch(
        &self,
        benchmark_start: NaiveDateTime,
        benchmark_end: NaiveDateTime,
        trading_data_schema: &Schema,
    ) -> Result<(), GlowError> {
        let initial_kline_data_df = self
            .load_or_fetch_kline_data(
                trading_data_schema,
                benchmark_start,
                benchmark_end,
                self.kline_duration,
            )
            .await?;

        let current_datetime = current_datetime();
        let is_last_kline_available = current_datetime > benchmark_end;

        if is_last_kline_available {
            let initial_data = TradingDataUpdate::Initial(initial_kline_data_df);
            self.klines_data_update_emitter.next(initial_data);
            return Ok(());
        }

        let current_timestamp = current_timestamp();
        let seconds_until_pending_kline_available = benchmark_end.timestamp() - current_timestamp;
        let duration_until_pending_kline_available =
            StdDuration::from_secs(seconds_until_pending_kline_available as u64);
        let pending_kline_available_at = Instant::now() + duration_until_pending_kline_available;

        let remaining_seconds_from_current_ts = current_timestamp % 60;
        let start_ms = (current_timestamp - remaining_seconds_from_current_ts) * 1000;
        let end_ms = benchmark_end.timestamp_millis();

        let pending_kline_df = self
            .fetch_data_after_waiting(
                pending_kline_available_at,
                start_ms,
                end_ms,
                trading_data_schema,
            )
            .await?;

        let initial_kline_data_df = initial_kline_data_df.vstack(&pending_kline_df)?;
        let initial_data = TradingDataUpdate::Initial(initial_kline_data_df);
        self.klines_data_update_emitter.next(initial_data);
        Ok(())
    }
}

impl DataProviderExchange for BinanceDataProvider {
    #[inline]
    fn get_kline_data_emitter(&self) -> &BehaviorSubject<TradingDataUpdate> {
        &self.klines_data_update_emitter
    }

    async fn handle_committed_ticks_data(
        &self,
        discard_ticks_before: NaiveDateTime,
        trading_data_schema: &Schema,
    ) -> Result<(), GlowError> {
        let mut ticks_to_commit_subscription = self.ticks_to_commit.subscribe();
        let discard_ticks_before = discard_ticks_before - Duration::nanoseconds(1);
        let trading_data_schema = trading_data_schema.clone();
        let kline_duration = self.kline_duration.clone();
        let unique_symbols = self.symbols.get_unique_symbols().clone();
        let klines_data_update_emitter = self.klines_data_update_emitter.clone();

        loop {
            let committed_ticks = ticks_to_commit_subscription.next().await;
            if committed_ticks.is_none() {
                continue;
            }
            let mut committed_ticks = committed_ticks.unwrap();
            if committed_ticks.len() <= 0
                || committed_ticks
                    .iter()
                    .filter(|tick| tick.start_time > discard_ticks_before)
                    .collect::<Vec<_>>()
                    .len()
                    <= 0
            {
                continue;
            }

            committed_ticks.sort_by(|a, b| a.start_time.cmp(&b.start_time));

            let committed_kline_df = map_ticks_data_to_df(&committed_ticks)?;
            let committed_kline_lf =
                coerce_df_to_schema(committed_kline_df, &trading_data_schema)?.lazy();

            let committed_kline_lf = downsample_tick_lf_to_kline_duration(
                &unique_symbols,
                kline_duration,
                committed_kline_lf,
                ClosedWindow::Left,
                None,
            )?;

            let committed_kline_df = committed_kline_lf.collect()?;

            let market_data = TradingDataUpdate::Market(committed_kline_df);
            klines_data_update_emitter.next(market_data);
        }
    }

    fn handle_ws_error(&self, trading_data_schema: &Schema) -> Option<NaiveDateTime> {
        let schema = trading_data_schema.clone();
        let last_error_ts: Option<i64>;
        {
            let last_error_guard = self
                .last_ws_error_ts
                .lock()
                .expect("handle_ws_error -> last_error_guard unwrap");
            last_error_ts = last_error_guard.clone();
        }
        if last_error_ts.is_none() {
            return None;
        }
        let now = Instant::now();
        let now_ts = current_timestamp_ms();

        let start_ms = timestamp_minute_start(true, last_error_ts);
        let end_ms = timestamp_minute_end(true, Some(now_ts));
        let result = NaiveDateTime::from_timestamp_millis(end_ms).unwrap();
        let data_provider = self.clone();
        let _ = spawn(async move {
            let duration_until_available = StdDuration::from_millis((now_ts - end_ms) as u64);

            let wait_until = now + duration_until_available + data_provider.fetch_leeway;
            let pending_kline_df = data_provider
                .fetch_data_after_waiting(wait_until, start_ms, end_ms, &schema)
                .await
                .expect("pending kline df");
            let market_data = TradingDataUpdate::Market(pending_kline_df);
            data_provider.klines_data_update_emitter.next(market_data);

            {
                let mut last_error_guard = data_provider.last_ws_error_ts.lock().unwrap();
                *last_error_guard = None;
            }
        });
        Some(result)
    }

    async fn init(
        &mut self,
        benchmark_start: Option<NaiveDateTime>,
        benchmark_end: Option<NaiveDateTime>,
        run_benchmark_only: bool,
        trading_data_schema: Schema,
    ) -> Result<(), GlowError> {
        let (benchmark_start, benchmark_end) = adjust_benchmark_datetimes(
            benchmark_start,
            benchmark_end,
            self.kline_duration,
            Some(1),
            self.minimum_klines_for_benchmarking as i32,
        )?;

        let _ = self
            .handle_initial_klines_fetch(benchmark_start, benchmark_end, &trading_data_schema)
            .await?;

        if run_benchmark_only {
            return Ok(());
        }

        println!(
            "{} | 💹 Initializing DataFeed -> trades might be open after {}",
            current_datetime(),
            benchmark_end
        );

        let binance_ws_base_url = env_var("BINANCE_WS_BASE_URL")?;
        let url = Url::parse(&format!("{}/ws/bookTicker", binance_ws_base_url))?; // ws url

        loop {
            match connect_async(url.clone()).await {
                Ok((wss, resp)) => {
                    eprintln!(
                        "Data provider connection stablished. \n Response: {:?}",
                        resp
                    );
                    let discard_ticks_before = self
                        .handle_ws_error(&trading_data_schema)
                        .unwrap_or(benchmark_end);
                    // TODO: verify if this works
                    match (
                        self.handle_committed_ticks_data(
                            discard_ticks_before,
                            &trading_data_schema,
                        )
                        .await,
                        self.listen_ticks(wss, discard_ticks_before).await,
                    ) {
                        (_, Err(error)) => {
                            set_ws_error_ts(self.last_ws_error_ts.clone(), error);
                            sleep(StdDuration::from_secs(WS_RECONNECT_INTERVAL_IN_SECS)).await;
                        }
                        (Err(error), _) => {
                            set_ws_error_ts(self.last_ws_error_ts.clone(), error);
                            sleep(StdDuration::from_secs(WS_RECONNECT_INTERVAL_IN_SECS)).await;
                        }
                        _ => {}
                    }
                }
                Err(error) => {
                    set_ws_error_ts(self.last_ws_error_ts.clone(), error.into());
                    sleep(StdDuration::from_secs(WS_RECONNECT_INTERVAL_IN_SECS)).await;
                }
            }
        }
    }

    async fn listen_ticks(
        &mut self,
        mut wss: WebSocketStream<MaybeTlsStream<TcpStream>>,
        discard_ticks_before: NaiveDateTime,
    ) -> Result<(), GlowError> {
        self.subscribe_to_tick_stream(&mut wss).await?;

        let mut current_staged_kline_minute = discard_ticks_before.time().minute();

        let unique_symbols_len = self.symbols.get_unique_symbols().len();
        loop {
            let message = wss.try_next().await;
            if let Err(error) = message {
                let mut last_error_guard = self
                    .last_ws_error_ts
                    .lock()
                    .expect("handle_websocket -> last_error_guard unwrap");
                let error_timestamp = current_timestamp();
                *last_error_guard = Some(error_timestamp);
                eprintln!("WebSocket message error: {:?}", error);
                return Err(GlowError::from(error));
            }

            let message = message.unwrap();
            if message.is_none() {
                continue;
            }
            let message = message.unwrap();
            match message {
                Message::Text(json) => {
                    let incoming_msg = from_str::<IncomingWsMessage>(&json).unwrap_or_default();
                    match incoming_msg {
                        IncomingWsMessage::Tick(tick) => {
                            let tick_data = from_tick_to_tick_data(tick, &self.symbols.get_tuple());

                            let tick_time = tick_data.start_time.time();
                            let tick_minute = tick_time.minute();
                            let tick_second = tick_time.second();
                            // we assume that if the received tick minute is the same as the current staged kline
                            // the tick still belongs to the kline
                            if tick_minute == current_staged_kline_minute {
                                self.staged_ticks
                                    .entry(tick_second)
                                    .or_insert(Vec::new())
                                    .push(tick_data.clone());
                            } else {
                                // otherwise, all ticks regarding the staged kline were already provided
                                // and the ticks must be committed as kline data

                                // commit ticks to kline data
                                self.ticks_to_commit.next(
                                    self.staged_ticks
                                        .values()
                                        .cloned()
                                        .into_iter()
                                        .flat_map(|vec| vec.into_iter())
                                        .collect(),
                                );

                                // clear staged ticks
                                self.staged_ticks.clear();

                                // insert the new tick data at respective map second
                                self.staged_ticks
                                    .insert(tick_second, vec![tick_data.clone()]);
                                // and update current committed kline minute
                                current_staged_kline_minute = tick_minute;
                            }

                            let second_staged_ticks = self.staged_ticks.get(&tick_second).unwrap();
                            if second_staged_ticks.len() == unique_symbols_len {
                                print!("{}", LogKlines(second_staged_ticks.to_vec()));
                            }
                        }
                        fallback => {
                            println!(
                                "fallback incoming msg from binance data provider {:?}",
                                fallback
                            );
                        }
                    }
                }
                Message::Ping(_) => wss.send(Message::Pong(vec![])).await?,
                fallback => {
                    println!("fallback msg from binance data provider {:?}", fallback);
                }
            }
        }
    }

    async fn subscribe_to_tick_stream(
        &mut self,
        wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<(), GlowError> {
        let ticker_params: Vec<String> = self
            .symbols
            .get_unique_symbols()
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
}

fn adjust_benchmark_datetimes(
    benchmark_start: Option<NaiveDateTime>,
    benchmark_end: Option<NaiveDateTime>,
    kline_duration: Duration,
    minimum_days_for_analysis: Option<i64>,
    minimum_klines_for_benchmarking: i32,
) -> Result<(NaiveDateTime, NaiveDateTime), GlowError> {
    if let (Some(benchmark_start), Some(benchmark_end)) = (benchmark_start, benchmark_end) {
        assert_or_error!(benchmark_end > benchmark_start);
    }

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

    let benchmark_start = benchmark_start.unwrap_or_else(|| {
        benchmark_end - (Duration::days(minimum_days_for_analysis.unwrap_or(1)))
    });

    let benchmark_start = benchmark_start - (kline_duration * minimum_klines_for_benchmarking);

    assert_or_error!(benchmark_end > benchmark_start);

    Ok((benchmark_start, benchmark_end))
}

fn set_ws_error_ts(last_ws_error_ts: Arc<Mutex<Option<i64>>>, error: GlowError) {
    let mut last_error_guard = last_ws_error_ts
        .lock()
        .expect("init -> last_error_guard unwrap");
    let error_timestamp = current_timestamp();
    *last_error_guard = Some(error_timestamp);
    eprintln!(
        "Market Websocket connection error: {:?}. Retrying...",
        error
    );
}
