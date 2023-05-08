use chrono::{Duration, NaiveDateTime};
use futures_util::{SinkExt, StreamExt};
use log::*;
use polars::prelude::*;
use reqwest::Client;
use std::env::{self};
use std::time::Duration as TimeDuration;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

use super::super::enums::log_level::LogLevel;
use super::super::enums::request_method::RequestMethod;
use super::super::errors::Error;
use super::super::functions::*;
use super::{strategy::Strategy, *};

pub struct MarketDataFeed<'a> {
    symbols: &'a [String; 2],
    bar_length: Duration,
    last_bar: NaiveDateTime,
    tick_data: LazyFrame,
    tick_data_schema: Schema,
    log_level: LogLevel,
    http: Client,
    fetch_offset: TimeDuration,
    socket: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    strategy: &'a mut Strategy,
}

impl MarketDataFeed<'_> {
    pub fn new<'a>(
        symbols: &'a [String; 2],
        bar_length: i64,
        strategy: &'a mut Strategy,
        log_level: LogLevel,
    ) -> MarketDataFeed<'a> {
        env::set_var("POLARS_FMT_MAX_COLS", "20"); // maximum number of columns shown when formatting DataFrames.
        env::set_var("POLARS_FMT_MAX_ROWS", "60");

        let mut schema_fields: Vec<Field> = vec![];

        schema_fields.push(Field::new(
            "start_time",
            DataType::Datetime(TimeUnit::Milliseconds, None),
        ));

        for symbol in symbols {
            let (open_col, high_col, close_col, low_col) = get_symbol_ohlc_cols(symbol);

            schema_fields.push(Field::new(open_col.as_str(), DataType::Float64));
            schema_fields.push(Field::new(high_col.as_str(), DataType::Float64));
            schema_fields.push(Field::new(close_col.as_str(), DataType::Float64));
            schema_fields.push(Field::new(low_col.as_str(), DataType::Float64));
        }
        let tick_data_schema = Schema::from(schema_fields.into_iter());
        let tick_data = DataFrame::from(&tick_data_schema).lazy();

        let bar_length = Duration::seconds(bar_length);
        let current_datetime: NaiveDateTime = current_datetime();
        let seconds_to_next_full_minute = 60 - current_datetime.timestamp() % 60;
        let last_bar = current_datetime + Duration::seconds(seconds_to_next_full_minute);
        println!(
            "{} | Initializing MarketDataFeed -> {} seconds to next full minute, initial last bar {}",
            current_datetime, seconds_to_next_full_minute, last_bar
        );

        MarketDataFeed {
            symbols,
            bar_length,
            last_bar,
            log_level,
            socket: None,
            tick_data,
            http: Client::new(),
            fetch_offset: TimeDuration::from_secs(seconds_to_next_full_minute as u64),
            tick_data_schema,
            strategy,
        }
    }

    /// A single connection to stream.binance.com is only valid for 24 hours; expect to be disconnected at the 24 hour mark
    pub async fn connect(&mut self) -> Result<(), Error> {
        let binance_ws_base_url = env::var("BINANCE_WS_BASE_URL")?;

        if self.log_level > LogLevel::Trades {
            info!("Connecting to Binance WS {}", binance_ws_base_url);
        }

        let url = Url::parse(&format!("{}/ws/bookTicker", binance_ws_base_url))?; // ws

        let (ws_stream, _) = connect_async(url).await?;
        self.socket = Some(ws_stream);

        Ok(())
    }

    pub async fn subscribe(&mut self) -> Result<(), Error> {
        let mut ticker_params: Vec<String> = vec![];
        if self.symbols[0] != self.symbols[1] {
            for symbol in self.symbols {
                let kline_param = format!("{}@kline_1m", symbol.to_lowercase());
                ticker_params.push(kline_param);
            }
        } else {
            ticker_params.push(format!("{}@kline_1m", self.symbols[0].to_lowercase()));
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
        self.socket
            .as_mut()
            .unwrap()
            .send(subscription_message)
            .await?;

        Ok(())
    }

    pub async fn listen_events(&mut self) -> Result<(), Error> {
        let socket: &mut WebSocketStream<MaybeTlsStream<TcpStream>> = self.socket.as_mut().unwrap();
        println!("Listen events start {:?}", current_datetime());
        let limit = 720;

        let mut tick_data_payload = get_historical_tick_data(
            &self.http,
            &self.symbols,
            &self.last_bar,
            limit,
            self.fetch_offset,
        )
        .await?;

        let new_tick_data_lf = consolidate_tick_data_into_lf(
            &self.symbols,
            &tick_data_payload,
            &self.tick_data_schema,
        )?;

        self.tick_data = resample_tick_data_to_min(
            &self.symbols,
            &self.bar_length,
            &new_tick_data_lf,
            ClosedWindow::Left,
        )?;

        // let mut strategy = self.strategy.borrow_mut();
        self.strategy
            .calculate_initial_positions(&mut self.tick_data, &self.symbols)
            .unwrap();

        println!("Listen events fetch finished {:?}", current_datetime());

        // tick_data_payload = clear_tick_data_to_last_bar(tick_data_payload, &previous_bar);
        tick_data_payload.clear();

        loop {
            tokio::select! {
                ws_message = socket.next() => {
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
                        let new_tick_data_lf = consolidate_tick_data_into_lf(&self.symbols, &tick_data_payload, &self.tick_data_schema)?;
                        let new_tick_data_lf = resample_tick_data_secs_to_min(
                            &self.symbols,
                            &new_tick_data_lf,
                            &self.tick_data_schema,
                            &self.last_bar,
                            &self.bar_length,
                        )?;

                        let filter_datetime = self.last_bar - Duration::days(1);
                        self.tick_data = concat_and_clean_lazyframes([self.tick_data.clone(), new_tick_data_lf], filter_datetime)?;
                        self.last_bar = last_tick_datetime;
                        tick_data_payload = clear_tick_data_to_last_bar(tick_data_payload, &self.last_bar);
                    }
                },
            }
        }
        // Ok(())
    }
}
