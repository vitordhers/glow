use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::trader::{
    exchanges::shared::deserializers::{parse_f64, parse_u64_as_datetime},
    models::tick_data::TickData,
};

use super::enums::BinanceRequestMethod;

#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceWsRequest {
    pub method: BinanceRequestMethod,
    pub params: Vec<String>,
    pub id: u8,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct BinanceEmptyResponse {
    result: Option<String>,
    id: i64,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct BinanceWsKlineResponse {
    #[serde(rename = "e")]
    pub event_type: String, // Event type
    #[serde(rename = "E")]
    pub event_time: i64, // Event time
    #[serde(rename = "s")]
    pub symbol: String, // Symbol
    #[serde(rename = "k")]
    pub data: BinanceWsKlineResponseData,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct BinanceWsKlineResponseData {
    #[serde(rename = "t", deserialize_with = "parse_u64_as_datetime")]
    pub start_time: NaiveDateTime, // Kline start time
    #[serde(rename = "T", deserialize_with = "parse_u64_as_datetime")]
    pub close_time: NaiveDateTime, // Kline close time
    pub s: String, // Symbol
    i: String,     // Interval
    f: i64,        // First trade ID
    #[serde(rename = "L")]
    last_trade_id: i64, // Last trade ID
    #[serde(rename = "o", deserialize_with = "parse_f64")]
    pub open: f64, // Open price
    #[serde(rename = "c", deserialize_with = "parse_f64")]
    pub close: f64, // Close price
    #[serde(rename = "h", deserialize_with = "parse_f64")]
    pub high: f64, // High price
    #[serde(rename = "l", deserialize_with = "parse_f64")]
    pub low: f64, // Low price
    v: String,     // Base asset volume
    n: i64,        // Number of trades
    x: bool,       // Is this kline closed?
    q: String,     // Quote asset volume
    #[serde(rename = "V")]
    buy_volume: String, // Taker buy base asset volume
    #[serde(rename = "Q")]
    taker_buy_quote_asset_volume: String, // Taker buy quote asset volume
    #[serde(rename = "B")]
    ignore: String, // Ignore
}

impl From<BinanceWsKlineResponse> for TickData {
    fn from(ws_kline: BinanceWsKlineResponse) -> Self {
        TickData {
            symbol: ws_kline.symbol,
            start_time: ws_kline.data.start_time,
            open: ws_kline.data.open,
            high: ws_kline.data.high,
            close: ws_kline.data.close,
            low: ws_kline.data.low,
        }
    }
}

#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize)]
pub struct BinanceHttpKlineResponse {
    pub timestamp: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    volume: String,
    close_time: u64,
    quote_asset_volume: String,
    number_of_trades: u32,
    taker_buy_base_asset_volume: String,
    taker_buy_quote_asset_volume: String,
    unused_field: String,
}
