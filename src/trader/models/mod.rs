use chrono::NaiveDateTime;
use serde::{Serialize, Deserialize};


pub mod strategy;
pub mod performance;
pub mod indicator;
pub mod market_data;

use super::enums::request_method::RequestMethod;

#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize)]
pub struct HttpKlineResponse {
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

#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    pub method: RequestMethod,
    pub params: Vec<String>,
    pub id: u8,
}

#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize)]
pub struct WsKlineResponseData {
    pub t: u64,    // Kline start time
    pub T: u64,    // Kline close time
    pub s: String, // Symbol
    i: String, // Interval
    f: i64,    // First trade ID
    L: i64,    // Last trade ID
    pub o: String, // Open price
    pub c: String, // Close price
    pub h: String, // High price
    pub l: String, // Low price
    v: String, // Base asset volume
    n: i64,    // Number of trades
    x: bool,   // Is this kline closed?
    q: String, // Quote asset volume
    V: String, // Taker buy base asset volume
    Q: String, // Taker buy quote asset volume
    B: String, // Ignore
}
#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize)]
pub struct WsKlineResponse {
    pub e: String,
    pub E: i64,
    pub s: String,
    pub k: WsKlineResponseData,
}

#[derive(Debug, Clone)]
pub struct TickData {
    pub symbol: String,
    pub date: NaiveDateTime,
    pub open: f64,
    pub high: f64,
    pub close: f64,
    pub low: f64,
}
