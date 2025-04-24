pub mod http {
    pub mod response {
        use serde::Deserialize;
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
    }
}

pub mod ws {
    pub mod outgoing {
        use crate::binance::enums::OutgoingWsMessageMethod;
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Serialize, Deserialize)]
        pub struct WsOutgoingMessage {
            pub method: OutgoingWsMessageMethod,
            pub params: Vec<String>,
            pub id: u8,
        }
    }

    pub mod incoming {
        use chrono::{DateTime, Utc};
        // use common::structs::TickData;
        use serde::Deserialize;

        use crate::shared::deserializers::{parse_f64, parse_u64_as_datetime};

        #[derive(Debug, Deserialize)]
        pub struct EmptyMessage {
            pub result: Option<String>,
            pub id: i64,
        }

        #[derive(Debug, Deserialize)]
        pub struct TickMessage {
            #[serde(rename = "e")]
            pub event_type: String, // Event type
            #[serde(rename = "E")]
            pub event_time: i64, // Event time
            #[serde(rename = "s")]
            pub symbol: String, // Symbol
            #[serde(rename = "k")]
            pub data: TickMessageData,
        }

        #[allow(dead_code)]
        #[derive(Debug, Deserialize)]
        pub struct TickMessageData {
            #[serde(rename = "t", deserialize_with = "parse_u64_as_datetime")]
            pub start_time: DateTime<Utc>, // Kline start time
            #[serde(rename = "T", deserialize_with = "parse_u64_as_datetime")]
            pub close_time: DateTime<Utc>, // Kline close time
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

        // impl From<BinanceWsTickResponse> for TickData {
        //     fn from(tick: BinanceWsTickResponse) -> Self {
        //         TickData {
        //             symbol: tick.symbol.as_str(),
        //             start_time: tick.data.start_time,
        //             open: tick.data.open,
        //             high: tick.data.high,
        //             close: tick.data.close,
        //             low: tick.data.low,
        //         }
        //     }
        // }
    }
}
