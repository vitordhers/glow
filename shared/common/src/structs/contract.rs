use super::Symbol;
use chrono::{DateTime, Utc};

#[derive(Clone, Debug)]
pub struct Contract {
    pub available_since: DateTime<Utc>,
    pub max_leverage: f64,
    pub maximum_order_sizes: (f64, f64), // (market, limit) in units
    pub minimum_order_size: f64,         // in units
    pub symbol: &'static Symbol,
    pub tick_size: f64, // in USDT
}

impl Contract {
    pub fn new(
        available_since: DateTime<Utc>,
        max_leverage: f64,
        maximum_order_sizes: (f64, f64),
        minimum_order_size: f64,
        symbol: &'static Symbol,
        tick_size: f64,
    ) -> Self {
        Contract {
            available_since,
            max_leverage,
            maximum_order_sizes,
            minimum_order_size,
            symbol,
            tick_size,
        }
    }

    pub fn tick_data_decimal_places(&self) -> usize {
        self.tick_size.to_string().split('.').last().unwrap().len()
    }
}
