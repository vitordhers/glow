use super::Symbol;
use chrono::{DateTime, Duration, NaiveTime, Utc};

#[derive(Clone, Debug)]
pub struct Contract {
    pub available_since: DateTime<Utc>,
    _funding_interval: Duration,
    pub funding_rate: f64,
    pub max_leverage: f64,
    pub maximum_order_sizes: (f64, f64), // (market, limit) in units
    pub minimum_order_size: f64,         // in units
    pub next_funding: Option<NaiveTime>,
    pub symbol: &'static Symbol,
    pub tick_size: f64, // in USDT
}

impl Contract {
    pub fn new(
        available_since: DateTime<Utc>,
        funding_interval: Duration,
        funding_rate: f64,
        max_leverage: f64,
        maximum_order_sizes: (f64, f64),
        minimum_order_size: f64,
        next_funding: Option<NaiveTime>,
        symbol: &'static Symbol,
        tick_size: f64,
    ) -> Self {
        Contract {
            available_since,
            _funding_interval: funding_interval,
            funding_rate,
            max_leverage,
            maximum_order_sizes,
            minimum_order_size,
            next_funding,
            symbol,
            tick_size,
        }
    }

    pub fn update_next_funding(&mut self, _time: NaiveTime) {
        todo!("implement this");
        // self.next_funding = Some(date_time);
    }

    pub fn tick_data_decimal_places(&self) -> usize {
        self.tick_size.to_string().split('.').last().unwrap().len()
    }
}
