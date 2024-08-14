use chrono::{Duration, NaiveDateTime, NaiveTime};

#[derive(Clone, Debug)]
pub struct Contract {
    pub available_since: NaiveDateTime,
    funding_interval: Duration,
    pub funding_rate: f64,
    pub max_leverage: f64,
    pub maximum_order_size: f64, // in contract Symbol
    pub minimum_order_size: f64, // in contract Symbol
    pub next_funding: Option<NaiveTime>,
    pub symbol: &'static str,
    pub tick_size: f64, // in USDT
}

impl Contract {
    pub fn new(
        available_since: NaiveDateTime,
        funding_interval: Duration,
        funding_rate: f64,
        max_leverage: f64,
        maximum_order_size: f64,
        minimum_order_size: f64,
        next_funding: Option<NaiveTime>,
        symbol: &'static str,
        tick_size: f64,
    ) -> Self {
        Contract {
            available_since,
            funding_interval,
            funding_rate,
            max_leverage,
            maximum_order_size,
            minimum_order_size,
            next_funding,
            symbol,
            tick_size,
        }
    }

    pub fn update_next_funding(&mut self, time: NaiveTime) {
        todo!("implement this");
        // self.next_funding = Some(date_time);
    }

    pub fn tick_data_decimal_places(&self) -> usize {
        self.tick_size.to_string().split('.').last().unwrap().len()
    }
}
