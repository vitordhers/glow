use chrono::{DateTime, Duration, Utc};

#[derive(Clone, Debug)]
pub struct Contract {
    pub symbol: String,
    pub funding_rate: f64,
    funding_interval: Duration,
    pub next_funding: Option<DateTime<Utc>>,
    pub tick_size: f64,
    pub maximum_order_size: f64,
    pub minimum_order_size: f64,
    pub max_leverage: f64,
}

impl Contract {
    pub fn new(
        symbol: String,
        funding_rate: f64,
        funding_interval: Duration,
        next_funding: Option<DateTime<Utc>>,
        tick_size: f64,
        maximum_order_size: f64,
        minimum_order_size: f64,
        max_leverage: f64,
    ) -> Self {
        Contract {
            symbol,
            funding_rate,
            funding_interval,
            next_funding,
            tick_size,
            maximum_order_size,
            minimum_order_size,
            max_leverage,
        }
    }

    pub fn update_next_funding(&mut self, date_time: DateTime<Utc>) {
        todo!("implement this");
        // self.next_funding = Some(date_time);
    }

    pub fn tick_data_decimal_places(&self) -> usize {
        self.tick_size.to_string().split('.').last().unwrap().len()
    }
}
