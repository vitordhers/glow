use async_trait::async_trait;

use crate::trader::errors::Error;

#[derive(Debug, Clone)]
pub struct Exchange {
    pub name: String,
    pub maker_fee_rate: f64,
    pub taker_fee_rate: f64,
    http_url: String,
    ws_url: String,
}

impl Exchange {
    pub fn new(
        name: String,
        maker_fee_rate: f64,
        taker_fee_rate: f64,
        http_url: String,
        ws_url: String,
    ) -> Self {
        Self {
            name,
            maker_fee_rate,
            taker_fee_rate,
            http_url,
            ws_url,
        }
    }
    async fn go_short(&self, dto: TakePositionDTO) -> Result<(), Error> {
        todo!("implement this");
        Ok(())
    }
    async fn go_long(&self, dto: TakePositionDTO) -> Result<(), Error> {
        todo!("implement this");
        Ok(())
    }
    async fn close_position(&self, dto: TakePositionDTO) -> Result<(), Error> {
        todo!("implement this");
        Ok(())
    }
}

unsafe impl Send for Exchange {}
unsafe impl Sync for Exchange {}

pub struct TakePositionDTO {
    symbol: String,
    amount: Option<f64>,
    units: Option<f64>,
    position: i32,
}

impl TakePositionDTO {
    fn new(symbol: String, amount: Option<f64>, units: Option<f64>, position: i32) -> Self {
        Self {
            symbol,
            amount,
            units,
            position,
        }
    }
}
