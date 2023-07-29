use super::contract::Contract;
use super::strategy::LeveragedOrder;
use crate::trader::{errors::Error, Order};
use async_trait::async_trait;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

#[async_trait]
pub trait Exchange {
    fn clone_box(&self) -> Box<dyn Exchange + Send + Sync>;
    fn get_anchor_contract(&self) -> &Contract;
    fn get_traded_contract(&self) -> &Contract;
    fn get_current_symbols(&self) -> Vec<String>;
    fn get_taker_fee(&self) -> f64;
    fn get_maker_fee(&self) -> f64;
    async fn connect_to_ws(&self) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error>;
    async fn go_short(&self, order: LeveragedOrder) -> Result<(), Error>;
    async fn go_long(&self, order: LeveragedOrder) -> Result<(), Error>;
    async fn close_position(&self, dto: LeveragedOrder) -> Result<(), Error>;
    fn get_processer(&self) -> Box<dyn WsProcesser + Send + Sync>;
}

impl Clone for Box<dyn Exchange + Send + Sync> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

pub trait WsProcesser {
    fn process_ws_message(&self, json: String) -> ProcesserAction;
}

pub enum ProcesserAction {
    Nil,
    Auth { success: bool },
    UpdateOrder { order: Order },
    UpdateBalance { balance_available: f64 },
}
