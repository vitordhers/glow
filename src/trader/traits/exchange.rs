use async_trait::async_trait;
use reqwest::Client;
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::trader::{
    enums::{
        balance::Balance,
        modifiers::{leverage::Leverage, position_lock::PositionLock},
        order_status::OrderStatus,
        order_type::OrderType,
        side::Side,
        trade_status::TradeStatus,
    },
    errors::Error,
    models::{
        contract::Contract, execution::Execution, order::Order, trade::Trade,
        trading_settings::TradingSettings,
    },
};

use super::ws_processer::WsProcesser;

#[async_trait]
pub trait Exchange {
    fn clone_box(&self) -> Box<dyn Exchange + Send + Sync>;
    fn get_anchor_contract(&self) -> &Contract;
    fn get_traded_contract(&self) -> &Contract;
    fn get_trading_settings(&self) -> TradingSettings;
    fn get_current_symbols(&self) -> Vec<String>;
    fn get_taker_fee(&self) -> f64;
    fn get_maker_fee(&self) -> f64;
    fn calculate_open_order_units_and_balance_remainder(
        &self,
        side: Side,
        order_cost: f64,
        price: f64,
    ) -> Result<(f64, f64), Error>;

    fn get_order_fee_rate(&self, order_type: OrderType) -> (f64, bool);

    fn calculate_order_fees(
        &self,
        order_type: OrderType,
        side: Side,
        units: f64,
        price: f64,
    ) -> ((f64, f64), f64, bool);

    fn calculate_order_stop_loss_price(&self, side: Side, price: f64) -> Option<f64>;

    fn calculate_order_take_profit_price(&self, side: Side, price: f64) -> Option<f64>;
    /// This function creates a new order from a given amount of USDT
    ///
    /// # Arguments
    ///
    /// * `side`: Position side, being Buy or Sell.
    /// * `amount`: Amount in USDT for opening the position, paying the Opening Fee, Closing Fee provision and Initial Margin.
    /// * `price_opt`: (Optional) Price for opening the position. Required for OrderType::Limit.
    ///
    fn create_open_order(&self, side: Side, order_cost: f64, price: f64) -> Result<Order, Error>;

    fn create_benchmark_open_order(
        &self,
        timestamp: i64,
        side: Side,
        order_cost: f64,
        price: f64,
    ) -> Result<Order, Error>;

    fn create_benchmark_close_order(
        &self,
        timestamp: i64,
        trade_id: &String,
        close_price: f64,
        open_order: Order,
        final_status: OrderStatus,
    ) -> Result<Order, Error>;

    fn get_ws_url(&self) -> Result<Url, Error>;

    async fn auth_ws(
        &self,
        wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<(), Error>;

    async fn subscribe_ws(
        &self,
        wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<(), Error>;

    async fn fetch_order_executions(
        &self,
        order_uuid: String,
        start_timestamp: i64,
        end_timestamp: i64,
    ) -> Result<Vec<Execution>, Error>;

    /// Query order history. If you want to get real-time order information, use fetch_open_order
    async fn fetch_history_order(
        &self,
        id: Option<String>,
        side: Option<Side>,
        fetch_executions: bool,
    ) -> Result<Order, Error>;

    /// Query unfilled or partially filled orders in real-time. To query older order records, please use fetch_closed_order
    async fn fetch_opened_order(
        &self,
        order_id: String,
        fetch_executions: bool,
    ) -> Result<Order, Error>;

    async fn fetch_current_position_trade(&self) -> Result<Option<Trade>, Error>;

    async fn fetch_trade_state(
        &self,
        trade_id: String,
        last_status: TradeStatus,
    ) -> Result<Trade, Error>;

    async fn fetch_current_usdt_balance(&self) -> Result<Balance, Error>;

    async fn open_order(
        &self,
        side: Side,
        amount: f64,
        expected_price: f64,
    ) -> Result<Order, Error>;

    async fn amend_order(
        &self,
        order_id: String,
        updated_units: Option<f64>,
        updated_price: Option<f64>,
        updated_stop_loss_price: Option<f64>,
        updated_take_profit_price: Option<f64>,
    ) -> Result<bool, Error>;

    async fn try_close_position(&self, trade: &Trade, est_price: f64) -> Result<Order, Error>;

    /// this function is meant to be run by trades with status TradeStatus::PartiallyOpen and TradeStatus::CloseOrderStandBy
    async fn cancel_order(&self, order_id: String) -> Result<bool, Error>;

    async fn set_leverage(&self, leverage: Leverage) -> Result<bool, Error>;

    fn get_processer(&self) -> Box<dyn WsProcesser + Send + Sync>;

    fn get_http_client(&self) -> &Client;

    fn get_ws_ping_interval(&self) -> u64;

    fn get_ws_ping_message(&self) -> Option<Message>;
    // fn get_signature(&self, method: HttpMethod, payload: Box<dyn Serialize>) -> String;
}

impl Clone for Box<dyn Exchange + Send + Sync> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
