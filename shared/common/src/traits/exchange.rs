use crate::{
    enums::{
        balance::Balance,
        modifiers::{leverage::Leverage, price_level::PriceLevel},
        order_status::OrderStatus,
        order_type::OrderType,
        side::Side,
        symbol_id::SymbolId,
        trade_status::TradeStatus,
    },
    structs::{Contract, Execution, Order, Symbol, Trade, TradingSettings},
};
use chrono::NaiveDateTime;
use glow_error::GlowError;
use polars::prelude::Schema;
use reqwest::Client;
use std::{collections::HashMap, future::Future};
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

pub trait TraderHelper {
    fn calculate_order_fees(
        &self,
        order_type: OrderType,
        side: Side,
        units: f64,
        price: f64,
    ) -> ((f64, f64), f64, bool);

    fn calculate_order_stop_loss_price(&self, side: Side, price: f64) -> Option<f64>;
    fn calculate_order_take_profit_price(&self, side: Side, price: f64) -> Option<f64>;

    fn calculate_open_order_units_and_balance_remainder(
        &self,
        side: Side,
        order_cost: f64,
        price: f64,
    ) -> Result<(f64, f64), GlowError>;

    fn get_contracts(&self) -> &HashMap<SymbolId, Contract>;
    fn get_anchor_contract(&self) -> &Contract {
        let contracts = self.get_contracts();
        contracts
            .get(&self.get_anchor_symbol().id)
            .expect("Exchange to have anchor contract")
    }
    fn get_anchor_symbol(&self) -> &'static Symbol {
        let trading_settings = self.get_trading_settings();
        trading_settings.get_anchor_symbol()
    }
    fn get_traded_contract(&self) -> &Contract {
        let contracts = self.get_contracts();
        contracts
            .get(&self.get_traded_symbol().id)
            .expect("Exchange to have anchor contract")
    }
    fn get_traded_symbol(&self) -> &'static Symbol {
        let trading_settings = self.get_trading_settings();
        trading_settings.get_traded_symbol()
    }
    fn get_unique_symbols(&self) -> Vec<&'static Symbol> {
        let trading_settings = self.get_trading_settings();
        trading_settings.get_unique_symbols()
    }

    fn get_leverage_factor(&self) -> f64 {
        let settings = self.get_trading_settings();
        settings.leverage.get_factor()
    }

    fn get_trading_settings(&self) -> &TradingSettings;
    fn get_taker_fee(&self) -> f64;
    fn get_maker_fee(&self) -> f64;

    fn get_order_fee_rate(&self, order_type: OrderType) -> (f64, bool);
}

// TODO: change this name
pub trait TraderExchange: TraderHelper {
    /// This function creates a new order from a given amount of USDT
    ///
    /// # Arguments
    ///
    /// * `side`: Position side, being Buy or Sell.
    /// * `amount`: Amount in USDT for opening the position, paying the Opening Fee, Closing Fee provision and Initial Margin.
    /// * `price_opt`: (Optional) Price for opening the position. Required for OrderType::Limit.
    ///
    fn new_open_order(&self, side: Side, order_cost: f64, price: f64) -> Result<Order, GlowError>;

    fn get_ws_url(&self) -> Result<Url, GlowError>;
    fn process_ws_message(&self, json: &String) -> Result<(), GlowError>;
    fn get_http_client(&self) -> &Client;
    fn get_ws_ping_interval(&self) -> u64;
    fn get_ws_ping_message(&self) -> Result<Message, GlowError>;

    // async methods
    // ws
    fn auth_ws(
        &self,
        wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> impl Future<Output = Result<(), GlowError>> + Send;
    fn subscribe_ws(
        &self,
        wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> impl Future<Output = Result<(), GlowError>> + Send;
    // http
    fn fetch_order_executions(
        &self,
        order_uuid: String,
        start_timestamp: i64,
        end_timestamp: i64,
    ) -> impl Future<Output = Result<Vec<Execution>, GlowError>> + Send;
    /// Query unfilled or partially filled orders in real-time. To query older order records, please use fetch_history_order
    fn fetch_current_order(
        &self,
        order_id: String,
        fetch_executions: bool,
    ) -> impl Future<Output = Result<Order, GlowError>> + Send;
    /// Query order history. If you want to get real-time order information, use fetch_current_order
    fn fetch_history_order(
        &self,
        id: Option<String>,
        side: Option<Side>,
        fetch_executions: bool,
    ) -> impl Future<Output = Result<Order, GlowError>> + Send;
    fn fetch_current_trade_position(
        &self,
    ) -> impl Future<Output = Result<Option<Trade>, GlowError>> + Send;
    fn fetch_trade_state(
        &self,
        trade_id: String,
        last_status: TradeStatus,
    ) -> impl Future<Output = Result<Trade, GlowError>> + Send;
    fn fetch_current_usdt_balance(&self)
        -> impl Future<Output = Result<Balance, GlowError>> + Send;
    fn open_order(
        &self,
        side: Side,
        amount: f64,
        expected_price: f64,
    ) -> impl Future<Output = Result<Order, GlowError>> + Send;
    fn amend_order(
        &self,
        order_id: String,
        updated_units: Option<f64>,
        updated_price: Option<f64>,
        updated_stop_loss_price: Option<f64>,
        updated_take_profit_price: Option<f64>,
    ) -> impl Future<Output = Result<bool, GlowError>> + Send;
    fn try_close_position(
        &self,
        trade: &Trade,
        est_price: f64,
    ) -> impl Future<Output = Result<Order, GlowError>> + Send;
    /// this function is meant to be run by trades with status TradeStatus::PartiallyOpen and TradeStatus::CloseOrderStandBy
    fn cancel_order(
        &self,
        order_id: String,
    ) -> impl Future<Output = Result<bool, GlowError>> + Send;
    fn set_leverage(
        &self,
        leverage: Leverage,
    ) -> impl Future<Output = Result<bool, GlowError>> + Send;

    // ws
    fn update_position_data_on_faulty_exchange_ws(
        &self,
    ) -> impl std::future::Future<Output = Result<(), GlowError>> + Send;
    fn init(&mut self) -> impl std::future::Future<Output = Result<(), GlowError>> + Send;
    fn listen_messages(
        &mut self,
        wss: WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> impl std::future::Future<Output = Result<(), GlowError>> + Send;
}

pub trait BenchmarkExchange: TraderHelper {
    fn new_benchmark_open_order(
        &self,
        timestamp: i64,
        side: Side,
        order_cost: f64,
        price: f64,
    ) -> Result<Order, GlowError>;
    fn new_benchmark_close_order(
        &self,
        timestamp: i64,
        trade_id: &String,
        close_price: f64,
        open_order: Order,
        final_status: OrderStatus,
    ) -> Result<Order, GlowError>;

    fn check_price_level_modifiers(
        &self,
        trade: &Trade,
        current_timestamp: i64,
        close_price: f64,
        stop_loss: Option<&PriceLevel>,
        take_profit: Option<&PriceLevel>,
        trailing_stop_loss: Option<&PriceLevel>,
        current_peak_returns: f64,
    ) -> Result<Option<Trade>, GlowError>;
}

pub trait DataProviderExchange {
    fn subscribe_to_tick_stream(
        &mut self,
        wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> impl Future<Output = Result<(), GlowError>> + Send;

    fn listen_ticks(
        &mut self,
        wss: WebSocketStream<MaybeTlsStream<TcpStream>>,
        benchmark_end: NaiveDateTime,
    ) -> impl Future<Output = Result<(), GlowError>> + Send;

    fn init(
        &mut self,
        benchmark_start: Option<NaiveDateTime>,
        benchmark_end: Option<NaiveDateTime>,
        run_benchmark_only: bool,
        trading_data_schema: Schema,
    ) -> impl Future<Output = Result<(), GlowError>> + Send;

    // fn handle_ws_error(&self) -> impl Future<Output = Result<(), GlowError>> + Send;

    fn handle_committed_ticks_data(
        &self,
        benchmark_end: NaiveDateTime,
        trading_data_schema: &Schema,
    ) -> impl Future<Output = Result<(), GlowError>> + Send;
}
