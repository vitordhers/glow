use crate::{binance::structs::BinanceDataProvider, bybit::BybitTraderExchange};
use chrono::NaiveDateTime;
use common::{
    enums::{
        balance::Balance, modifiers::leverage::Leverage, order_action::OrderAction,
        order_status::OrderStatus, order_type::OrderType, side::Side, symbol_id::SymbolId,
        trade_status::TradeStatus, trading_data_update::TradingDataUpdate,
    },
    structs::{BehaviorSubject, Contract, Execution, Order, Trade, TradingSettings},
    traits::exchange::{BenchmarkExchange, DataProviderExchange, TraderExchange, TraderHelper},
};
use glow_error::GlowError;
use polars::prelude::Schema;
use reqwest::Client;
use std::collections::HashMap;
use strategy::Strategy;
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

#[derive(Clone, Copy, Debug, Default)]
pub enum DataProviderExchangeId {
    #[default]
    Binance,
}

#[derive(Clone)]
pub enum DataProviderExchangeWrapper {
    Binance(BinanceDataProvider),
}

impl DataProviderExchangeWrapper {
    pub fn new(
        selected_exchange: DataProviderExchangeId,
        strategy: &Strategy,
        trading_settings: &TradingSettings,
    ) -> Self {
        match selected_exchange {
            DataProviderExchangeId::Binance => {
                Self::Binance(BinanceDataProvider::new(trading_settings, strategy))
            }
        }
    }

    pub fn get_selection_list() -> Vec<String> {
        vec![String::from("Binance")]
    }

    pub fn patch_settings(&mut self, trading_settings: &TradingSettings) {
        match self {
            Self::Binance(ex) => ex.patch_settings(trading_settings),
        }
    }

    pub fn patch_strategy(&mut self, strategy: &Strategy) {
        match self {
            Self::Binance(ex) => ex.patch_strategy(strategy),
        }
    }
}

impl DataProviderExchange for DataProviderExchangeWrapper {
    fn get_kline_data_emitter(&self) -> &BehaviorSubject<TradingDataUpdate> {
        match self {
            Self::Binance(ex) => ex.get_kline_data_emitter(),
        }
    }

    async fn subscribe_to_tick_stream(
        &mut self,
        wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<(), GlowError> {
        match self {
            Self::Binance(ex) => ex.subscribe_to_tick_stream(wss).await,
        }
    }

    async fn init(
        &mut self,
        benchmark_start: Option<NaiveDateTime>,
        benchmark_end: Option<NaiveDateTime>,
        run_benchmark_only: bool,
        trading_data_schema: Schema,
    ) -> Result<(), GlowError> {
        match self {
            Self::Binance(ex) => {
                ex.init(
                    benchmark_start,
                    benchmark_end,
                    run_benchmark_only,
                    trading_data_schema,
                )
                .await
            }
        }
    }

    async fn listen_ticks(
        &mut self,
        wss: WebSocketStream<MaybeTlsStream<TcpStream>>,
        benchmark_end: NaiveDateTime,
    ) -> Result<(), GlowError> {
        match self {
            Self::Binance(ex) => ex.listen_ticks(wss, benchmark_end).await,
        }
    }

    async fn handle_committed_ticks_data(
        &self,
        benchmark_end: NaiveDateTime,
        trading_data_schema: &Schema,
    ) -> Result<(), GlowError> {
        match self {
            Self::Binance(ex) => {
                ex.handle_committed_ticks_data(benchmark_end, trading_data_schema)
                    .await
            }
        }
    }

    fn handle_ws_error(&self, trading_data_schema: &Schema) -> Option<NaiveDateTime> {
        match self {
            Self::Binance(ex) => ex.handle_ws_error(trading_data_schema),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum TraderExchangeId {
    #[default]
    Bybit,
}

#[derive(Clone)]
pub enum TraderExchangeWrapper {
    Bybit(BybitTraderExchange),
}

impl TraderExchangeWrapper {
    pub fn new(selected_exchange: TraderExchangeId, trading_settings: &TradingSettings) -> Self {
        match selected_exchange {
            TraderExchangeId::Bybit => Self::Bybit(BybitTraderExchange::new(trading_settings)),
        }
    }

    pub fn get_selection_list() -> Vec<String> {
        vec![String::from("Bybit")]
    }

    pub fn patch_settings(&mut self, trading_settings: &TradingSettings) {
        match self {
            TraderExchangeWrapper::Bybit(ex) => ex.patch_settings(trading_settings),
        }
    }
}

impl TraderHelper for TraderExchangeWrapper {
    fn get_trading_settings(&self) -> &TradingSettings {
        match self {
            Self::Bybit(ex) => ex.get_trading_settings(),
        }
    }

    fn get_taker_fee(&self) -> f64 {
        match self {
            Self::Bybit(ex) => ex.get_taker_fee(),
        }
    }

    fn get_maker_fee(&self) -> f64 {
        match self {
            Self::Bybit(ex) => ex.get_maker_fee(),
        }
    }

    fn calculate_open_order_units_and_balance_remainder(
        &self,
        side: Side,
        order_cost: f64,
        price: f64,
    ) -> Result<(f64, f64), GlowError> {
        match self {
            Self::Bybit(ex) => {
                ex.calculate_open_order_units_and_balance_remainder(side, order_cost, price)
            }
        }
    }

    fn get_order_fee_rate(&self, order_type: OrderType) -> (f64, bool) {
        match self {
            Self::Bybit(ex) => ex.get_order_fee_rate(order_type),
        }
    }

    fn calculate_order_fees(
        &self,
        order_type: OrderType,
        side: Side,
        units: f64,
        price: f64,
    ) -> ((f64, f64), f64, bool) {
        match self {
            Self::Bybit(ex) => ex.calculate_order_fees(order_type, side, units, price),
        }
    }

    fn calculate_order_stop_loss_price(&self, side: Side, price: f64) -> Option<f64> {
        match self {
            Self::Bybit(ex) => ex.calculate_order_stop_loss_price(side, price),
        }
    }

    fn calculate_order_take_profit_price(&self, side: Side, price: f64) -> Option<f64> {
        match self {
            Self::Bybit(ex) => ex.calculate_order_take_profit_price(side, price),
        }
    }

    fn get_contracts(&self) -> &HashMap<SymbolId, Contract> {
        match self {
            Self::Bybit(ex) => ex.get_contracts(),
        }
    }
}

impl TraderExchange for TraderExchangeWrapper {
    fn new_open_order(&self, side: Side, order_cost: f64, price: f64) -> Result<Order, GlowError> {
        match self {
            Self::Bybit(ex) => ex.new_open_order(side, order_cost, price),
        }
    }

    fn get_ws_url(&self) -> Result<Url, GlowError> {
        match self {
            Self::Bybit(ex) => ex.get_ws_url(),
        }
    }

    async fn auth_ws(
        &self,
        wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<(), GlowError> {
        match self {
            Self::Bybit(ex) => ex.auth_ws(wss).await,
        }
    }

    async fn subscribe_ws(
        &self,
        wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<(), GlowError> {
        match self {
            Self::Bybit(ex) => ex.subscribe_ws(wss).await,
        }
    }

    async fn fetch_order_executions(
        &self,
        order_uuid: String,
        start_timestamp: i64,
        end_timestamp: i64,
    ) -> Result<Vec<Execution>, GlowError> {
        match self {
            Self::Bybit(ex) => {
                ex.fetch_order_executions(order_uuid, start_timestamp, end_timestamp)
                    .await
            }
        }
    }

    async fn fetch_history_order(
        &self,
        id: Option<String>,
        side: Option<Side>,
        fetch_executions: bool,
    ) -> Result<Order, GlowError> {
        match self {
            Self::Bybit(ex) => ex.fetch_history_order(id, side, fetch_executions).await,
        }
    }

    async fn fetch_current_order(
        &self,
        order_id: String,
        fetch_executions: bool,
    ) -> Result<Order, GlowError> {
        match self {
            Self::Bybit(ex) => ex.fetch_current_order(order_id, fetch_executions).await,
        }
    }

    async fn fetch_current_trade_position(&self) -> Result<Option<Trade>, GlowError> {
        match self {
            Self::Bybit(ex) => ex.fetch_current_trade_position().await,
        }
    }

    async fn fetch_trade_state(
        &self,
        trade_id: String,
        last_status: TradeStatus,
    ) -> Result<Trade, GlowError> {
        match self {
            Self::Bybit(ex) => ex.fetch_trade_state(trade_id, last_status).await,
        }
    }

    async fn fetch_current_usdt_balance(&self) -> Result<Balance, GlowError> {
        match self {
            Self::Bybit(ex) => ex.fetch_current_usdt_balance().await,
        }
    }

    async fn open_order(
        &self,
        side: Side,
        amount: f64,
        expected_price: f64,
    ) -> Result<Order, GlowError> {
        match self {
            Self::Bybit(ex) => ex.open_order(side, amount, expected_price).await,
        }
    }

    async fn amend_order(
        &self,
        order_id: String,
        updated_units: Option<f64>,
        updated_price: Option<f64>,
        updated_stop_loss_price: Option<f64>,
        updated_take_profit_price: Option<f64>,
    ) -> Result<bool, GlowError> {
        match self {
            Self::Bybit(ex) => {
                ex.amend_order(
                    order_id,
                    updated_units,
                    updated_price,
                    updated_stop_loss_price,
                    updated_take_profit_price,
                )
                .await
            }
        }
    }

    async fn try_close_position(&self, trade: &Trade, est_price: f64) -> Result<Order, GlowError> {
        match self {
            Self::Bybit(ex) => ex.try_close_position(trade, est_price).await,
        }
    }

    async fn cancel_order(&self, order_id: String) -> Result<bool, GlowError> {
        match self {
            Self::Bybit(ex) => ex.cancel_order(order_id).await,
        }
    }

    async fn set_leverage(&self, leverage: Leverage) -> Result<bool, GlowError> {
        match self {
            Self::Bybit(ex) => ex.set_leverage(leverage).await,
        }
    }

    fn get_http_client(&self) -> &Client {
        match self {
            Self::Bybit(ex) => ex.get_http_client(),
        }
    }

    fn get_ws_ping_interval(&self) -> u64 {
        match self {
            Self::Bybit(ex) => ex.get_ws_ping_interval(),
        }
    }

    fn get_ws_ping_message(&self) -> Result<Message, GlowError> {
        match self {
            Self::Bybit(ex) => ex.get_ws_ping_message(),
        }
    }

    fn process_ws_message(&self, json: &String) -> Result<(), GlowError> {
        match self {
            Self::Bybit(ex) => ex.process_ws_message(json),
        }
    }

    async fn update_position_data_on_faulty_exchange_ws(&self) -> Result<(), GlowError> {
        match self {
            Self::Bybit(ex) => ex.update_position_data_on_faulty_exchange_ws().await,
        }
    }

    async fn init(&mut self) -> Result<(), GlowError> {
        match self {
            Self::Bybit(ex) => ex.init().await,
        }
    }

    async fn listen_messages(
        &mut self,
        wss: WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<(), GlowError> {
        match self {
            Self::Bybit(ex) => ex.listen_messages(wss).await,
        }
    }

    fn get_balance_update_emitter(&self) -> &BehaviorSubject<Balance> {
        match self {
            TraderExchangeWrapper::Bybit(ex) => ex.get_balance_update_emitter(),
        }
    }

    fn get_executions_update_emitter(&self) -> &BehaviorSubject<Vec<Execution>> {
        match self {
            TraderExchangeWrapper::Bybit(ex) => ex.get_executions_update_emitter(),
        }
    }

    fn get_order_update_emitter(&self) -> &BehaviorSubject<OrderAction> {
        match self {
            TraderExchangeWrapper::Bybit(ex) => ex.get_order_update_emitter(),
        }
    }

    fn get_trade_update_emitter(&self) -> &BehaviorSubject<Option<Trade>> {
        match self {
            TraderExchangeWrapper::Bybit(ex) => ex.get_trade_update_emitter(),
        }
    }
}

impl BenchmarkExchange for TraderExchangeWrapper {
    fn new_benchmark_open_order(
        &self,
        timestamp: i64,
        side: Side,
        order_cost: f64,
        price: f64,
    ) -> Result<Order, GlowError> {
        match self {
            Self::Bybit(ex) => ex.new_benchmark_open_order(timestamp, side, order_cost, price),
        }
    }

    fn new_benchmark_close_order(
        &self,
        timestamp: i64,
        trade_id: &String,
        close_price: f64,
        open_order: Order,
        final_status: OrderStatus,
    ) -> Result<Order, GlowError> {
        match self {
            Self::Bybit(ex) => ex.new_benchmark_close_order(
                timestamp,
                trade_id,
                close_price,
                open_order,
                final_status,
            ),
        }
    }

    fn close_benchmark_trade_on_binding_price(
        &self,
        trade: &Trade,
        current_timestamp: i64,
        binding_price: f64,
    ) -> Result<Trade, GlowError> {
        match self {
            Self::Bybit(ex) => {
                ex.close_benchmark_trade_on_binding_price(trade, current_timestamp, binding_price)
            }
        }
    }

    // fn check_price_level_modifiers(
    //     &self,
    //     trade: &Trade,
    //     current_timestamp: i64,

    // ) -> Result<Option<Trade>, GlowError> {
    //     match self {
    //         Self::Bybit(ex) => ex.check_price_level_modifiers(
    //             trade,
    //             current_timestamp,
    //             close_price,
    //             stop_loss,
    //             take_profit,
    //             trailing_stop_loss,
    //             current_peak_returns,
    //         ),
    //     }
    // }
}
