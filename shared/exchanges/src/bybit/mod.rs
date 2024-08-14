pub mod enums;
pub mod functions;
pub mod structs;
use self::enums::BybitWsMessage;
use self::structs::{
    AmendOrderDto, EmptyObject, ExecutionData, FetchCurrentOrderDto, FetchExecutionsDto,
    FetchHistoryOrderDto, FetchPositionDto, OrderData, OrderResponse, PositionResponseData,
    SetLeverageDto, WsRequest,
};
use crate::{
    config::{EXCHANGES_CONFIGS, EXCHANGES_CONTEXTS, WS_RECONNECT_INTERVAL_IN_SECS},
    structs::{ApiCredentials, ApiEndpoints},
};
use common::constants::SECONDS_IN_MIN;
use common::enums::modifiers::price_level::{PriceLevel, TrailingStopLoss};
use common::enums::order_action::OrderAction;
use common::functions::{
    closest_multiple_below, current_datetime, current_timestamp, current_timestamp_ms,
    timestamp_minute_end,
};
use common::traits::exchange::{BenchmarkExchange, TraderHelper};
use common::{
    enums::{
        balance::Balance,
        http_method::HttpMethod,
        modifiers::{leverage::Leverage, position_lock::PositionLock},
        order_stage::OrderStage,
        order_status::OrderStatus,
        order_type::OrderType,
        side::Side,
        time_in_force::TimeInForce,
        trade_status::TradeStatus,
    },
    functions::{
        calculate_hmac, calculate_remainder, count_decimal_places, round_down_nth_decimal,
    },
    structs::{BehaviorSubject, Contract, Execution, Order, Trade, TradingSettings},
    traits::exchange::TraderExchange,
};
use enums::AccountType;
use futures_util::SinkExt;
use glow_error::GlowError;
use reqwest::{
    header::{self, HeaderMap, HeaderValue},
    Client, Error, RequestBuilder, Response,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{from_str, to_string as to_json_string};
use serde_urlencoded::to_string as to_url_string;
use std::{
    collections::HashMap,
    sync::Mutex,
    sync::{Arc, RwLock},
    time::Duration,
};
use structs::{
    BybitHttpResponseWrapper, CancelOrderDto, CreateOrderDto, FetchWalletBalanceDto,
    HttpResultList, PingWsMessage, WalletData,
};
use tokio::{
    net::TcpStream,
    select,
    time::sleep,
    time::{interval, sleep_until, Instant, Interval},
};
use tokio_stream::StreamExt;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;
// pub mod tests;

#[derive(Clone)]
pub struct BybitTraderExchange {
    pub contracts: &'static HashMap<&'static str, Contract>,
    credentials: ApiCredentials,
    current_trade_listener: BehaviorSubject<Option<Trade>>,
    endpoints: ApiEndpoints,
    pub fee_rates: (f64, f64),
    http: Client,
    last_ws_error_ts: Arc<Mutex<Option<i64>>>,
    pub name: &'static str,
    pub trading_settings: Arc<RwLock<TradingSettings>>,
    update_balance_listener: BehaviorSubject<Option<Balance>>,
    update_executions_listener: BehaviorSubject<Vec<Execution>>,
    update_order_listener: BehaviorSubject<Option<OrderAction>>,
}

impl BybitTraderExchange {
    pub fn new(
        current_trade_listener: &BehaviorSubject<Option<Trade>>,
        last_ws_error_ts: &Arc<Mutex<Option<i64>>>,
        trading_settings: &Arc<RwLock<TradingSettings>>,
        update_balance_listener: &BehaviorSubject<Option<Balance>>,
        update_executions_listener: &BehaviorSubject<Vec<Execution>>,
        update_order_listener: &BehaviorSubject<Option<OrderAction>>,
    ) -> Self {
        let config = EXCHANGES_CONFIGS
            .get("Bybit")
            .expect("Bybit to has Exchange Config");
        let context = EXCHANGES_CONTEXTS
            .get("Bybit")
            .expect("Bybit to has Exchange Context");

        let mut headers = HeaderMap::new();
        headers.insert("X-BAPI-SIGN-TYPE", HeaderValue::from_static("2"));
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
        headers.insert(
            header::ACCEPT_ENCODING,
            HeaderValue::from_static("gzip, deflate, br"),
        );
        headers.insert(header::ACCEPT, HeaderValue::from_static("application/json"));

        Self {
            contracts: &context.contracts,
            credentials: config.credentials,
            current_trade_listener: current_trade_listener.clone(),
            endpoints: config.endpoints,
            fee_rates: (context.maker_fee, context.taker_fee),
            http: Client::builder()
                .default_headers(headers)
                .build()
                .expect("Reqwest client to build"),
            last_ws_error_ts: last_ws_error_ts.clone(),
            name: "Bybit",
            trading_settings: trading_settings.clone(),
            update_balance_listener: update_balance_listener.clone(),
            update_executions_listener: update_executions_listener.clone(),
            update_order_listener: update_order_listener.clone(),
        }
    }

    async fn try_parse_response<T: DeserializeOwned>(
        result: Result<Response, Error>,
    ) -> Result<T, GlowError> {
        if let Err(error) = result {
            return Err(GlowError::from(error));
        }

        let response = result.unwrap();
        if !response.status().is_success() {
            let description = format!("try_response -> unsucessful response {:?}", response);
            return Err(GlowError::new_unsuccessful_response(description));
        }
        let response_text = response.text().await?;
        let parsed_response = from_str::<T>(&response_text)?;
        Ok(parsed_response)
    }

    fn get_signature<S: Serialize>(
        &self,
        method: HttpMethod,
        timestamp: i64,
        recv_window: i32,
        payload: &S,
    ) -> Result<String, GlowError> {
        let suffix = match method {
            HttpMethod::Get => to_url_string(&payload)?,
            HttpMethod::Post => to_json_string(&payload)?,
        };
        let query_string = format!(
            "{}{}{}{}",
            timestamp, self.credentials.key, recv_window, suffix
        );
        let signature = calculate_hmac(&self.credentials.key, &query_string)?;
        Ok(signature)
    }

    fn append_request_headers(
        &self,
        request_builder: RequestBuilder,
        timestamp: i64,
        signature: String,
        recv_window: i32,
    ) -> RequestBuilder {
        request_builder
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-API-KEY", self.credentials.key)
            .header("X-BAPI-TIMESTAMP", timestamp)
            .header("X-BAPI-RECV-WINDOW", recv_window)
    }

    fn prepare_request_builder<T: Serialize>(
        &self,
        method: HttpMethod,
        req_uri: &str,
        payload: &T,
    ) -> Result<RequestBuilder, GlowError> {
        let timestamp = current_timestamp_ms();
        let recv_window = 5000;

        let query_params_str = to_url_string(&payload)?;
        let signature = self.get_signature(method, timestamp, recv_window, payload)?;
        let url = format!("{}{}?{}", self.endpoints.http, req_uri, query_params_str);
        let mut request_builder = self.get_http_client().get(url);
        request_builder =
            self.append_request_headers(request_builder, timestamp, signature, recv_window);
        Ok(request_builder)
    }
}

impl TraderHelper for BybitTraderExchange {
    #[inline]
    fn get_contracts(&self) -> &HashMap<&str, Contract> {
        self.contracts
    }
    #[inline]
    fn get_maker_fee(&self) -> f64 {
        self.fee_rates.0
    }
    #[inline]
    fn get_taker_fee(&self) -> f64 {
        self.fee_rates.1
    }
    #[inline]
    fn get_trading_settings(&self) -> TradingSettings {
        self.trading_settings
            .read()
            .expect("trading settings to be readable")
            .clone()
    }

    fn calculate_open_order_units_and_balance_remainder(
        &self,
        side: Side,
        order_cost: f64,
        price: f64,
    ) -> Result<(f64, f64), GlowError> {
        let trading_settings = self.get_trading_settings();
        let leverage_factor = trading_settings.leverage.get_factor();
        // TODO: check if open order type is relevant to this calculation
        let open_order_type = trading_settings.get_open_order_type();
        // Order Cost = Initial Margin + Fee to Open Position + Fee to Close Position
        // Initial Margin = (Order Price × Order Quantity) / Leverage
        // Fee to Open Position = Order Quantity × Order Price × Taker Fee Rate
        // Fee to Close Position = Order Quantity × Bankruptcy Price × Taker Fee Rate
        // Order Cost × Leverage / [Order Price × (0.0012 × Leverage + 1.0006)]

        // _balance / ((price / leverage_factor) + (price * fee_rate) + (default_price * fee_rate))
        let contract = self.get_traded_contract();
        let taker_fee_rate = self.get_taker_fee();

        // let (fee_rate, is_maker) = self.get_order_fee_rate(open_order_type);

        let fee_position_modifier = if side == Side::Sell {
            taker_fee_rate
        } else if side == Side::Buy {
            -taker_fee_rate
        } else {
            let error = format!(
                "calculate_order_units_and_balance_remainder -> Invalid side {:?}",
                side
            );

            return Err(GlowError::new(String::from("Invalid Side Error"), error));
        };

        let mut units = order_cost * leverage_factor
            / (price
                * (((2.0 * taker_fee_rate) * leverage_factor) + (1.0 + fee_position_modifier)));

        let fract_units = calculate_remainder(units, contract.minimum_order_size);

        let size_decimals = count_decimal_places(contract.minimum_order_size);

        units = round_down_nth_decimal(units - fract_units, size_decimals);

        if units == 0.0
            || units < contract.minimum_order_size
            || units > contract.maximum_order_size
            || leverage_factor > contract.max_leverage
        {
            println!("Some contract constraints stopped the order from being placed");
            let error;
            if units == 0.0 {
                error = format!("units == 0.0 -> {}", units == 0.0);
            } else if units < contract.minimum_order_size {
                error = format!(
                    "units < contract.minimum_order_size -> {} | units = {}, minimum order size = {}",
                    units < contract.minimum_order_size,
                    units,
                    contract.minimum_order_size
                );
            } else if units > contract.maximum_order_size {
                error = format!(
                    "units > contract.maximum_order_size -> {} | units = {}, maximum order size = {}",
                    units > contract.maximum_order_size,
                    units,
                    contract.maximum_order_size
                );
            } else if leverage_factor > contract.max_leverage {
                error = format!(
                        "leverage_factor > contract.max_leverage -> {}  | leverage_factor = {}, max_leverage = {}",
                        leverage_factor > contract.max_leverage,
                        leverage_factor,
                        contract.max_leverage
                    );
            } else {
                error = String::from("unknown error");
            }

            let error = format!(
                "calculate_order_units -> Invalid side {:?}, error {}",
                side, error
            );

            return Err(GlowError::new(String::from("Invalid Side Error"), error));
        }

        let balance_remainder = fract_units * price / leverage_factor;
        // let open_fee = units * price * fee_rate;
        // let bankruptcy_price = if side == Side::Sell {
        //     // Bankruptcy Price for Short Position = Order Price × ( Leverage + 1) / Leverage
        //     price * (leverage_factor + 1.0) / leverage_factor
        // } else if side == Side::Buy {
        //     // Bankruptcy Price for Long Position = Order Price × ( Leverage − 1) / Leverage
        //     price * (leverage_factor - 1.0) / leverage_factor
        // } else {
        //     0.0
        // };
        // let close_fee = units * bankruptcy_price * fee_rate;

        return Ok((units, balance_remainder));
    }

    fn get_order_fee_rate(&self, order_type: OrderType) -> (f64, bool) {
        let maker_fee_rate = self.get_maker_fee();
        let taker_fee_rate = self.get_taker_fee();

        if order_type == OrderType::Limit {
            (maker_fee_rate, true)
        } else {
            (taker_fee_rate, false)
        }
    }

    fn calculate_order_fees(
        &self,
        order_type: OrderType,
        side: Side,
        units: f64,
        price: f64,
    ) -> ((f64, f64), f64, bool) {
        let trading_settings = self.get_trading_settings();
        let leverage_factor = trading_settings.leverage.get_factor();
        let (fee_rate, is_maker) = self.get_order_fee_rate(order_type);
        let open_fee = units * price * fee_rate;
        let bankruptcy_price = if side == Side::Sell {
            // Bankruptcy Price for Short Position = Order Price × ( Leverage + 1) / Leverage
            price * (leverage_factor + 1.0) / leverage_factor
        } else if side == Side::Buy {
            // Bankruptcy Price for Long Position = Order Price × ( Leverage − 1) / Leverage
            price * (leverage_factor - 1.0) / leverage_factor
        } else {
            0.0
        };
        let close_fee = units * bankruptcy_price * fee_rate;

        ((open_fee, close_fee), fee_rate, is_maker)
    }

    fn calculate_order_stop_loss_price(&self, side: Side, price: f64) -> Option<f64> {
        let contract = self.get_traded_contract();
        let trading_settings = self.get_trading_settings();
        let stop_loss = trading_settings.price_level_modifier_map.get("sl");
        let leverage_factor = trading_settings.leverage.get_factor();
        match stop_loss {
            Some(stop_loss) => {
                let stop_loss_percentage = stop_loss.get_percentage();
                // short
                // factor 60%
                // price * (leverage_factor + (1.0 * stop_loss_percentage)) / leverage_factor
                // 50 * (10 + (1 * 0.6)) / 10
                // 50 * (10 + 0.6) / 10
                // 530 / 10
                // 53
                // (53 - 50) / (50/10)
                // 3/5 = 60%

                // long
                // factor 30%
                // price * (leverage_factor - (1.0 * stop_loss_percentage)) / leverage_factor
                // 50 * (10 - (0.3)) / 10
                // 50 * (9.7) / 10
                // 48.5
                // (48.5 - 50) / (50 / 10)
                // 1.5/5
                // 30%

                let position_mod = if side == Side::Sell {
                    leverage_factor + (1.0 * stop_loss_percentage)
                } else if side == Side::Buy {
                    leverage_factor - (1.0 * stop_loss_percentage)
                } else {
                    return None;
                };
                let mut stop_loss_price = price * position_mod / leverage_factor;
                let tick_decimals = count_decimal_places(contract.tick_size);
                let stop_loss_price_remainder =
                    calculate_remainder(stop_loss_price, contract.tick_size);
                stop_loss_price = round_down_nth_decimal(
                    stop_loss_price - stop_loss_price_remainder,
                    tick_decimals,
                );
                Some(stop_loss_price)
            }
            None => None,
        }
    }

    fn calculate_order_take_profit_price(&self, side: Side, price: f64) -> Option<f64> {
        let contract = self.get_traded_contract();
        let trading_settings = self.get_trading_settings();
        let take_profit = trading_settings.price_level_modifier_map.get("tp");
        let leverage_factor = trading_settings.leverage.get_factor();
        match take_profit {
            Some(take_profit) => {
                let take_profit_percentage = take_profit.get_percentage();
                // short
                // factor 80%
                // price * (leverage_factor - (1.0 * take_profit_percentage)) / leverage_factor
                // 50 * (10 - 0.8) / 10
                // 50 * 9.2 / 10
                // 460 / 10
                // 46
                // (46 - 50) / (50/10)
                // 4/5 = 80%

                // long
                // factor 30%
                // price * (leverage_factor + (1.0 * take_profit_percentage)) / leverage_factor
                // 50 * (10 + 0.3) / 10
                // 50 * 10.3 / 10
                // 51.5
                // (51.5 - 50) / (50 / 10)
                // 1.5/5
                // 30%

                let position_mod = if side == Side::Sell {
                    leverage_factor - (1.0 * take_profit_percentage)
                } else if side == Side::Buy {
                    leverage_factor + (1.0 * take_profit_percentage)
                } else {
                    return None;
                };
                let mut take_profit_price = price * position_mod / leverage_factor;
                let tick_decimals = count_decimal_places(contract.tick_size);
                let take_profit_price_remainer =
                    calculate_remainder(take_profit_price, contract.tick_size);
                take_profit_price = round_down_nth_decimal(
                    take_profit_price - take_profit_price_remainer,
                    tick_decimals,
                );
                Some(take_profit_price)
            }
            None => None,
        }
    }
}

impl TraderExchange for BybitTraderExchange {
    #[inline]
    fn get_http_client(&self) -> &Client {
        &self.http
    }
    #[inline]
    fn get_ws_url(&self) -> Result<Url, GlowError> {
        let url = Url::parse(&format!("{}/v5/private", self.endpoints.ws))?;
        Ok(url)
    }

    async fn auth_ws(
        &self,
        wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<(), GlowError> {
        let timestamp = current_timestamp();
        let expires = ((timestamp + 5) * 1000).to_string();
        let message = format!("GET/realtime{}", expires);
        let signature = calculate_hmac(&self.credentials.secret, &message).unwrap();
        let args = vec![self.credentials.key.to_string(), expires, signature];
        let auth_request = WsRequest::new("auth".to_string(), args);
        let auth_json_str = to_json_string(&auth_request)
            .expect(&format!("JSON ({:?}) parsing error", auth_request));
        let auth_message = Message::Text(auth_json_str);
        wss.send(auth_message).await?;
        Ok(())
    }

    async fn subscribe_ws(
        &self,
        wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<(), GlowError> {
        let subscribe_request = WsRequest::new(
            "subscribe".to_string(),
            vec![
                "execution".to_string(),
                "order".to_string(),
                "wallet".to_string(),
            ],
        );

        let subscription_json = to_json_string(&subscribe_request)
            .expect(&format!("JSON ({:?}) parsing error", subscribe_request));

        let subscription_message = Message::Text(subscription_json);

        wss.send(subscription_message).await?;
        Ok(())
    }

    // TODO: divide this in own methods
    fn process_ws_message(&self, json: &String) -> Result<(), GlowError> {
        let response: BybitWsMessage = from_str::<BybitWsMessage>(&json).unwrap_or_default();

        match response {
            BybitWsMessage::None => Ok(()),
            BybitWsMessage::Pong(message) => {
                println!(
                    "{:?} | Trading Exchange pong message: {:?}",
                    current_datetime(),
                    message
                );
                Ok(())
            }
            BybitWsMessage::Auth(message) => {
                let success = message.success;
                println!(
                    "{:?} | Trading Exchange websocket authentication success: {}",
                    current_datetime(),
                    success
                );
                Ok(())
            }
            BybitWsMessage::Execution(message) => {
                let executions = message
                    .data
                    .into_iter()
                    .map(|data| {
                        Execution::new(
                            data.exec_id,
                            data.order_id,
                            data.order_type,
                            data.exec_time,
                            data.exec_price,
                            data.exec_qty,
                            data.exec_fee,
                            data.fee_rate,
                            data.is_maker,
                            data.closed_size.unwrap_or(0.0),
                        )
                    })
                    .collect();
                self.update_executions_listener.next(executions);
                Ok(())
            }
            BybitWsMessage::Order(message) => {
                let order_response = message.data.into_iter().find(|order_data| {
                    order_data.symbol == self.get_traded_symbol() && !order_data.is_trigger_order()
                });

                if let None = order_response {
                    println!(
                        "No order response was found being the same symbol and not a trigger order"
                    );
                    return Ok(());
                }

                let order_response = order_response.unwrap();

                if order_response.is_cancel() {
                    let cancelled_order = order_response.new_order_from_response_data(
                        self.get_leverage_factor(),
                        self.get_taker_fee(),
                    );
                    let cancel_order_action = OrderAction::Cancel(cancelled_order);
                    self.update_order_listener.next(Some(cancel_order_action));
                    return Ok(());
                }
                let updated_order: Order = order_response
                    .new_order_from_response_data(self.get_leverage_factor(), self.get_taker_fee());

                if updated_order.is_stop {
                    let stop_order_action = OrderAction::Stop(updated_order);
                    self.update_order_listener.next(Some(stop_order_action));
                    return Ok(());
                }

                let order_action = OrderAction::Update(updated_order);
                self.update_order_listener.next(Some(order_action));
                Ok(())
            }
            BybitWsMessage::Wallet(message) => {
                let usdt_data = message
                    .data
                    .into_iter()
                    .find_map(|wallet_data| {
                        let found_coin = wallet_data
                            .coin
                            .into_iter()
                            .find(|coin_data| coin_data.coin == "USDT");
                        if let Some(coin) = found_coin {
                            Some(coin)
                        } else {
                            None
                        }
                    })
                    .expect(
                        "process_ws_message error -> USDT coin is missing in ws wallet message",
                    );

                let balance = Balance {
                    timestamp: message.creation_time,
                    available_to_withdraw: usdt_data.available_to_withdraw,
                    wallet_balance: usdt_data.wallet_balance,
                };
                self.update_balance_listener.next(Some(balance));
                Ok(())
            }
        }
    }

    async fn fetch_order_executions(
        &self,
        order_uuid: String,
        start_timestamp: i64,
        end_timestamp: i64,
    ) -> Result<Vec<Execution>, GlowError> {
        let traded_symbol = self.get_traded_symbol();
        let payload = FetchExecutionsDto {
            category: "linear".to_string(),
            order_uuid,
            symbol: traded_symbol.to_string(),
            start_timestamp,
            end_timestamp,
        };

        let request_builder =
            self.prepare_request_builder(HttpMethod::Get, "/v5/execution/list", &payload)?;

        let result = request_builder.send().await;

        let parsed_response = Self::try_parse_response::<
            BybitHttpResponseWrapper<HttpResultList<ExecutionData>>,
        >(result)
        .await?;

        if parsed_response.ret_code != 0 || parsed_response.ret_message != String::from("OK") {
            return Ok(vec![]);
        }

        let executions: Vec<Execution> = parsed_response
            .result
            .list
            .into_iter()
            .map(|execution_response| execution_response.into())
            .collect();
        Ok(executions)
    }

    async fn fetch_history_order(
        &self,
        id: Option<String>,
        side: Option<Side>,
        fetch_executions: bool,
    ) -> Result<Order, GlowError> {
        let traded_symbol = self.get_traded_symbol();
        let payload = FetchHistoryOrderDto {
            category: "linear".to_string(),
            id: id.clone(),
            side,
            symbol: traded_symbol.to_string(),
        };

        let request_builder =
            self.prepare_request_builder(HttpMethod::Get, "/v5/order/history", &payload)?;

        let result = request_builder.send().await;

        let mut parsed_response =
            Self::try_parse_response::<BybitHttpResponseWrapper<HttpResultList<OrderData>>>(result)
                .await?;

        if parsed_response.ret_code != 0 || parsed_response.ret_message != "OK".to_string() {
            let error = format!(
                "fetch_history_order -> unexpected order response -> {:?}",
                parsed_response
            );
            return Err(GlowError::new(String::from("Failed Query"), error));
        }

        if id.clone().is_none() {
            parsed_response
                .result
                .list
                .sort_by(|a, b| b.updated_time.cmp(&a.updated_time));
        }

        let order_response = parsed_response
            .result
            .list
            .into_iter()
            .find(|order_data| !order_data.is_cancel() && !order_data.is_trigger_order());

        if let None = order_response {
            let error = "fetch_history_order -> no closed order was found".to_string();
            return Err(GlowError::new(String::from("Failed Query"), error));
        }

        let order_response = order_response.unwrap();

        let executed_qty = order_response.cum_exec_qty;
        let trading_settings = self.get_trading_settings();
        let leverage_factor = trading_settings.leverage.get_factor();

        let mut order: Order =
            order_response.new_order_from_response_data(leverage_factor, self.get_taker_fee());

        let mut executions = vec![];

        if fetch_executions && executed_qty > 0.0 {
            let fetched_executions = self
                .fetch_order_executions(order.uuid.clone(), order.created_at, order.updated_at)
                .await?;
            executions.extend(fetched_executions);
        }
        order = order.push_executions_if_new(executions);
        Ok(order)
    }

    async fn fetch_current_order(
        &self,
        order_id: String,
        fetch_executions: bool,
    ) -> Result<Order, GlowError> {
        let traded_symbol = self.get_traded_symbol();
        let payload = FetchCurrentOrderDto {
            category: "linear".to_string(),
            id: order_id.clone(),
            symbol: traded_symbol.to_string(),
            open_only: 2,
        };

        let request_builder =
            self.prepare_request_builder(HttpMethod::Get, "/v5/order/realtime", &payload)?;

        let result = request_builder.send().await;

        let parsed_response =
            Self::try_parse_response::<BybitHttpResponseWrapper<HttpResultList<OrderData>>>(result)
                .await?;

        if parsed_response.ret_code != 0 || parsed_response.ret_message != String::from("OK") {
            let description = format!(
                "fetch_opened_order -> inadequate order response -> {:?}",
                parsed_response
            );
            return Err(GlowError::new_unsuccessful_response(description));
        }

        let order_response =
            parsed_response.result.list.into_iter().find(
                |order_data| {
                    println!("@@@@ fetch_opened_order. order_data.order_link_id = {}, order_data.is_trigger_order() = {}", order_data.order_link_id, order_data.is_trigger_order());
                    order_data.order_link_id == order_id.clone()
                        && !order_data.is_trigger_order()
                },
            );

        if let None = order_response {
            let mut retries = 2;
            let error = format!("fetch_opened_order -> order wasn't opened at all");

            let mut result = Err(GlowError::new(String::from("Failed Query"), error));
            while retries > 0 {
                match self
                    .fetch_history_order(Some(order_id.clone()), None, fetch_executions)
                    .await
                {
                    Ok(order) => {
                        result = Ok(order);
                        retries = 0;
                    }
                    Err(_) => {
                        if retries != 0 {
                            retries -= 1;
                            sleep(Duration::from_secs(5)).await;
                        }
                    }
                }
            }
            return result;
        }

        let order_response = order_response.unwrap();

        let mut executions = vec![];
        let executed_qty = order_response.cum_exec_qty;
        let leverage_factor = self.get_leverage_factor();
        let mut order: Order =
            order_response.new_order_from_response_data(leverage_factor, self.get_taker_fee());

        if fetch_executions && executed_qty > 0.0 {
            let fetched_executions = self
                .fetch_order_executions(order.uuid.clone(), order.created_at, order.updated_at)
                .await?;
            executions.extend(fetched_executions);
        }
        order = order.push_executions_if_new(executions);
        Ok(order)
    }

    async fn fetch_current_trade_position(&self) -> Result<Option<Trade>, GlowError> {
        let traded_symbol = self.get_traded_symbol();
        let payload = FetchPositionDto {
            category: "linear".to_string(),
            symbol: traded_symbol.to_string(),
        };

        let request_builder =
            self.prepare_request_builder(HttpMethod::Get, "/v5/position/list", &payload)?;
        let result = request_builder.send().await;
        let parsed_response = Self::try_parse_response::<
            BybitHttpResponseWrapper<HttpResultList<PositionResponseData>>,
        >(result)
        .await?;

        let position_response = parsed_response.result.list.into_iter().next();

        if let None = position_response {
            return Ok(None);
        }
        let position_response = position_response.unwrap();

        if position_response.side == Side::None {
            return Ok(None);
        }

        let latest_order = self
            .fetch_history_order(None, Some(position_response.side), true)
            .await?;

        let open_order;
        let close_order: Option<Order>;

        if latest_order.is_close {
            let open_order_id = latest_order.id.replace("close", "open");
            close_order = Some(latest_order);
            open_order = self
                .fetch_history_order(Some(open_order_id), None, true)
                .await?;
        } else {
            let close_order_id = latest_order.id.replace("open", "close");
            open_order = latest_order;
            match self.fetch_current_order(close_order_id, true).await {
                Ok(order) => {
                    close_order = Some(order);
                }
                Err(_) => {
                    close_order = None;
                }
            }
        }

        let trade = Trade::new(open_order, close_order);

        Ok(Some(trade))
    }

    /// automatically fetches respective orders and executions
    // TODO: verify if this is usefull at all
    async fn fetch_trade_state(
        &self,
        trade_id: String,
        last_status: TradeStatus,
    ) -> Result<Trade, GlowError> {
        let traded_symbol = self.get_traded_symbol();
        let payload = FetchPositionDto {
            category: "linear".to_string(),
            symbol: traded_symbol.to_string(),
        };

        let request_builder =
            self.prepare_request_builder(HttpMethod::Get, "/v5/position/list", &payload)?;

        let result = request_builder.send().await;

        let parsed_response = Self::try_parse_response::<
            BybitHttpResponseWrapper<HttpResultList<PositionResponseData>>,
        >(result)
        .await?;

        let position_response = parsed_response.result.list.into_iter().next();

        if let None = position_response {
            let error = format!(
                r#"fetch_trade_state -> symbol {} doesn't have any open position"#,
                traded_symbol
            );
            return Err(GlowError::new(
                String::from("Invalid Position Error"),
                error,
            ));
        }

        // if position.side == Side::None {
        //     return Ok(None);
        // }
        let open_order_id = format!("{}_open", trade_id);
        let close_order_id = format!("{}_close", trade_id);

        let open_order = self.fetch_current_order(open_order_id, true).await?;
        let close_order: Option<Order>;

        match last_status {
            TradeStatus::Cancelled => {
                close_order = None;
            }
            TradeStatus::New => match open_order.status {
                OrderStatus::Cancelled | OrderStatus::StandBy | OrderStatus::PartiallyFilled => {
                    close_order = None;
                }
                OrderStatus::Filled => match self.fetch_current_order(close_order_id, true).await {
                    Ok(opened_close_order) => {
                        close_order = Some(opened_close_order);
                    }
                    Err(_) => {
                        close_order = None;
                    }
                },
                OrderStatus::PartiallyClosed
                | OrderStatus::Closed
                | OrderStatus::StoppedBR
                | OrderStatus::StoppedSL
                | OrderStatus::StoppedTP
                | OrderStatus::StoppedTSL => {
                    let error = format!(
                        r#"fetch_trade_state -> TradeStatus::New -> open order has {:?} status"#,
                        last_status
                    );
                    return Err(GlowError::new(String::from("Wrong Status Error"), error));
                }
            },
            TradeStatus::PartiallyOpen => match open_order.status {
                OrderStatus::Cancelled | OrderStatus::StandBy => {
                    let error = format!(
                        r#"fetch_trade_state -> TradeStatus::PartiallyOpen -> open order has PartiallyClosed/Closed status {:?}"#,
                        open_order
                    );
                    return Err(GlowError::new(String::from("Wrong Status Error"), error));
                }
                OrderStatus::PartiallyFilled => {
                    close_order = None;
                }
                OrderStatus::Filled => match self.fetch_current_order(close_order_id, true).await {
                    Ok(opened_close_order) => {
                        close_order = Some(opened_close_order);
                    }
                    Err(_) => {
                        close_order = None;
                    }
                },
                OrderStatus::PartiallyClosed
                | OrderStatus::Closed
                | OrderStatus::StoppedBR
                | OrderStatus::StoppedSL
                | OrderStatus::StoppedTP
                | OrderStatus::StoppedTSL => {
                    let error = format!(
                        r#"fetch_trade_state -> TradeStatus::PartiallyOpen -> open order has {:?} status"#,
                        last_status
                    );
                    return Err(GlowError::new(String::from("Wrong Status Error"), error));
                }
            },
            TradeStatus::PendingCloseOrder => {
                match self.fetch_current_order(close_order_id, true).await {
                    Ok(opened_close_order) => {
                        close_order = Some(opened_close_order);
                    }
                    Err(_) => {
                        close_order = None;
                    }
                }
            }
            TradeStatus::CloseOrderStandBy | TradeStatus::PartiallyClosed | TradeStatus::Closed => {
                let opened_close_order = self.fetch_current_order(close_order_id, true).await?;
                close_order = Some(opened_close_order);
            }
        }

        let trade = Trade::new(open_order, close_order);

        Ok(trade)

        // // NOTE THAT fetch open orders only fetch partially filled and new orders, this needs fixing
        // let current_orders = self.fetch_open_orders(true).await?;
        // let total_current_orders = current_orders.len();

        // match total_current_orders {
        //     0 => {
        //         return Ok(None);
        //     }
        //     1 => {
        //         let current_order = current_orders.into_iter().next().unwrap();
        //         if current_order.is_close_order() {
        //             let open_order_id =
        //                 current_order.id.clone().replace("close", "open");
        //             // fetch open order from history
        //             let open_order =
        //                 self.fetch_closed_order(open_order_id, true).await?;

        //             let trade = Trade::new(
        //                 &traded_symbol,
        //                 position.leverage,
        //                 open_order,
        //                 Some(current_order),
        //             );
        //             Ok(Some(trade))
        //         } else {
        //             let trade = Trade::new(
        //                 &traded_symbol,
        //                 position.leverage,
        //                 current_order,
        //                 None,
        //             );
        //             Ok(Some(trade))
        //         }
        //     }
        //     2 => {
        //         // theoretically, this shouldn't happen, as order is amended to reduce left amount
        //         // when closing
        //         // get close / open order
        //         let open_order = current_orders
        //             .iter()
        //             .find(|order| order.id.contains("open"));
        //         let close_order = current_orders
        //             .iter()
        //             .find(|order| order.id.contains("close"));
        //         if open_order.is_none() || close_order.is_none() {
        //             let error = format!(
        //                 r#"fetch_open_trade -> either open or close order or both are missing.
        //                 Is open order = {}, is close order = {}
        //                 orders ={:?}"#,
        //                 open_order.is_none(),
        //                 close_order.is_none(),
        //                 current_orders
        //             );
        //             return Err(Error::CustomError(CustomError::new(error)));
        //         }
        //         let open_order = open_order.unwrap();
        //         let close_order = close_order.unwrap();
        //         // check if they share same id
        //         let trade_id = &open_order.id.replace("_open", "");
        //         if !close_order.id.contains(trade_id) {
        //             let error = format!(
        //                 r#"fetch_open_trade -> open order id and close order id don't match.
        //                 open order id = {}, close order id = {}
        //                 orders ={:?}"#,
        //                 open_order.id, close_order.id, current_orders
        //             );
        //             return Err(Error::CustomError(CustomError::new(error)));
        //         }
        //         let trade = Trade::new(
        //             &traded_symbol,
        //             position.leverage,
        //             open_order.clone(),
        //             Some(close_order.clone()),
        //         );
        //         Ok(Some(trade))
        //     }
        //     _ => {
        //         let error = format!(
        //             r#"fetch_open_trade -> more than 2 orders were found: {:?}"#,
        //             current_orders
        //         );
        //         Err(Error::CustomError(CustomError::new(error)))
        //     }
        // }
    }

    async fn fetch_current_usdt_balance(&self) -> Result<Balance, GlowError> {
        let payload = FetchWalletBalanceDto::new(AccountType::Contract, Some("USDT".to_string()));
        let request_builder =
            self.prepare_request_builder(HttpMethod::Get, "/v5/account/wallet-balance", &payload)?;
        let result = request_builder.send().await;
        let parsed_response = Self::try_parse_response::<
            BybitHttpResponseWrapper<HttpResultList<WalletData>>,
        >(result)
        .await?;

        let usdt_coin_data = parsed_response.result.list.iter().find_map(|wallet_data| {
            wallet_data
                .coin
                .iter()
                .find(|coin_data| coin_data.coin == "USDT".to_string())
        });

        let usdt_data = usdt_coin_data.expect("get_current_usdt_balance -> missing usdt coin data");

        let balance = Balance::new(
            parsed_response.time,
            usdt_data.available_to_withdraw,
            usdt_data.wallet_balance,
        );
        Ok(balance)
    }

    async fn open_order(
        &self,
        side: Side,
        amount: f64,
        expected_price: f64,
    ) -> Result<Order, GlowError> {
        assert_ne!(side, Side::None, "Invalid Open Order Side!");
        assert!(
            expected_price <= 0.0,
            "open_order -> expected price is less than 0!"
        );

        let trading_settings = self.get_trading_settings();
        let traded_contract = self.get_traded_contract();

        let mut expected_price = expected_price;
        if trading_settings.get_open_order_type() == OrderType::Limit {
            if side == Side::Sell {
                // add last price marginally in order to realize maker_fee
                let tick_decimals = count_decimal_places(traded_contract.tick_size);
                expected_price = round_down_nth_decimal(
                    expected_price + traded_contract.tick_size,
                    tick_decimals,
                );
            } else {
                // subtract last price marginally in order to realize maker_fee
                let tick_decimals = count_decimal_places(traded_contract.tick_size);
                expected_price = round_down_nth_decimal(
                    expected_price - traded_contract.tick_size,
                    tick_decimals,
                );
            }
        }
        let mut order = self.new_open_order(side, amount, expected_price)?;
        let order_id = order.id.clone();
        let payload: CreateOrderDto = order.clone().into();
        let request_builder =
            self.prepare_request_builder(HttpMethod::Post, "/v5/order/create", &payload)?;
        let result = request_builder.send().await;
        let parsed_response =
            Self::try_parse_response::<BybitHttpResponseWrapper<OrderResponse>>(result).await?;

        if parsed_response.ret_code != 0 || parsed_response.ret_message != String::from("OK") {
            let error = format!("open_order -> unexpected response => {:?}", parsed_response);
            println!("{:?}", error);
            return Err(GlowError::new(String::from("Wrong Response Error"), error));
        }
        if parsed_response.result.order_link_id != order_id {
            let error = format!(
                "open_order -> parsed_response.result.order_link_id != order_id! => {:?}",
                parsed_response.result
            );

            println!("{:?}", error);

            return Err(GlowError::new(
                String::from("Invalid Order Id Error"),
                error,
            ));
        }
        order.uuid = parsed_response.result.order_id;
        Ok(order)
    }

    async fn amend_order(
        &self,
        order_id: String,
        updated_units: Option<f64>,
        updated_price: Option<f64>,
        updated_stop_loss_price: Option<f64>,
        updated_take_profit_price: Option<f64>,
    ) -> Result<bool, GlowError> {
        let payload = AmendOrderDto {
            category: "linear".to_string(),
            order_id: order_id.clone(),
            updated_units,
            updated_price,
            updated_stop_loss_price,
            updated_take_profit_price,
        };
        let request_builder =
            self.prepare_request_builder(HttpMethod::Post, "/v5/order/amend", &payload)?;
        let result = request_builder.send().await;
        let parsed_response =
            Self::try_parse_response::<BybitHttpResponseWrapper<OrderResponse>>(result).await?;
        if parsed_response.ret_code != 0
            || parsed_response.ret_message != String::from("OK")
            || parsed_response.result.order_link_id != order_id
        {
            println!("amend_order -> unexpected response {:?}", parsed_response);
            return Ok(false);
        }

        Ok(true)
    }

    async fn try_close_position(&self, trade: &Trade, est_price: f64) -> Result<Order, GlowError> {
        let mut est_price = est_price;
        let traded_contract = self.get_traded_contract();
        let trading_settings = self.get_trading_settings();
        let close_order_type = trading_settings.get_close_order_type();

        if close_order_type == OrderType::Limit {
            if trade.open_order.side == Side::Sell {
                // close order will have open order opposite side, subtract last price marginally in order to realize maker_fee
                est_price -= traded_contract.minimum_order_size
            } else if trade.open_order.side == Side::Buy {
                // close order will have open order opposite side, add last price marginally in order to realize maker_fee
                est_price += traded_contract.minimum_order_size;
            }
        }

        let est_fee_rate = if close_order_type == OrderType::Market {
            self.fee_rates.1
        } else {
            self.fee_rates.0
        };

        let mut close_order = trade.new_close_order(close_order_type, est_price)?;

        // println!("try_close_position -> close order = {:?}", close_order);

        match trading_settings.position_lock_modifier {
            PositionLock::None => {}
            PositionLock::Fee => {
                // since we didn't assign close_order to trade, it makes sense to get unrealized profit and loss
                // as an estimate of profit and loss, and calculate it against the sum of fees
                let (profit_and_loss, _) = trade.calculate_unrealized_pnl_and_returns(est_price);
                let total_fee = trade.open_order.get_executed_order_fee()
                    + close_order.get_estimate_close_order_fee(est_fee_rate, est_price);
                if total_fee >= profit_and_loss.abs() {
                    let error = format!(
                        "Trade wasn't closed due to PositionLockModifier::Fee -> profit and loss = {}, total fee = {}",
                        profit_and_loss,
                        total_fee
                    );
                    return Err(GlowError::new(String::from("Close Position Error"), error));
                }
            }
            PositionLock::Loss => {
                let (profit_and_loss, _returns) =
                    trade.calculate_unrealized_pnl_and_returns(est_price);
                if profit_and_loss <= 0.0 {
                    let error = format!(
                        "Trade wasn't closed due to PositionLockModifier::Loss -> profit and loss = {}",
                        profit_and_loss
                    );
                    print!("{}", &error);
                    return Err(GlowError::new(String::from("Close Position Error"), error));
                }
            }
        }

        let close_order_id = close_order.id.clone();
        let payload: CreateOrderDto = close_order.clone().into();
        let request_builder =
            self.prepare_request_builder(HttpMethod::Post, "/v5/order/create", &payload)?;
        let result = request_builder.send().await;
        let parsed_response =
            Self::try_parse_response::<BybitHttpResponseWrapper<OrderResponse>>(result).await?;
        if parsed_response.ret_code != 0
            || parsed_response.ret_message != "OK".to_string()
            || parsed_response.result.order_link_id != close_order_id
        {
            close_order.uuid = parsed_response.result.order_id;
            Ok(close_order)
        } else {
            let error = format!(
                "try_close_position -> parsed response {:?}",
                parsed_response
            );
            Err(GlowError::new(String::from("Close Position Error"), error))
        }
    }

    async fn cancel_order(&self, order_id: String) -> Result<bool, GlowError> {
        let traded_symbol = self.get_traded_symbol();
        let payload = CancelOrderDto::new(
            order_id.clone(),
            "linear".to_string(),
            traded_symbol.to_string(),
        );
        let request_builder =
            self.prepare_request_builder(HttpMethod::Post, "/v5/order/cancel", &payload)?;
        let result = request_builder.send().await;
        let parsed_response =
            Self::try_parse_response::<BybitHttpResponseWrapper<OrderResponse>>(result).await?;
        if parsed_response.ret_code != 0
            || parsed_response.ret_message != String::from("OK")
            || parsed_response.result.order_link_id != order_id
        {
            println!("cancel_order -> parsed response {:?}", parsed_response);
            return Ok(false);
        }
        Ok(true)
    }

    async fn set_leverage(&self, leverage: Leverage) -> Result<bool, GlowError> {
        let leverage_factor = leverage.get_factor();
        let traded_contract = self.get_traded_contract();
        let traded_symbol = traded_contract.symbol;
        let max_leverage_allowed = traded_contract.max_leverage;
        assert!(
            leverage_factor > max_leverage_allowed,
            "symbol {} only allows for max {} leverage, {} was sent",
            traded_symbol,
            max_leverage_allowed,
            leverage_factor
        );

        let payload = SetLeverageDto::new(
            "linear".to_string(),
            traded_symbol.to_string(),
            leverage_factor,
        );

        let request_builder =
            self.prepare_request_builder(HttpMethod::Post, "/v5/position/set-leverage", &payload)?;
        let result = request_builder.send().await;
        let parsed_response =
            Self::try_parse_response::<BybitHttpResponseWrapper<EmptyObject>>(result).await?;
        if (parsed_response.ret_code != 0 || parsed_response.ret_message != "OK".to_string())
            || (parsed_response.ret_code == 110043
                && parsed_response.ret_message == "Set leverage not modified".to_string())
        {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn new_open_order(&self, side: Side, order_cost: f64, price: f64) -> Result<Order, GlowError> {
        let trading_settings = self.get_trading_settings();
        let leverage_factor = trading_settings.leverage.get_factor();
        let open_order_type = trading_settings.get_open_order_type();
        let (units, balance_remainder) =
            self.calculate_open_order_units_and_balance_remainder(side, order_cost, price)?;

        // TODO: check this
        let ((open_fee, _), fee_rate, is_maker) =
            self.calculate_order_fees(open_order_type, side, units, price);

        let contract = self.get_traded_contract();

        let timestamp = current_timestamp_ms();
        let id = format!(
            "{}_{}_{}",
            &contract.symbol,
            timestamp,
            OrderStage::Open.to_string()
        );
        let avg_price = if open_order_type == OrderType::Limit {
            Some(price)
        } else {
            None
        };
        let stop_loss_price = self.calculate_order_stop_loss_price(side, price);

        let take_profit_price = self.calculate_order_take_profit_price(side, price);

        let time_in_force = if open_order_type == OrderType::Limit {
            TimeInForce::GTC
        } else {
            TimeInForce::IOC
        };

        let order = Order::new(
            "".to_string(),
            id,
            contract.symbol.to_string(),
            OrderStatus::StandBy,
            open_order_type.clone(),
            side,
            time_in_force,
            units,
            leverage_factor,
            stop_loss_price,
            take_profit_price,
            avg_price,
            vec![],
            self.get_taker_fee(),
            balance_remainder,
            false,
            false,
            timestamp,
            timestamp,
        );
        Ok(order)
    }

    fn get_ws_ping_interval(&self) -> u64 {
        20
    }

    fn get_ws_ping_message(&self) -> Result<Message, GlowError> {
        let ping_request = PingWsMessage {
            req_id: String::from("100001"),
            op: String::from("ping"),
        };
        let ping_json_str = to_json_string(&ping_request)?;
        Ok(Message::Text(ping_json_str))
    }

    async fn update_position_data_on_faulty_exchange_ws(&self) -> Result<(), GlowError> {
        // updates balance. TODO: check if it's convenient to separate this in a new thread
        let balance = self
            .fetch_current_usdt_balance()
            .await
            .expect("set_current_balance_handle -> get_current_usdt_balance error");
        // let update_action = UpdateAction::Balance(balance);
        self.update_balance_listener.next(Some(balance));

        let mut last_error_ts = None;
        {
            let last_error_guard = self.last_ws_error_ts.lock().unwrap();
            if let Some(last_error) = *last_error_guard {
                last_error_ts = Some(last_error);
            }
        }

        let current_trade = self.current_trade_listener.value();
        // check for last_error_ts
        if let None = last_error_ts {
            // get open trade and set if any is open
            let trade = self.fetch_current_trade_position().await?;
            if trade.is_some() {
                println!(
                    "{:?} | A initial trade was found! {:?}",
                    current_datetime(),
                    trade.clone().unwrap()
                );
            }
            self.current_trade_listener.next(trade);
            return Ok(());
        }
        let last_error_ts = last_error_ts.unwrap();
        println!(
            "{} | update_position_data_on_faulty_exchange_ws -> last error ts = {}",
            current_timestamp_ms(),
            last_error_ts
        );
        // if it exists, check for current trade
        if let Some(current_trade) = current_trade {
            let current_trade_status = current_trade.status();
            match current_trade_status {
                TradeStatus::PendingCloseOrder | TradeStatus::Closed | TradeStatus::Cancelled => {
                    // in this case, no close order was opened (as when a close order is open, current_trade is updated as soon as it finishes http query)
                    // also, it doesn't make sense to fetch data from finished trades (closed/cancelled)
                }
                _ => {
                    let current_order_uuid = current_trade.get_active_order_uuid()
                            .expect(&format!("update_position_data_on_faulty_exchange_ws -> missing current order uuid. trade = {:?}", &current_trade));
                    let start_timestamp = last_error_ts;
                    let current_minute_end_timestamp = timestamp_minute_end(true, None);

                    let interim_executions = match self
                        .fetch_order_executions(
                            current_order_uuid,
                            start_timestamp,
                            current_minute_end_timestamp,
                        )
                        .await
                    {
                        Ok(executions) => executions,
                        Err(error) => {
                            println!("update_position_data_on_faulty_exchange_ws -> executions error {:?}", error);
                            vec![]
                        }
                    };

                    let current_trade_status = current_trade.status();
                    // if executions were updated, order was too, but in case order could be cancelled (OrderStatus::New), we check for order updates
                    if interim_executions.len() > 0 || current_trade_status == TradeStatus::New {
                        // if executions were updated, we emit new executions action
                        if interim_executions.len() > 0 {
                            // let update_action = UpdateAction::Executions(interim_executions);
                            self.update_executions_listener.next(interim_executions);
                        }
                        let current_order_id = current_trade.get_active_order_id()
                                .expect(&format!("update_position_data_on_faulty_exchange_ws -> missing current order id. trade = {:?}", &current_trade));

                        // get order without executions
                        match self
                            .fetch_current_order(current_order_id.clone(), false)
                            .await
                        {
                            Ok(updated_order) => {
                                let order_action = if updated_order.is_cancel_order() {
                                    OrderAction::Cancel(updated_order)
                                } else {
                                    OrderAction::Update(updated_order)
                                };
                                // let update_action = UpdateAction::Order(Some(order_action));
                                self.update_order_listener.next(Some(order_action));
                            }
                            Err(error) => {
                                println!("handle_exchange_websocket -> fetch_opened_order failed {:?}. Error = {:?}", current_trade_status, error);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn init(&mut self) -> Result<(), GlowError> {
        let url = self.get_ws_url()?;

        loop {
            let connection = connect_async(url.clone()).await;
            if let Err(error) = connection {
                eprintln!(
                    "Exchange WebSocket connection failed. \n
                    Error: {} \n
                    Retrying in {} seconds...",
                    error.to_string(),
                    WS_RECONNECT_INTERVAL_IN_SECS
                );

                sleep(Duration::from_secs(WS_RECONNECT_INTERVAL_IN_SECS)).await;
                continue;
            }

            let (wss, resp) = connection.unwrap();
            eprintln!("Exchange connection stablished. \n Response: {:?}", resp);
            if let Err(err) = self.listen_messages(wss).await {
                let mut last_error_guard = self
                    .last_ws_error_ts
                    .lock()
                    .expect("handle_websocket -> last_error_guard unwrap");
                let error_timestamp = current_timestamp_ms();
                *last_error_guard = Some(error_timestamp);

                eprintln!(
                    "{:?} | Exchange websocket connection error: {:?}. Retrying...",
                    current_timestamp_ms(),
                    err
                );
            }
        }
    }

    async fn listen_messages(
        &mut self,
        mut wss: WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<(), GlowError> {
        self.auth_ws(&mut wss).await?;
        self.subscribe_ws(&mut wss).await?;
        let ping_interval = self.get_ws_ping_interval();
        let message = self.get_ws_ping_message()?;
        let exchange_wss_ping_interval_and_message = (ping_interval, message);

        let mut heartbeat_interval: Interval = interval(Duration::from_secs(
            exchange_wss_ping_interval_and_message.0,
        ));

        let mut sleep_deadline = Instant::now();
        let mut timeout_executed = false;

        loop {
            select! {
                ws_message = wss.next() => {
                    let message = ws_message.unwrap()?;

                    match message {
                        Message::Text(json) => {
                            let json = json.to_string();
                            let _ = self.process_ws_message(&json);
                        },
                        Message::Ping(_) => {
                            println!("received ping request");
                            wss.send(Message::Pong(vec![])).await?
                        },
                        fallback => {
                            println!("exchange fallback message {:?}", fallback);
                        }
                    }
                },
                // TODO: check if it's convenient to separate these in a new thread
                _ = sleep_until(sleep_deadline) => {
                    sleep_deadline = Instant::now() + Duration::from_secs(SECONDS_IN_MIN as u64);
                    if timeout_executed {
                        continue;
                    }

                    let _ = self.update_position_data_on_faulty_exchange_ws().await;

                    {
                        let mut last_error_guard = self.last_ws_error_ts.lock().unwrap();
                        *last_error_guard = None;
                    }

                    timeout_executed = true;
                }
                _ = heartbeat_interval.tick() => {
                    let ping_message = exchange_wss_ping_interval_and_message.1.clone();
                    let _send = wss.send(ping_message).await?;
                }
            }
        }
    }
}

impl BenchmarkExchange for BybitTraderExchange {
    fn new_benchmark_open_order(
        &self,
        timestamp: i64,
        side: Side,
        order_cost: f64,
        price: f64,
    ) -> Result<Order, GlowError> {
        let trading_settings = self.get_trading_settings();
        let leverage_factor = trading_settings.leverage.get_factor();
        let open_order_type = trading_settings.get_open_order_type();
        let (units, balance_remainder) =
            self.calculate_open_order_units_and_balance_remainder(side, order_cost, price)?;

        let ((open_fee, _), fee_rate, is_maker) =
            self.calculate_order_fees(open_order_type, side, units, price);

        let contract = self.get_traded_contract();

        let id = format!(
            "{}_{}_{}",
            &contract.symbol,
            timestamp,
            OrderStage::Open.to_string()
        );

        let avg_price = Some(price);

        let stop_loss_price = self.calculate_order_stop_loss_price(side, price);

        let take_profit_price = self.calculate_order_take_profit_price(side, price);

        let time_in_force = if open_order_type == OrderType::Limit {
            TimeInForce::GTC
        } else {
            TimeInForce::IOC
        };

        let order_uuid = format!("benchmark_open_order_{}", timestamp);

        let benchmark_execution = Execution::new(
            id.clone(),
            order_uuid.clone(),
            open_order_type,
            timestamp,
            price,
            units,
            open_fee,
            fee_rate,
            is_maker,
            0.0,
        );

        let order = Order::new(
            order_uuid,
            id,
            contract.symbol.to_string(),
            OrderStatus::Filled,
            open_order_type,
            side,
            time_in_force,
            units,
            leverage_factor,
            stop_loss_price,
            take_profit_price,
            avg_price,
            vec![benchmark_execution],
            self.get_taker_fee(),
            balance_remainder,
            false,
            false,
            timestamp,
            timestamp,
        );
        Ok(order)
    }

    fn new_benchmark_close_order(
        &self,
        timestamp: i64,
        trade_id: &String,
        close_price: f64,
        open_order: Order,
        final_status: OrderStatus,
    ) -> Result<Order, GlowError> {
        let trading_settings = self.get_trading_settings();
        let close_order_type = match final_status {
            OrderStatus::StoppedBR
            | OrderStatus::StoppedSL
            | OrderStatus::StoppedTP
            | OrderStatus::StoppedTSL => trading_settings.get_close_order_type(),
            _ => OrderType::Market,
        };
        let id = format!("{}_{}", trade_id, OrderStage::Close.to_string());
        let time_in_force = if close_order_type == OrderType::Market {
            TimeInForce::IOC
        } else {
            TimeInForce::GTC
        };

        let close_side = open_order.side.get_opposite_side()?;

        let ((_, close_fee), fee_rate, is_maker) =
            self.calculate_order_fees(close_order_type, close_side, open_order.units, close_price);

        let order_uuid = format!("pending_order_uuid_{}", timestamp);

        let benchmark_execution = Execution::new(
            id.clone(),
            order_uuid.clone(),
            close_order_type,
            timestamp,
            close_price,
            open_order.units,
            close_fee,
            fee_rate,
            is_maker,
            open_order.units,
        );

        let executions = vec![benchmark_execution];

        let avg_price = Some(close_price);

        let is_stop = final_status == OrderStatus::StoppedBR
            || final_status == OrderStatus::StoppedSL
            || final_status == OrderStatus::StoppedTP
            || final_status == OrderStatus::StoppedTSL;

        let order = Order::new(
            order_uuid,
            id,
            open_order.symbol.clone(),
            final_status,
            close_order_type,
            close_side,
            time_in_force,
            open_order.units,
            open_order.leverage_factor,
            None,
            None,
            avg_price,
            executions,
            0.0,
            0.0,
            true,
            is_stop,
            timestamp,
            timestamp,
        );

        Ok(order)
    }

    fn check_price_level_modifiers(
        &self,
        trade: &Trade,
        current_timestamp: i64,
        close_price: f64,
        stop_loss: Option<&PriceLevel>,
        take_profit: Option<&PriceLevel>,
        trailing_stop_loss: Option<&PriceLevel>,
        current_peak_returns: f64,
    ) -> Result<Option<Trade>, GlowError> {
        if trade.open_order.leverage_factor > 1.0 {
            let bankruptcy_price = trade
                .open_order
                .get_bankruptcy_price()
                .expect("check_price_level_modifiers -> get_bankruptcy_price unwrap");

            if trade.open_order.side == Side::Sell && bankruptcy_price <= close_price
                || trade.open_order.side == Side::Buy && bankruptcy_price >= close_price
            {
                let close_order = self.new_benchmark_close_order(
                    current_timestamp,
                    &trade.id,
                    close_price,
                    trade.open_order.clone(),
                    OrderStatus::StoppedBR,
                )?;

                let closed_trade = trade.update_trade(close_order)?;
                return Ok(Some(closed_trade));
            }
        }
        if stop_loss.is_some() || take_profit.is_some() || trailing_stop_loss.is_some() {
            let (_, returns) = trade.calculate_unrealized_pnl_and_returns(close_price);
            if let Some(stop_loss) = stop_loss {
                let stop_loss_percentage = stop_loss.get_percentage();
                if returns < 0.0 && stop_loss_percentage <= returns.abs() {
                    println!(
                        "Stop loss took effect: returns = {}, stop loss percentage = {}",
                        returns, stop_loss_percentage
                    );
                    let close_order = self.new_benchmark_close_order(
                        current_timestamp,
                        &trade.id,
                        close_price,
                        trade.open_order.clone(),
                        OrderStatus::StoppedSL,
                    )?;

                    let closed_trade = trade.update_trade(close_order)?;
                    return Ok(Some(closed_trade));
                }
            }

            if let Some(take_profit) = take_profit {
                let take_profit_percentage = take_profit.get_percentage();
                if returns > 0.0 && take_profit_percentage <= returns.abs() {
                    println!(
                        "Take profit took effect: returns = {}, take profit percentage = {}",
                        returns, take_profit_percentage
                    );
                    let close_order = self.new_benchmark_close_order(
                        current_timestamp,
                        &trade.id,
                        close_price,
                        trade.open_order.clone(),
                        OrderStatus::StoppedTP,
                    )?;

                    let closed_trade = trade.update_trade(close_order)?;
                    return Ok(Some(closed_trade));
                }
            }

            if let Some(trailing_stop_loss) = trailing_stop_loss {
                match trailing_stop_loss {
                    PriceLevel::TrailingStopLoss(tsl) => match tsl {
                        TrailingStopLoss::Percent(percentage, start_percentage) => {
                            if start_percentage < &current_peak_returns {
                                let acceptable_returns = percentage * current_peak_returns;
                                let acceptable_returns = acceptable_returns.max(*start_percentage);

                                if returns <= acceptable_returns {
                                    let close_order = self.new_benchmark_close_order(
                                        current_timestamp,
                                        &trade.id,
                                        close_price,
                                        trade.open_order.clone(),
                                        OrderStatus::StoppedTSL,
                                    )?;

                                    let closed_trade = trade.update_trade(close_order)?;
                                    return Ok(Some(closed_trade));
                                }
                            }
                        }
                        TrailingStopLoss::Stepped(percentage, start_percentage) => {
                            if start_percentage < &current_peak_returns {
                                let acceptable_returns =
                                    closest_multiple_below(*percentage, current_peak_returns);
                                let acceptable_returns = acceptable_returns.max(*start_percentage);
                                // println!(
                                //     "@@@ start_percentage {} current returns {}, acceptable_returns {}",
                                //     start_percentage, returns, acceptable_returns
                                // );
                                if returns <= acceptable_returns {
                                    let close_order = self.new_benchmark_close_order(
                                        current_timestamp,
                                        &trade.id,
                                        close_price,
                                        trade.open_order.clone(),
                                        OrderStatus::StoppedTP,
                                    )?;

                                    let closed_trade = trade.update_trade(close_order)?;
                                    return Ok(Some(closed_trade));
                                }
                            }
                        }
                    },
                    _ => {}
                }
            }
        }

        Ok(None)
    }
}
