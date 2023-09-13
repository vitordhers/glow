use crate::trader::constants::{API_KEY_ENV_SUFFIX, API_SECRET_ENV_SUFFIX};
use crate::trader::enums::balance::Balance;
use crate::trader::enums::http_method::HttpMethod;
use crate::trader::enums::modifiers::leverage::Leverage;
use crate::trader::enums::modifiers::position_lock::PositionLock;
use crate::trader::enums::order_stage::OrderStage;
use crate::trader::enums::order_status::OrderStatus;
use crate::trader::enums::order_type::OrderType;
use crate::trader::enums::processer_action::ProcesserAction;
use crate::trader::enums::side::Side;
use crate::trader::enums::time_in_force::TimeInForce;
use crate::trader::enums::trade_status::TradeStatus;
use crate::trader::errors::{CustomError, Error};
use crate::trader::exchanges::bybit::enums::AccountType;
use crate::trader::exchanges::bybit::models::{
    BybitHttpResponseWrapper, CancelOrderDto, CreateOrderDto, GetWalletBalanceDto, HttpResultList,
    WalletResponseData,
};
use crate::trader::functions::{
    calculate_hmac, calculate_remainder, count_decimal_places, round_down_nth_decimal,
};
use crate::trader::models::contract::Contract;
use crate::trader::models::execution::Execution;
use crate::trader::models::order::Order;
use crate::trader::models::trade::Trade;
use crate::trader::traits::exchange::Exchange;
use crate::trader::traits::ws_processer::WsProcesser;

use async_trait::async_trait;
use chrono::Utc;
use futures_util::SinkExt;
use reqwest::{Client, RequestBuilder};
use serde::Serialize;
use serde_json::{from_str, Value};
use serde_urlencoded::to_string;
use std::collections::HashMap;
use std::env::{self};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

use self::enums::BybitWsResponse;
use self::models::{
    AmendOrderDto, EmptyObject, ExecutionResponseData, FetchExecutionsDto, FetchHistoryOrderDto,
    FetchOpenedOrderDto, FetchPositionDto, OrderResponse, OrderResponseData, PositionResponseData,
    SetLeverageDto, WsRequest,
};
pub mod enums;
pub mod functions;
mod models;
use reqwest::header;
pub mod tests;

#[derive(Debug, Clone)]
pub struct BybitExchange {
    pub name: String,
    pub contracts: HashMap<String, Contract>,
    anchor_contract_symbol: String,
    traded_contract_symbol: String,
    pub maker_fee_rate: f64,
    pub taker_fee_rate: f64,
    http_url: String,
    ws_url: String,
    client: Client,
    api_key: String,
    api_secret: String,
}

impl BybitExchange {
    pub fn new(
        name: String,
        contracts: HashMap<String, Contract>,
        anchor_contract_symbol: String,
        traded_contract_symbol: String,
        maker_fee_rate: f64,
        taker_fee_rate: f64,
    ) -> Self {
        let env = env::var("ENV_NAME").unwrap().to_uppercase();
        let http_url: String;
        let ws_url: String;
        let exchange_title = name.to_uppercase();
        let api_key =
            env::var(format!("{}_{}_{}", exchange_title, API_KEY_ENV_SUFFIX, env)).unwrap();
        let api_secret = env::var(format!(
            "{}_{}_{}",
            exchange_title, API_SECRET_ENV_SUFFIX, env
        ))
        .unwrap();

        if env == "PROD".to_string() {
            http_url = env::var("BYBIT_HTTP_BASE_URL_PROD").unwrap();
            ws_url = env::var("BYBIT_WS_BASE_URL_PROD").unwrap();
        } else {
            http_url = env::var("BYBIT_HTTP_BASE_URL_DEV").unwrap();
            ws_url = env::var("BYBIT_WS_BASE_URL_DEV").unwrap();
        }

        Self {
            name,
            contracts,
            anchor_contract_symbol,
            traded_contract_symbol,
            maker_fee_rate,
            taker_fee_rate,
            http_url,
            ws_url,
            client: Client::new(),
            api_key,
            api_secret,
        }
    }

    fn get_signature(
        &self,
        method: HttpMethod,
        api_key: &String,
        timestamp: i64,
        recv_window: i32,
        payload: impl Serialize,
    ) -> Result<String, Error> {
        match method {
            HttpMethod::Get => {
                // const queryString = ts.toString()+pm.environment.get("bybit-api-key")+pm.environment.get("recvWindow").toString()+pm.request.url.query;
                let query_params = to_string(&payload).unwrap();
                let query_string =
                    format!("{}{}{}{}", timestamp, api_key, recv_window, query_params);
                let signature = calculate_hmac(&self.api_secret, &query_string)?;
                Ok(signature)
            }
            HttpMethod::Post => {
                //  queryString=ts.toString()+pm.environment.get("bybit-api-key")+pm.environment.get("recvWindow").toString()+pm.request.body.raw;
                let body_raw = serde_json::to_string(&payload).unwrap();
                let query_string = format!("{}{}{}{}", timestamp, api_key, recv_window, body_raw);
                let signature = calculate_hmac(&self.api_secret, &query_string)?;
                Ok(signature)
            }
        }
    }

    fn append_request_headers(
        &self,
        request_builder: RequestBuilder,
        api_key: String,
        timestamp: i64,
        signature: String,
        recv_window: i32,
    ) -> RequestBuilder {
        request_builder
            .header("X-BAPI-SIGN-TYPE", 2)
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-API-KEY", api_key)
            .header("X-BAPI-TIMESTAMP", timestamp)
            .header("X-BAPI-RECV-WINDOW", recv_window)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONNECTION, "keep-alive")
            .header(header::ACCEPT_ENCODING, "gzip, deflate, br")
            .header(header::ACCEPT, "application/json")
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Ping {
    req_id: String,
    op: String,
}

#[async_trait]
impl Exchange for BybitExchange {
    fn clone_box(&self) -> Box<dyn Exchange + Send + Sync> {
        Box::new(Self {
            name: self.name.clone(),
            contracts: self.contracts.clone(),
            anchor_contract_symbol: self.anchor_contract_symbol.clone(),
            traded_contract_symbol: self.traded_contract_symbol.clone(),
            maker_fee_rate: self.maker_fee_rate,
            taker_fee_rate: self.taker_fee_rate,
            http_url: self.http_url.clone(),
            ws_url: self.ws_url.clone(),
            client: self.client.clone(),
            api_key: self.api_key.clone(),
            api_secret: self.api_secret.clone(),
        })
    }

    fn get_taker_fee(&self) -> f64 {
        self.taker_fee_rate
    }

    fn get_maker_fee(&self) -> f64 {
        self.maker_fee_rate
    }

    fn get_anchor_contract(&self) -> &Contract {
        self.contracts.get(&self.anchor_contract_symbol).unwrap()
    }

    fn get_traded_contract(&self) -> &Contract {
        self.contracts.get(&self.traded_contract_symbol).unwrap()
    }

    fn get_current_symbols(&self) -> Vec<String> {
        let mut symbols = vec![self.anchor_contract_symbol.clone()];
        if self.traded_contract_symbol != self.anchor_contract_symbol {
            symbols.push(self.traded_contract_symbol.clone());
        }
        symbols
    }

    fn get_http_client(&self) -> &Client {
        &self.client
    }

    fn get_ws_url(&self) -> Result<Url, Error> {
        let url = Url::parse(&format!("{}/v5/private", self.ws_url))?;
        Ok(url)
    }

    async fn auth_ws(
        &self,
        wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<(), Error> {
        let timestamp = Utc::now().timestamp();
        let expires = (timestamp + 5) * 1000;
        let expires = expires.to_string();
        let message = format!("GET/realtime{}", expires);
        let signature = calculate_hmac(&self.api_secret, &message).unwrap();
        let args = vec![self.api_key.clone(), expires, signature];
        let auth_request = WsRequest::new("auth".to_string(), args);
        let auth_json_str = serde_json::to_string(&auth_request)
            .expect(&format!("JSON ({:?}) parsing error", auth_request));
        let auth_message = Message::Text(auth_json_str);
        wss.send(auth_message).await?;
        Ok(())
    }
    async fn subscribe_ws(
        &self,
        wss: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<(), Error> {
        let subscribe_request = WsRequest::new(
            "subscribe".to_string(),
            vec![
                "execution".to_string(),
                "order".to_string(),
                "wallet".to_string(),
            ],
        );

        let subscription_json = serde_json::to_string(&subscribe_request)
            .expect(&format!("JSON ({:?}) parsing error", subscribe_request));

        let subscription_message = Message::Text(subscription_json);

        wss.send(subscription_message).await?;
        Ok(())
    }

    fn get_processer(&self) -> Box<dyn WsProcesser + Send + Sync + 'static> {
        let traded_symbol = self.get_traded_contract().symbol.clone();
        Box::new(BybitWsProcesser::new(traded_symbol))
    }

    async fn fetch_order_executions(
        &self,
        order_uuid: String,
        start_timestamp: i64,
        end_timestamp: i64,
    ) -> Result<Vec<Execution>, Error> {
        let http = self.get_http_client();
        let timestamp = Utc::now().timestamp() * 1000;
        let recv_window = 5000;
        let traded_symbol = self.get_traded_contract().symbol.clone();
        let payload = FetchExecutionsDto {
            category: "linear".to_string(),
            order_uuid,
            symbol: traded_symbol,
            start_timestamp,
            end_timestamp,
        };

        let signature = self.get_signature(
            HttpMethod::Get,
            &self.api_key,
            timestamp,
            recv_window,
            payload.clone(),
        )?;

        let query_params_str = to_string(&payload)?;
        let url = format!("{}/v5/execution/list?{}", self.http_url, query_params_str);

        let mut request_builder = http.get(url.clone());
        request_builder = self.append_request_headers(
            request_builder,
            self.api_key.clone(),
            timestamp,
            signature,
            recv_window,
        );

        let result = request_builder.send().await;
        match result {
            Ok(response) => {
                if response.status().is_success() {
                    match response.text().await {
                        Ok(response_json) => {
                            match serde_json::from_str::<
                                BybitHttpResponseWrapper<HttpResultList<ExecutionResponseData>>,
                            >(&response_json)
                            {
                                Ok(parsed_response) => {
                                    if parsed_response.ret_code == 0
                                        && parsed_response.ret_message == "OK".to_string()
                                    {
                                        let executions: Vec<Execution> = parsed_response
                                            .result
                                            .list
                                            .into_iter()
                                            .map(|execution_response| execution_response.into())
                                            .collect();
                                        Ok(executions)
                                    } else {
                                        Ok(vec![])
                                    }
                                }
                                Err(error) => {
                                    println!(
                                        "EXECUTIONS SERDE ERROR {:?}, JSON = {:?}",
                                        error, response_json
                                    );
                                    Err(Error::SerdeError(error))
                                }
                            }
                        }
                        Err(error) => {
                            println!("EXECUTIONS REQWEST ERROR {:?}", error);
                            Err(Error::ReqwestError(error))
                        }
                    }
                } else {
                    let error = format!(
                        "fetch_order_executions -> unsucessful response {:?}",
                        response
                    );
                    Err(Error::CustomError(CustomError::new(error)))
                }
            }
            Err(error) => {
                let formmated_error =
                    format!("fetch_order_executions -> query failed -> {:?}", error);
                Err(Error::CustomError(CustomError::new(formmated_error)))
            }
        }
    }

    async fn fetch_history_order(
        &self,
        id: Option<String>,
        side: Option<Side>,
        fetch_executions: bool,
    ) -> Result<Order, Error> {
        let http = self.get_http_client();
        let timestamp = Utc::now().timestamp() * 1000;
        let recv_window = 5000;
        let traded_symbol = self.get_traded_contract().symbol.clone();
        let payload = FetchHistoryOrderDto {
            category: "linear".to_string(),
            id: id.clone(),
            side,
            symbol: traded_symbol,
        };

        let signature = self.get_signature(
            HttpMethod::Get,
            &self.api_key,
            timestamp,
            recv_window,
            payload.clone(),
        )?;

        let query_params_str = to_string(&payload)
            .expect("fetch_history_order -> error parsing payload to query params");
        let url = format!("{}/v5/order/history?{}", self.http_url, query_params_str);

        let mut request_builder = http.get(url.clone());
        request_builder = self.append_request_headers(
            request_builder,
            self.api_key.clone(),
            timestamp,
            signature,
            recv_window,
        );

        let result = request_builder.send().await;
        match result {
            Ok(response) => {
                if response.status().is_success() {
                    let mut parsed_response: BybitHttpResponseWrapper<
                        HttpResultList<OrderResponseData>,
                    > = response
                        .json()
                        .await
                        .expect("fetch_history_order -> parsed json response error");
                    if parsed_response.ret_code == 0
                        && parsed_response.ret_message == "OK".to_string()
                    {
                        let order_response_opt = if id.clone().is_some() {
                            parsed_response.result.list.into_iter().find(|order_data| {
                                !order_data.is_cancel() && !order_data.is_trigger_order()
                            })
                        } else {
                            parsed_response
                                .result
                                .list
                                .sort_by(|a, b| b.updated_time.cmp(&a.updated_time));
                            parsed_response.result.list.into_iter().find(|order_data| {
                                !order_data.is_cancel() && !order_data.is_trigger_order()
                            })
                        };

                        if let Some(order_response) = order_response_opt {
                            let executed_qty = order_response.cum_exec_qty;
                            let mut order: Order = order_response.into();
                            let mut executions = vec![];
                            if fetch_executions && executed_qty > 0.0 {
                                let fetched_executions = self
                                    .fetch_order_executions(
                                        order.uuid.clone(),
                                        order.created_at,
                                        order.updated_at,
                                    )
                                    .await?;
                                executions.extend(fetched_executions);
                            }
                            order = order.push_executions_if_new(executions);
                            Ok(order)
                        } else {
                            Err(Error::CustomError(CustomError::new(
                                "fetch_history_order -> no closed order was found".to_string(),
                            )))
                        }
                    } else {
                        Err(Error::CustomError(CustomError::new(format!(
                            "fetch_history_order -> unexpected order response -> {:?}",
                            parsed_response
                        ))))
                    }
                } else {
                    let error =
                        format!("fetch_history_order -> unsucessful response {:?}", response);
                    Err(Error::CustomError(CustomError::new(error)))
                }
            }
            Err(error) => {
                let formmated_error = format!("fetch_history_order -> query failed -> {:?}", error);
                Err(Error::CustomError(CustomError::new(formmated_error)))
            }
        }
    }

    async fn fetch_opened_order(
        &self,
        order_id: String,
        fetch_executions: bool,
    ) -> Result<Order, Error> {
        let http = self.get_http_client();
        let timestamp = Utc::now().timestamp() * 1000;
        let recv_window = 5000;
        let traded_symbol = self.get_traded_contract().symbol.clone();
        let payload = FetchOpenedOrderDto {
            category: "linear".to_string(),
            id: order_id.clone(),
            symbol: traded_symbol,
            open_only: 2,
        };

        let signature = self.get_signature(
            HttpMethod::Get,
            &self.api_key,
            timestamp,
            recv_window,
            payload.clone(),
        )?;

        let query_params_str = to_string(&payload)
            .expect("fetch_opened_order -> error parsing payload to query params");
        let url = format!("{}/v5/order/realtime?{}", self.http_url, query_params_str);

        let mut request_builder = http.get(url.clone());
        request_builder = self.append_request_headers(
            request_builder,
            self.api_key.clone(),
            timestamp,
            signature,
            recv_window,
        );

        let result = request_builder.send().await;
        match result {
            Ok(response) => {
                if response.status().is_success() {
                    match response.text().await {
                        Ok(response_json) => match from_str::<
                            BybitHttpResponseWrapper<HttpResultList<OrderResponseData>>,
                        >(&response_json)
                        {
                            Ok(parsed_response) => {
                                if parsed_response.ret_code == 0
                                    && parsed_response.ret_message == "OK".to_string()
                                {
                                    let order_response_opt =
                                        parsed_response.result.list.into_iter().find(
                                            |order_data| {
                                                println!("@@@@ fetch_opened_order. order_data.order_link_id = {}, order_data.is_trigger_order() = {}", order_data.order_link_id, order_data.is_trigger_order());
                                                order_data.order_link_id == order_id.clone()
                                                    && !order_data.is_trigger_order()
                                            },
                                        );

                                    if let Some(order_response) = order_response_opt {
                                        let mut executions = vec![];
                                        let executed_qty = order_response.cum_exec_qty;

                                        let mut order: Order = order_response.into();

                                        if fetch_executions && executed_qty > 0.0 {
                                            let fetched_executions = self
                                                .fetch_order_executions(
                                                    order.uuid.clone(),
                                                    order.created_at,
                                                    order.updated_at,
                                                )
                                                .await?;
                                            executions.extend(fetched_executions);
                                        }
                                        order = order.push_executions_if_new(executions);
                                        Ok(order)
                                    } else {
                                        let mut retries = 2;
                                        let error = format!(
                                            "fetch_opened_order -> order wasn't opened at all"
                                        );

                                        let mut result =
                                            Err(Error::CustomError(CustomError::new(error)));
                                        while retries > 0 {
                                            match self
                                                .fetch_history_order(
                                                    Some(order_id.clone()),
                                                    None,
                                                    fetch_executions,
                                                )
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
                                        result
                                    }
                                } else {
                                    let error = format!(
                                        "fetch_opened_order -> inadequate order response -> {:?}",
                                        parsed_response
                                    );
                                    Err(Error::CustomError(CustomError::new(error)))
                                }
                            }
                            Err(error) => Err(Error::CustomError(CustomError::new(format!(
                                "fetch_opened_order -> failed to parse response from_str. response_json -> {:?}, error = {:?}",
                                response_json,
                                error
                            )))),
                        },
                        Err(error) => Err(Error::CustomError(CustomError::new(format!(
                            "fetch_opened_order -> failed to parse response from_str. Error {:?}",
                            error,
                        )))),
                    }
                } else {
                    let error = format!(
                        "fetch_opened_order -> unsuccessful response -> {:?}",
                        response
                    );
                    Err(Error::CustomError(CustomError::new(error)))
                }
            }
            Err(error) => Err(Error::CustomError(CustomError::new(format!(
                "fetch_opened_order -> query failed -> {:?}",
                error
            )))),
        }
    }

    async fn fetch_current_position_trade(&self) -> Result<Option<Trade>, Error> {
        let http = self.get_http_client();
        let timestamp = Utc::now().timestamp() * 1000;
        let recv_window = 5000;
        let traded_symbol = self.get_traded_contract().symbol.clone();
        let payload = FetchPositionDto {
            category: "linear".to_string(),
            symbol: traded_symbol.clone(),
        };

        let signature = self.get_signature(
            HttpMethod::Get,
            &self.api_key,
            timestamp,
            recv_window,
            payload.clone(),
        )?;

        let query_params_str = to_string(&payload)
            .expect("fetch_current_position_trade -> error parsing payload to query params");
        let url = format!("{}/v5/position/list?{}", self.http_url, query_params_str);

        let mut request_builder = http.get(url.clone());
        request_builder = self.append_request_headers(
            request_builder,
            self.api_key.clone(),
            timestamp,
            signature,
            recv_window,
        );
        // fetch position
        let result = request_builder.send().await;
        match result {
            Ok(response) => {
                if response.status().is_success() {
                    let parsed_response: BybitHttpResponseWrapper<
                        HttpResultList<PositionResponseData>,
                    > = response.json().await?;

                    let position_response_opt = parsed_response.result.list.into_iter().next();

                    if let Some(position) = position_response_opt {
                        if position.side == Side::None {
                            return Ok(None);
                        } else {
                            let latest_order = self
                                .fetch_history_order(None, Some(position.side), true)
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
                                match self.fetch_opened_order(close_order_id, true).await {
                                    Ok(order) => {
                                        close_order = Some(order);
                                    }
                                    Err(_) => {
                                        close_order = None;
                                    }
                                }
                            }

                            let trade = Trade::new(
                                &traded_symbol,
                                position.leverage,
                                open_order,
                                close_order,
                            );

                            Ok(Some(trade))
                        }
                    } else {
                        Ok(None)
                    }
                } else {
                    let error = format!(
                        "fetch_current_position_trade -> unsuccessful response -> {:?}",
                        response
                    );
                    Err(Error::CustomError(CustomError::new(error)))
                }
            }
            Err(error) => {
                let formmated_error = format!(
                    "fetch_current_position_trade -> query failed -> {:?}",
                    error
                );
                Err(Error::CustomError(CustomError::new(formmated_error)))
            }
        }
    }

    /// automatically fetches respective orders and executions
    async fn fetch_trade_state(
        &self,
        trade_id: String,
        last_status: TradeStatus,
    ) -> Result<Trade, Error> {
        let http = self.get_http_client();
        let timestamp = Utc::now().timestamp() * 1000;
        let recv_window = 5000;
        let traded_symbol = self.get_traded_contract().symbol.clone();
        let payload = FetchPositionDto {
            category: "linear".to_string(),
            symbol: traded_symbol.clone(),
        };

        let signature = self.get_signature(
            HttpMethod::Get,
            &self.api_key,
            timestamp,
            recv_window,
            payload.clone(),
        )?;

        let query_params_str = to_string(&payload)
            .expect("fetch_trade_state -> error parsing payload to query params");
        let url = format!("{}/v5/position/list?{}", self.http_url, query_params_str);

        let mut request_builder = http.get(url.clone());
        request_builder = self.append_request_headers(
            request_builder,
            self.api_key.clone(),
            timestamp,
            signature,
            recv_window,
        );

        let result = request_builder.send().await;
        match result {
            Ok(response) => {
                if response.status().is_success() {
                    let parsed_response: BybitHttpResponseWrapper<
                        HttpResultList<PositionResponseData>,
                    > = response.json().await?;

                    let position_response_opt = parsed_response.result.list.into_iter().next();

                    if let Some(position) = position_response_opt {
                        // if position.side == Side::None {
                        //     return Ok(None);
                        // }
                        let open_order_id = format!("{}_open", trade_id);
                        let close_order_id = format!("{}_close", trade_id);

                        let open_order = self.fetch_opened_order(open_order_id, true).await?;
                        let close_order: Option<Order>;

                        match last_status {
                            TradeStatus::Cancelled => {
                                close_order = None;
                            }
                            TradeStatus::New => match open_order.status {
                                OrderStatus::Cancelled
                                | OrderStatus::StandBy
                                | OrderStatus::PartiallyFilled => {
                                    close_order = None;
                                }
                                OrderStatus::Filled => {
                                    match self.fetch_opened_order(close_order_id, true).await {
                                        Ok(opened_close_order) => {
                                            close_order = Some(opened_close_order);
                                        }
                                        Err(_) => {
                                            close_order = None;
                                        }
                                    }
                                }
                                OrderStatus::PartiallyClosed | OrderStatus::Closed => {
                                    let error = format!(
                                        r#"fetch_trade_state -> TradeStatus::New -> open order has OrderStatus::PartiallyClosed | OrderStatus::Closed status {:?}"#,
                                        open_order
                                    );
                                    return Err(Error::CustomError(CustomError::new(error)));
                                }
                            },
                            TradeStatus::PartiallyOpen => match open_order.status {
                                OrderStatus::Cancelled | OrderStatus::StandBy => {
                                    let error = format!(
                                        r#"fetch_trade_state -> TradeStatus::PartiallyOpen -> open order has PartiallyClosed/Closed status {:?}"#,
                                        open_order
                                    );
                                    return Err(Error::CustomError(CustomError::new(error)));
                                }
                                OrderStatus::PartiallyFilled => {
                                    close_order = None;
                                }
                                OrderStatus::Filled => {
                                    match self.fetch_opened_order(close_order_id, true).await {
                                        Ok(opened_close_order) => {
                                            close_order = Some(opened_close_order);
                                        }
                                        Err(_) => {
                                            close_order = None;
                                        }
                                    }
                                }
                                OrderStatus::PartiallyClosed | OrderStatus::Closed => {
                                    let error = format!(
                                        r#"fetch_trade_state -> TradeStatus::PartiallyOpen -> open order has OrderStatus::PartiallyClosed | OrderStatus::Closed status {:?}"#,
                                        open_order
                                    );
                                    return Err(Error::CustomError(CustomError::new(error)));
                                }
                            },
                            TradeStatus::PendingCloseOrder => {
                                match self.fetch_opened_order(close_order_id, true).await {
                                    Ok(opened_close_order) => {
                                        close_order = Some(opened_close_order);
                                    }
                                    Err(_) => {
                                        close_order = None;
                                    }
                                }
                            }
                            TradeStatus::CloseOrderStandBy
                            | TradeStatus::PartiallyClosed
                            | TradeStatus::Closed => {
                                let opened_close_order =
                                    self.fetch_opened_order(close_order_id, true).await?;
                                close_order = Some(opened_close_order);
                            }
                        }

                        let trade =
                            Trade::new(&traded_symbol, position.leverage, open_order, close_order);

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
                    } else {
                        let error = format!(
                            r#"fetch_trade_state -> symbol {} doesn't have any open position"#,
                            traded_symbol
                        );
                        return Err(Error::CustomError(CustomError::new(error)));
                    }
                } else {
                    let error = format!(
                        "fetch_open_trade -> unsuccessful response -> {:?}",
                        response
                    );
                    Err(Error::CustomError(CustomError::new(error)))
                }
            }
            Err(error) => {
                let formatted_error = format!("fetch_open_trade -> query failed -> {:?}", error);
                Err(Error::CustomError(CustomError::new(formatted_error)))
            }
        }
    }

    async fn fetch_current_usdt_balance(&self) -> Result<Balance, Error> {
        let http = self.get_http_client();
        let timestamp = Utc::now().timestamp() * 1000;
        let recv_window = 5000;
        let payload: GetWalletBalanceDto =
            GetWalletBalanceDto::new(AccountType::Contract, Some("USDT".to_string()));

        let signature = self.get_signature(
            HttpMethod::Get,
            &self.api_key,
            timestamp,
            recv_window,
            payload.clone(),
        )?;

        let query_params_str = to_string(&payload)
            .expect("get_current_usdt_balance -> error parsing payload to query params");
        let url = format!(
            "{}/v5/account/wallet-balance?{}",
            self.http_url, query_params_str
        );

        let mut request_builder = http.get(url.clone());
        request_builder = self.append_request_headers(
            request_builder,
            self.api_key.clone(),
            timestamp,
            signature,
            recv_window,
        );

        let result = request_builder.send().await;
        match result {
            Ok(response) => {
                if response.status().is_success() {
                    // let response_text = response.text().await;
                    let parsed_response: BybitHttpResponseWrapper<
                        HttpResultList<WalletResponseData>,
                    > = response
                        .json()
                        .await
                        .expect("get_current_usdt_balance -> parsed json response error");

                    let usdt_coin_data =
                        parsed_response.result.list.iter().find_map(|wallet_data| {
                            wallet_data
                                .coin
                                .iter()
                                .find(|coin_data| coin_data.coin == "USDT".to_string())
                        });

                    let usdt_data =
                        usdt_coin_data.expect("get_current_usdt_balance -> missing usdt coin data");

                    let balance = Balance::new(
                        parsed_response.time,
                        usdt_data.available_to_withdraw,
                        usdt_data.wallet_balance,
                    );
                    Ok(balance)
                } else {
                    let error = format!(
                        "get_current_usdt_balance -> unsuccessful response -> {:?}",
                        response
                    );
                    Err(Error::CustomError(CustomError::new(error)))
                }
            }
            Err(error) => {
                println!("cancel_order -> request error: {:?}", error);
                Err(Error::CustomError(CustomError::new(
                    "get_current_usdt_balance -> query failed".to_string(),
                )))
            }
        }
    }

    async fn open_order(
        &self,
        side: Side,
        order_type: OrderType,
        amount: f64,
        expected_price: f64,
        leverage_factor: f64,
        stop_loss_percentage_opt: Option<f64>,
        take_profit_percentage_opt: Option<f64>,
    ) -> Result<Order, Error> {
        let traded_contract = self.get_traded_contract();
        let mut expected_price = expected_price;
        if order_type == OrderType::Limit {
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

        if expected_price <= 0.0 {
            let error = Error::CustomError(CustomError::new(format!(
                "open_order -> expected price is less than 0! "
            )));
            println!("{:?}", error);
            return Err(error);
        }

        let order = self.create_new_open_order(
            order_type,
            side,
            amount,
            leverage_factor,
            expected_price,
            stop_loss_percentage_opt,
            take_profit_percentage_opt,
        );

        if order.is_none() {
            let error = Error::CustomError(CustomError::new(format!(
                "open_order -> open order wasn't created!"
            )));

            println!("{:?}", error);
            return Err(error);
        }

        let mut order = order.unwrap();

        let http = self.get_http_client();
        let timestamp = Utc::now().timestamp() * 1000;
        let recv_window = 5000;
        let order_id = order.id.clone();
        let payload: CreateOrderDto = order.clone().into();

        let signature = self.get_signature(
            HttpMethod::Post,
            &self.api_key,
            timestamp,
            recv_window,
            payload.clone(),
        )?;

        let json_data = serde_json::to_string(&payload)?;
        let query_params_str =
            to_string(&payload).expect("open_order -> error parsing payload to query params");

        let url = format!("{}/v5/order/create?{}", self.http_url, query_params_str);

        let mut request_builder = http.post(url).body(json_data);
        request_builder = self.append_request_headers(
            request_builder,
            self.api_key.clone(),
            timestamp,
            signature,
            recv_window,
        );

        let result = request_builder.send().await;
        match result {
            Ok(response) => {
                if response.status().is_success() {
                    let json_response = response.text().await?;

                    match from_str::<BybitHttpResponseWrapper<OrderResponse>>(&json_response) {
                        Ok(parsed_response) => {
                            if parsed_response.ret_code == 0
                                && parsed_response.ret_message == "OK".to_string()
                            {
                                if parsed_response.result.order_link_id == order_id {
                                    order.uuid = parsed_response.result.order_id;
                                    Ok(order)
                                } else {
                                    let error = CustomError::new(format!(
                                        "open_order -> parsed_response.result.order_link_id != order_id! => {:?}",
                                        parsed_response.result
                                    ));

                                    println!("{:?}", error);
                                    Err(Error::CustomError(error))
                                }
                            } else {
                                let error = CustomError::new(format!(
                                    "open_order -> unexpected response => {:?}",
                                    parsed_response
                                ));
                                println!("{:?}", error);
                                Err(Error::CustomError(error))
                            }
                        }
                        Err(error) => {
                            let error = format!(
                                "open_order -> parsing from_str -> open_order = {:?} json response = {:?}, error = {:?}",
                                order,
                                json_response,
                                error
                            );
                            let error = CustomError::new(error);
                            println!("{:?}", &error);
                            Err(Error::CustomError(error))
                        }
                    }
                } else {
                    let error = CustomError::new(format!(
                        "open_order -> unsuccessful response => {:?}",
                        response
                    ));

                    println!("{:?}", error);
                    Err(Error::CustomError(error))
                }
            }
            Err(error) => {
                let error =
                    CustomError::new(format!("open_order -> request error: -> {:?}", error));
                println!("{:?}", error);
                Err(Error::CustomError(error))
            }
        }
    }

    async fn amend_order(
        &self,
        order_id: String,
        updated_units: Option<f64>,
        updated_price: Option<f64>,
        updated_stop_loss_price: Option<f64>,
        updated_take_profit_price: Option<f64>,
    ) -> Result<bool, Error> {
        let http = self.get_http_client();
        let timestamp = Utc::now().timestamp() * 1000;
        let recv_window = 5000;
        let payload = AmendOrderDto {
            category: "linear".to_string(),
            order_id: order_id.clone(),
            updated_units,
            updated_price,
            updated_stop_loss_price,
            updated_take_profit_price,
        };

        let signature = self.get_signature(
            HttpMethod::Post,
            &self.api_key,
            timestamp,
            recv_window,
            payload.clone(),
        )?;

        let json_data = serde_json::to_string(&payload)?;
        let query_params_str =
            to_string(&payload).expect("amend_order -> error parsing payload to query params");

        let url = format!("{}/v5/order/amend?{}", self.http_url, query_params_str);

        let mut request_builder = http.post(url).body(json_data);
        request_builder = self.append_request_headers(
            request_builder,
            self.api_key.clone(),
            timestamp,
            signature,
            recv_window,
        );

        let result = request_builder.send().await;
        match result {
            Ok(response) => {
                if response.status().is_success() {
                    let json_response = response.text().await?;

                    match from_str::<BybitHttpResponseWrapper<OrderResponse>>(&json_response) {
                        Ok(parsed_response) => {
                            if parsed_response.ret_code == 0
                                && parsed_response.ret_message == "OK".to_string()
                                && parsed_response.result.order_link_id == order_id
                            {
                                Ok(true)
                            } else {
                                println!(
                                    "amend_order -> unexpected response {:?}",
                                    parsed_response
                                );
                                Ok(false)
                            }
                        }
                        Err(error) => {
                            let error = format!(
                                "amend_order -> parsing from_str -> json response = {:?}, error = {:?}",
                                json_response,
                                error
                            );
                            let error = CustomError::new(error);
                            println!("{:?}", &error);
                            Err(Error::CustomError(error))
                        }
                    }
                } else {
                    let warning = format!("amend_order -> unsuccessful response -> {:?}", response);
                    println!("{}", warning);
                    Ok(false)
                }
            }
            Err(error) => {
                println!("amend_order -> request error: {:?}", error);
                return Err(Error::ReqwestError(error));
            }
        }
    }

    async fn try_close_position(
        &self,
        trade: &Trade,
        close_order_type: OrderType,
        est_price: f64,
        position_lock_modifier: PositionLock,
    ) -> Result<Order, Error> {
        let mut est_price = est_price;
        let traded_contract = self.get_traded_contract();

        if close_order_type == OrderType::Limit {
            if trade.open_order.position == -1 {
                // close order will have open order opposite side, subtract last price marginally in order to realize maker_fee
                est_price -= traded_contract.minimum_order_size
            } else if trade.open_order.position == 1 {
                // close order will have open order opposite side, add last price marginally in order to realize maker_fee
                est_price += traded_contract.minimum_order_size;
            }
        }

        let est_fee_rate = if close_order_type == OrderType::Market {
            self.taker_fee_rate
        } else {
            self.maker_fee_rate
        };

        let mut close_order = trade.new_close_order(close_order_type, est_price);

        // println!("try_close_position -> close order = {:?}", close_order);

        match position_lock_modifier {
            PositionLock::Nil => {}
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
                    return Err(Error::CustomError(CustomError::new(error)));
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
                    return Err(Error::CustomError(CustomError::new(error)));
                }
            }
        }

        let http = self.get_http_client();
        let timestamp = Utc::now().timestamp() * 1000;
        let recv_window = 5000;
        let close_order_id = close_order.id.clone();
        let payload: CreateOrderDto = close_order.clone().into();

        let signature = self.get_signature(
            HttpMethod::Post,
            &self.api_key,
            timestamp,
            recv_window,
            payload.clone(),
        )?;
        let json_data = serde_json::to_string(&payload)?;
        // println!("try_close_position -> json_data = {:?}", json_data);

        let query_params_str = to_string(&payload)
            .expect("try_close_position -> error parsing payload to query params");
        let url = format!("{}/v5/order/create?{}", self.http_url, query_params_str);
        // println!(
        //     "try_close_position -> URL = {:?}, PARAMS STR ={:?}",
        //     url, query_params_str
        // );
        let mut request_builder = http.post(url).body(json_data);
        request_builder = self.append_request_headers(
            request_builder,
            self.api_key.clone(),
            timestamp,
            signature,
            recv_window,
        );

        let result = request_builder.send().await;
        match result {
            Ok(response) => {
                if response.status().is_success() {
                    let json_response = response.text().await?;
                    match from_str::<BybitHttpResponseWrapper<OrderResponse>>(&json_response) {
                        Ok(parsed_response) => {
                            if parsed_response.ret_code == 0
                                && parsed_response.ret_message == "OK".to_string()
                                && parsed_response.result.order_link_id == close_order_id
                            {
                                close_order.uuid = parsed_response.result.order_id;
                                Ok(close_order)
                            } else {
                                let error = format!(
                                    "try_close_position -> parsed response {:?}",
                                    parsed_response
                                );
                                Err(Error::CustomError(CustomError::new(error)))
                            }
                        }
                        Err(error) => {
                            let error = format!(
                                "try_close_position -> parsing from_str -> json response = {:?}, error = {:?}",
                                json_response,
                                error
                            );
                            let error = CustomError::new(error);
                            println!("{:?}", &error);
                            Err(Error::CustomError(error))
                        }
                    }
                } else {
                    let error = format!(
                        "try_close_position -> unsuccessful response -> {:?}",
                        response
                    );
                    println!("{:?}", &error);
                    Err(Error::CustomError(CustomError::new(error)))
                }
            }
            Err(error) => {
                let error = format!("try_close_position -> request error: {:?}", error);
                let error = Error::CustomError(CustomError::new(error));
                println!("{:?}", &error);

                Err(error)
            }
        }
    }

    async fn cancel_order(&self, order_id: String) -> Result<bool, Error> {
        let http = self.get_http_client();
        let timestamp = Utc::now().timestamp() * 1000;
        let recv_window = 5000;
        let traded_symbol = self.get_traded_contract().symbol.clone();
        let payload: CancelOrderDto =
            CancelOrderDto::new(order_id.clone(), "linear".to_string(), traded_symbol);

        let signature = self.get_signature(
            HttpMethod::Post,
            &self.api_key,
            timestamp,
            recv_window,
            payload.clone(),
        )?;

        let json_data = serde_json::to_string(&payload)?;
        let query_params_str =
            to_string(&payload).expect("cancel_order -> error parsing payload to query params");

        let url = format!("{}/v5/order/cancel?{}", self.http_url, query_params_str);
        let mut request_builder = http.post(url).body(json_data);
        request_builder = self.append_request_headers(
            request_builder,
            self.api_key.clone(),
            timestamp,
            signature,
            recv_window,
        );

        let result = request_builder.send().await;
        match result {
            Ok(response) => {
                if response.status().is_success() {
                    let json_response = response.text().await?;
                    match from_str::<BybitHttpResponseWrapper<OrderResponse>>(&json_response) {
                        Ok(parsed_response) => {
                            if parsed_response.ret_code == 0
                                && parsed_response.ret_message == "OK".to_string()
                                && parsed_response.result.order_link_id == order_id
                            {
                                Ok(true)
                            } else {
                                println!("cancel_order -> parsed response {:?}", parsed_response);
                                Ok(false)
                            }
                        }
                        Err(error) => {
                            let error = format!(
                                "cancel_order -> parsing from_str -> json response = {:?}, error = {:?}",
                                json_response,
                                error
                            );
                            let error = CustomError::new(error);
                            println!("{:?}", &error);
                            Err(Error::CustomError(error))
                        }
                    }
                } else {
                    Ok(false)
                }
            }
            Err(error) => {
                println!("cancel_order -> request error: {:?}", error);
                Ok(false)
            }
        }
    }

    async fn set_leverage(&self, leverage: Leverage) -> Result<bool, Error> {
        let http = self.get_http_client();
        let timestamp = Utc::now().timestamp() * 1000;
        let recv_window = 5000;
        let traded_contract = self.get_traded_contract();
        let traded_symbol = traded_contract.symbol.clone();
        let leverage_factor = leverage.get_factor();
        if leverage_factor > traded_contract.max_leverage {
            let error = format!(
                "symbol {} only allows for {} leverage, {} was sent",
                traded_symbol.clone(),
                traded_contract.max_leverage,
                leverage_factor
            );
            return Err(Error::CustomError(CustomError::new(error)));
        }
        // let trade_mode = match leverage {
        //     Leverage::Isolated(_) => 1,
        //     Leverage::Cross(_) => 0,
        // };
        let payload = SetLeverageDto::new("linear".to_string(), traded_symbol, leverage_factor);

        let signature = self.get_signature(
            HttpMethod::Post,
            &self.api_key,
            timestamp,
            recv_window,
            payload.clone(),
        )?;

        let json_data = serde_json::to_string(&payload)?;

        let query_params_str =
            to_string(&payload).expect("set_leverage -> error parsing payload to query params");
        let url = format!(
            "{}/v5/position/set-leverage?{}",
            self.http_url, query_params_str
        );
        let mut request_builder = http.post(url).body(json_data);
        request_builder = self.append_request_headers(
            request_builder,
            self.api_key.clone(),
            timestamp,
            signature,
            recv_window,
        );

        let result = request_builder.send().await;
        match result {
            Ok(response) => {
                if response.status().is_success() {
                    let json_response: Value = response.json().await?;

                    let parsed_response: BybitHttpResponseWrapper<EmptyObject> =
                        serde_json::from_value(json_response)?;

                    if (parsed_response.ret_code == 0
                        && parsed_response.ret_message == "OK".to_string())
                        || (parsed_response.ret_code == 110043
                            && parsed_response.ret_message
                                == "Set leverage not modified".to_string())
                    {
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                } else {
                    let error = format!("set_leverage -> unsuccessful response -> {:?}", response);
                    Err(Error::CustomError(CustomError::new(error)))
                }
            }
            Err(error) => Err(Error::CustomError(CustomError::new(format!(
                "set_leverage -> query failed, error, error = {:?}",
                error
            )))),
        }
    }

    fn create_new_open_order(
        &self,
        order_type: OrderType,
        side: Side,
        amount: f64,
        leverage_factor: f64,
        price: f64,
        stop_loss_percentage_opt: Option<f64>,
        take_profit_percentage_opt: Option<f64>,
    ) -> Option<Order> {
        // Order Cost = Initial Margin + Fee to Open Position + Fee to Close Position
        // Initial Margin = (Order Price × Order Quantity) / Leverage
        // Fee to Open Position = Order Quantity × Order Price × Taker Fee Rate
        // Fee to Close Position = Order Quantity × Bankruptcy Price × Taker Fee Rate
        // Order Cost × Leverage / [Order Price × (0.0012 × Leverage + 1.0006)]

        // _balance / ((price / leverage_factor) + (price * fee_rate) + (default_price * fee_rate))
        let contract = self.get_traded_contract();
        let (maker_fee_rate, taker_fee_rate) = (self.get_maker_fee(), self.get_taker_fee());
        let fee_rate = if order_type == OrderType::Limit {
            maker_fee_rate
        } else {
            taker_fee_rate
        };

        let position_mod = if side == Side::Sell {
            fee_rate
        } else if side == Side::Buy {
            -fee_rate
        } else {
            return None;
        };

        let mut units = amount * leverage_factor
            / (price * (((2.0 * fee_rate) * leverage_factor) + (1.0 + position_mod)));

        let fract_units = calculate_remainder(units, contract.minimum_order_size);

        let size_decimals = count_decimal_places(contract.minimum_order_size);

        units = round_down_nth_decimal(units - fract_units, size_decimals);

        if units == 0.0
            || units < contract.minimum_order_size
            || units > contract.maximum_order_size
            || leverage_factor > contract.max_leverage
        {
            println!("Some contract constraints stopped the order from being placed");
            if units == 0.0 {
                println!("units == 0.0 -> {}", units == 0.0)
            };
            if units < contract.minimum_order_size {
                println!(
                    "units < contract.minimum_order_size -> {} | units = {}, minimum order size = {}",
                    units < contract.minimum_order_size,
                    units,
                    contract.minimum_order_size
                );
            }
            if units > contract.maximum_order_size {
                println!(
                    "units > contract.maximum_order_size -> {} | units = {}, maximum order size = {}",
                    units > contract.maximum_order_size,
                    units,
                    contract.maximum_order_size
                );
            }
            if leverage_factor > contract.max_leverage {
                println!(
                    "leverage_factor > contract.max_leverage -> {}  | leverage_factor = {}, max_leverage = {}",
                    leverage_factor > contract.max_leverage,
                    leverage_factor,
                    contract.max_leverage
                );
            }
            return None;
        }

        // let balance_remainder = fract_units * price / leverage_factor;
        // let open_fee = units * price * fee_rate;
        // let bankruptcy_price = if side == -1 {
        //     // Bankruptcy Price for Short Position = Order Price × ( Leverage + 1) / Leverage
        //     price * (leverage_factor + 1.0) / leverage_factor
        // } else if position == 1 {
        //     // Bankruptcy Price for Long Position = Order Price × ( Leverage − 1) / Leverage
        //     price * (leverage_factor - 1.0) / leverage_factor
        // } else {
        //     0.0
        // };
        // let close_fee = units * bankruptcy_price * fee_rate;
        let timestamp = Utc::now().timestamp() * 1000;
        // let symbol = contract.symbol;
        let id = format!(
            "{}_{}_{}",
            &contract.symbol,
            timestamp,
            OrderStage::Open.to_string()
        );
        let position: i32 = side.into();
        let avg_price = if order_type == OrderType::Limit {
            Some(price)
        } else {
            None
        };
        let stop_loss_price_opt = match stop_loss_percentage_opt {
            Some(stop_loss_percentage) => {
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
        };
        let take_profit_price_opt = match take_profit_percentage_opt {
            Some(take_profit_percentage) => {
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
        };
        let time_in_force = if order_type == OrderType::Limit {
            TimeInForce::GTC
        } else {
            TimeInForce::IOC
        };
        let order = Order::new(
            "".to_string(),
            id,
            contract.symbol.clone(),
            OrderStatus::StandBy,
            false,
            false,
            order_type.clone(),
            position,
            units,
            avg_price,
            vec![],
            stop_loss_price_opt,
            take_profit_price_opt,
            time_in_force,
            timestamp,
            timestamp,
        );
        Some(order)
    }

    fn get_ws_ping_interval(&self) -> u64 {
        20
    }

    fn get_ws_ping_message(&self) -> Option<Message> {
        let ping_request = Ping {
            req_id: String::from("100001"),
            op: String::from("ping"),
        };
        let ping_json_str = serde_json::to_string(&ping_request).expect(&format!(
            "ws_connection_callback -> JSON ({:?}) parsing error",
            ping_request
        ));
        Some(Message::Text(ping_json_str))
    }
}

struct BybitWsProcesser {
    traded_symbol: String,
}

impl BybitWsProcesser {
    fn new(traded_symbol: String) -> Self {
        BybitWsProcesser { traded_symbol }
    }
}

impl WsProcesser for BybitWsProcesser {
    fn process_ws_message(&self, json: &String) -> ProcesserAction {
        let response: BybitWsResponse =
            serde_json::from_str::<BybitWsResponse>(&json).unwrap_or_default();
        match response {
            BybitWsResponse::PongWsResponse(_) => ProcesserAction::Nil,
            BybitWsResponse::Nil => ProcesserAction::Nil,
            BybitWsResponse::AuthWsResponse(auth_response) => {
                let success = auth_response.success;
                ProcesserAction::Auth { success }
            }
            BybitWsResponse::Execution(execution_response_wapper) => {
                let executions = execution_response_wapper
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
                ProcesserAction::Execution { executions }
            }
            BybitWsResponse::OrderWsResponse(order_response_wrapper) => {
                let order_response_opt =
                    order_response_wrapper.data.into_iter().find(|order_data| {
                        order_data.symbol == self.traded_symbol && !order_data.is_trigger_order()
                    });
                match order_response_opt {
                    Some(order_response) => {
                        if order_response.is_cancel() {
                            let cancelled_order = order_response.into();
                            ProcesserAction::CancelOrder { cancelled_order }
                        } else {
                            let updated_order: Order = order_response.into();
                            if updated_order.is_stop {
                                ProcesserAction::StopOrder {
                                    stop_order: updated_order,
                                }
                            } else {
                                ProcesserAction::UpdateOrder { updated_order }
                            }
                        }
                    }
                    None => {
                        println!("No order response was found being the same symbol and not a trigger order ");
                        ProcesserAction::Nil
                    }
                }
            }
            BybitWsResponse::WalletWsResponse(wallet_response) => {
                let usdt_data = wallet_response
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
                    .expect("process_ws_message error -> USDT coin is missing in wallet response");
                let balance = Balance {
                    timestamp: wallet_response.creation_time,
                    available_to_withdraw: usdt_data.available_to_withdraw,
                    wallet_balance: usdt_data.wallet_balance,
                };
                ProcesserAction::Balance { balance }
            }
        }
    }
}
