use crate::trader::constants::{API_KEY_ENV_SUFFIX, API_SECRET_ENV_SUFFIX};
use crate::trader::errors::Error;
use crate::trader::functions::calculate_hmac;
use crate::trader::models::contract::Contract;
use crate::trader::models::exchange::{Exchange, ProcesserAction, WsProcesser};
use crate::trader::models::strategy::LeveragedOrder;
use async_trait::async_trait;
use chrono::Utc;
use futures_util::SinkExt;
use std::collections::HashMap;
use std::env::{self};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

use self::enums::BybitWsResponse;
use self::models::{OrderWsResponseData, WsRequest};
pub mod enums;
pub mod functions;
mod models;

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
}

impl BybitExchange {
    pub fn new(
        name: String,
        contracts: HashMap<String, Contract>,
        anchor_contract_symbol: String,
        traded_contract_symbol: String,
        maker_fee_rate: f64,
        taker_fee_rate: f64,
        http_url: String,
        ws_url: String,
    ) -> Self {
        Self {
            name,
            contracts,
            anchor_contract_symbol,
            traded_contract_symbol,
            maker_fee_rate,
            taker_fee_rate,
            http_url,
            ws_url,
        }
    }
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

    async fn connect_to_ws(&self) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
        let timestamp = Utc::now().timestamp();
        let expires = (timestamp + 5) * 1000;
        let exchange_title = self.name.to_uppercase();
        let api_key = env::var(format!("{}_{}", exchange_title, API_KEY_ENV_SUFFIX))?;
        let api_secret = env::var(format!("{}_{}", exchange_title, API_SECRET_ENV_SUFFIX))?;
        let expires = expires.to_string();
        let message = format!("GET/realtime{}", expires);
        let signature = calculate_hmac(&api_secret, &message).unwrap();
        let url = Url::parse(&format!("{}/v5/private", self.ws_url))?;
        let (mut wss, _) = connect_async(url).await?;

        let args = vec![api_key, expires, signature];

        let auth_request = WsRequest::new("auth".to_string(), args);

        let auth_json_str = serde_json::to_string(&auth_request)
            .expect(&format!("JSON ({:?}) parsing error", auth_request));

        let auth_message = Message::Text(auth_json_str);
        wss.send(auth_message).await?;

        let orders_request = WsRequest::new("subscribe".to_string(), vec!["order".to_string()]);

        let orders_json_str = serde_json::to_string(&orders_request)
            .expect(&format!("JSON ({:?}) parsing error", orders_request));

        let order_subscription_message = Message::Text(orders_json_str);

        wss.send(order_subscription_message).await?;

        Ok(wss)
    }

    fn get_processer(&self) -> Box<dyn WsProcesser + Send + Sync + 'static> {
        let traded_symbol = self.get_traded_contract().symbol.clone();
        Box::new(BybitWsProcesser::new(traded_symbol))
    }

    async fn go_short(&self, order: LeveragedOrder) -> Result<(), Error> {
        todo!("implement this");
    }
    async fn go_long(&self, order: LeveragedOrder) -> Result<(), Error> {
        todo!("implement this");
    }
    async fn close_position(&self, order: LeveragedOrder) -> Result<(), Error> {
        todo!("implement this");
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
    fn process_ws_message(&self, json: String) -> ProcesserAction {
        let response: BybitWsResponse =
            serde_json::from_str::<BybitWsResponse>(&json).unwrap_or_default();
        match response {
            BybitWsResponse::Nil => ProcesserAction::Nil,
            BybitWsResponse::AuthWsResponse(auth_response) => {
                let success = auth_response.success;
                ProcesserAction::Auth { success }
            }
            BybitWsResponse::OrderWsResponse(order_response) => {
                let order = order_response
                    .data
                    .into_iter()
                    .find(|order_data| order_data.symbol == self.traded_symbol)
                    .expect(
                        format!(
                            "process_ws_message error -> {} symbol not found",
                            self.traded_symbol.as_str()
                        )
                        .as_str(),
                    );
                let order = order.into();
                ProcesserAction::UpdateOrder { order }
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
                let balance_available = usdt_data.available_to_withdraw;
                ProcesserAction::UpdateBalance { balance_available }
            }
        }
    }
}

// impl FromStr?
// convert order with from / try from?

// #[test]
// fn test_serialization() {
//     // let json = r#"{"success":true,"ret_msg":"","op":"auth","conn_id":"chledgksvfrsvugulp60-esph"}"#;
//     let json = r#"{"topic":"wallet","id":"6ddda0756b25a3e451ff54cd77f6f969:c4fab423b1111307:0:01","creationTime":1690326895173,"data":[{"accountType":"CONTRACT","accountIMRate":"","accountMMRate":"","accountLTV":"","totalEquity":"","totalWalletBalance":"","totalMarginBalance":"","totalAvailableBalance":"","totalPerpUPL":"","totalInitialMargin":"","totalMaintenanceMargin":"","coin":[{"coin":"USDT","equity":"10005.71002554","usdValue":"","walletBalance":"10005.71002554","availableToWithdraw":"10005.71002554","borrowAmount":"","availableToBorrow":"","accruedInterest":"","totalOrderIM":"0","totalPositionIM":"0","totalPositionMM":"","unrealisedPnl":"0","cumRealisedPnl":"5.71002554"}]}]}"#;
//     // let json = r#"{
//     //     "topic":"order",
//     //     "id":"93cd19c5bc68c81a59785a2f0db5b193:8b06e5be8687f583:0:01",
//     //     "creationTime":1690249607694,
//     //     "data": [
//     //         {
//     //             "avgPrice":"",
//     //             "blockTradeId":"",
//     //             "cancelType":"UNKNOWN",
//     //             "category":"linear",
//     //             "closeOnTrigger":false,
//     //             "createdTime":"1690249607688",
//     //             "cumExecFee":"0",
//     //             "cumExecQty":"0",
//     //             "cumExecValue":"0",
//     //             "leavesQty":"1",
//     //             "leavesValue":"1.1846",
//     //             "orderId":"c4d90733-7723-466c-b44d-c172fe8deaf3",
//     //             "orderIv":"",
//     //             "isLeverage":"",
//     //             "lastPriceOnCreated":"1.1847",
//     //             "orderStatus":"New",
//     //             "orderLinkId":"",
//     //             "orderType":"Limit",
//     //             "positionIdx":0,
//     //             "price":"1.1846",
//     //             "qty":"1",
//     //             "reduceOnly":false,
//     //             "rejectReason":"EC_NoError",
//     //             "side":"Buy",
//     //             "slTriggerBy":"UNKNOWN",
//     //             "stopLoss":"0.0000",
//     //             "stopOrderType":"UNKNOWN",
//     //             "symbol":"ARBUSDT",
//     //             "takeProfit":"0.0000",
//     //             "timeInForce":"GTC",
//     //             "tpTriggerBy":"UNKNOWN",
//     //             "triggerBy":"UNKNOWN",
//     //             "triggerDirection":0,
//     //             "triggerPrice":"0.0000",
//     //             "updatedTime":"1690249607691",
//     //             "placeType":"",
//     //             "smpType":"None",
//     //             "smpGroup":0,
//     //             "smpOrderId":"",
//     //             "tpslMode":"UNKNOWN",
//     //             "tpLimitPrice":"",
//     //             "slLimitPrice":""
//     //         }
//     //     ]
//     // }"#;

//     match serde_json::from_str::<BybitWsResponse>(json) {
//         Ok(result) => {
//             println!("#### RESULT {:?}", result);
//         }
//         Err(error) => {
//             println!("@@@@@@@@@@@@@@@ PARSING ERROR: {:?}", error);
//         }
//     }
//     // let result: BybitWsResponse =
// }
