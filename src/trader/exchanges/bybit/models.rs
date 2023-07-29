use crate::trader::{
    models::{
        contract::Contract,
        strategy::{LeveragedOrder, OrderStatus},
    },
    Order, OrderType,
};

use super::{enums::*, functions::*};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct WsRequest {
    op: String,
    args: Vec<String>,
}

impl WsRequest {
    pub fn new(op: String, args: Vec<String>) -> Self {
        WsRequest { op, args }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AuthWsResponse {
    pub success: bool,
    pub ret_msg: String,
    pub op: String,
    pub conn_id: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct ByBitWsResponseWrapper<T> {
    pub topic: String,
    pub id: String,
    #[serde(rename = "creationTime")]
    pub creation_time: i64,
    pub data: Vec<T>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct OrderWsResponseData {
    #[serde(rename = "avgPrice", deserialize_with = "parse_f64_option")]
    pub avg_price: Option<f64>, // Average filled price. If unfilled, it is ""
    //For normal account USDT Perp and Inverse derivatives trades, if a partially filled order, and the final orderStatus is Cancelled, then avgPrice is "0";
    // For Normal spot is not supported, it is always "";
    #[serde(rename = "blockTradeId")]
    pub block_trade_id: String, // Block trade ID
    #[serde(rename = "cancelType")]
    pub cancel_type: CancelType, // Cancel type CancelType
    pub category: String, // Product type
    #[serde(rename = "closeOnTrigger")]
    pub close_on_trigger: bool, // Close on trigger
    #[serde(rename = "createdTime", deserialize_with = "parse_i64")]
    pub created_time: i64, // Order created timestamp (ms)
    #[serde(rename = "cumExecFee", deserialize_with = "parse_f64")]
    pub cum_exec_fee: f64, // Cumulative executed trading fee. For normal spot, it is the execution fee per single fill
    #[serde(rename = "cumExecQty", deserialize_with = "parse_f64")]
    pub cum_exec_qty: f64, // Cumulative executed order qty
    #[serde(rename = "cumExecValue", deserialize_with = "parse_f64")]
    pub cum_exec_value: f64, // Cumulative executed order value
    #[serde(rename = "leavesQty", deserialize_with = "parse_f64")]
    pub leaves_qty: f64, // The remaining qty not executed. Normal spot is not supported
    #[serde(rename = "leavesValue", deserialize_with = "parse_f64")]
    pub leaves_value: f64, // The remaining value not executed. Normal spot is not supported
    #[serde(rename = "orderId")]
    pub order_id: String, // Order ID
    #[serde(rename = "orderIv")]
    pub order_iv: String, // Implied volatility
    #[serde(rename = "isLeverage")]
    pub is_leverage: String, // Whether to borrow. Unified spot only. 0: false, 1: true. . Normal spot is not supported, always 0
    #[serde(rename = "lastPriceOnCreated", deserialize_with = "parse_f64")]
    pub last_price_on_created: f64, // Last price when place the order
    #[serde(rename = "orderStatus")]
    pub order_status: BybitOrderStatus, // Order status
    #[serde(rename = "orderLinkId")]
    pub order_link_id: String, // User customised order ID
    #[serde(rename = "orderType")]
    pub order_type: OrderType, // Order type. Market,Limit. For TP/SL order, it means the order type after triggered
    #[serde(rename = "positionIdx")]
    pub position_idx: i8, // Position index. Used to identify positions in different position modes
    // 0 one-way mode position
    // 1 Buy side of hedge-mode position
    // 2 Sell side of hedge-mode position
    #[serde(deserialize_with = "parse_f64")]
    pub price: f64, // Order price
    #[serde(deserialize_with = "parse_f64")]
    pub qty: f64, // Order qty
    #[serde(rename = "reduceOnly")]
    pub reduce_only: bool, // Reduce only. true means reduce position size
    #[serde(rename = "rejectReason")]
    pub reject_reason: RejectReason, // Reject reason. Normal spot is not supported
    pub side: Side, // Side. Buy,Sell
    #[serde(rename = "slTriggerBy")]
    pub sl_trigger_by: TriggerBy, // The price type to trigger stop loss
    #[serde(rename = "stopLoss", deserialize_with = "parse_f64_option")]
    pub stop_loss: Option<f64>, // Stop loss price
    #[serde(rename = "stopOrderType")]
    pub stop_order_type: StopOrderType, // Stop order type
    pub symbol: String, // Symbol name
    #[serde(rename = "takeProfit", deserialize_with = "parse_f64_option")]
    pub take_profit: Option<f64>, // Take profit price
    #[serde(rename = "timeInForce")]
    pub time_in_force: TimeInForce, // Time in force
    #[serde(rename = "tpTriggerBy")]
    pub tp_trigger_by: TriggerBy, // The price type to trigger take profit
    #[serde(rename = "triggerBy")]
    pub trigger_by: TriggerBy, // 	The price type of trigger price
    #[serde(rename = "triggerDirection")]
    pub trigger_direction: i8, // Trigger direction. 1: rise, 2: fall
    #[serde(rename = "triggerPrice", deserialize_with = "parse_f64")]
    pub trigger_price: f64, // Trigger price. If stopOrderType=TrailingStop, it is activate price. Otherwise, it is trigger price
    #[serde(rename = "updatedTime", deserialize_with = "parse_i64")]
    pub updated_time: i64, // Order updated timestamp (ms)
    #[serde(rename = "placeType")]
    pub place_type: String, // Place type, option used. iv, price
    #[serde(rename = "smpType")]
    pub smp_type: SmpType, // MP execution type
    #[serde(rename = "smpGroup")]
    pub smp_group: u32, // Smp group ID. If the uid has no group, it is 0 by default
    #[serde(rename = "smpOrderId")]
    pub smp_order_id: String, // The counterparty's orderID which triggers this SMP execution
    #[serde(rename = "tpslMode")]
    pub tpsl_mode: TpslMode, // TP/SL mode, Full: entire position for TP/SL. Partial: partial position tp/sl. Spot does not have this field, and Option returns always ""
    #[serde(rename = "tpLimitPrice", deserialize_with = "parse_f64_option")]
    pub tp_limit_price: Option<f64>, // The limit order price when take profit price is triggered
    #[serde(rename = "slLimitPrice", deserialize_with = "parse_f64_option")]
    pub sl_limit_price: Option<f64>, // The limit order price when stop loss price is triggered
}

impl From<OrderWsResponseData> for Order {
    fn from(bybit_order: OrderWsResponseData) -> Self {
        let status = bybit_order.order_status.into();
        let position = if bybit_order.side == Side::Buy { 1 } else { -1 };
        let price = if let Some(avg_price) = bybit_order.avg_price {
            avg_price
        } else {
            bybit_order.price
        };
        let is_cancel_order = match bybit_order.cancel_type {
            CancelType::Nil => false,
            _ => true,
        };
        Order::new(
            bybit_order.order_link_id,
            bybit_order.symbol,
            status,
            position,
            bybit_order.cum_exec_qty,
            price,
            bybit_order.sl_limit_price,
            bybit_order.tp_limit_price,
            bybit_order.cum_exec_fee,
            is_cancel_order,
        )
    }
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct WalletResponseData {
    #[serde(rename = "accountType")]
    pub account_type: AccountType,
    #[serde(rename = "accountIMRate", deserialize_with = "parse_f64_option")]
    pub account_initial_margin_rate: Option<f64>,
    #[serde(rename = "accountMMRate", deserialize_with = "parse_f64_option")]
    pub account_maintenance_margin_rate: Option<f64>,
    #[serde(rename = "accountLTV", deserialize_with = "parse_f64_option")]
    pub account_ltv: Option<f64>,
    #[serde(rename = "totalEquity", deserialize_with = "parse_f64_option")]
    pub total_equity: Option<f64>,
    #[serde(rename = "totalWalletBalance", deserialize_with = "parse_f64_option")]
    pub total_wallet_balance: Option<f64>,
    #[serde(rename = "totalMarginBalance", deserialize_with = "parse_f64_option")]
    pub total_margin_balance: Option<f64>,
    #[serde(
        rename = "totalAvailableBalance",
        deserialize_with = "parse_f64_option"
    )]
    pub total_available_balance: Option<f64>,
    #[serde(rename = "totalPerpUPL", deserialize_with = "parse_f64_option")]
    pub total_perpetual_unrealized_profit_loss: Option<f64>,
    #[serde(rename = "totalInitialMargin", deserialize_with = "parse_f64_option")]
    pub total_initial_margin: Option<f64>,

    #[serde(
        rename = "totalMaintenanceMargin",
        deserialize_with = "parse_f64_option"
    )]
    pub total_maintenance_margin: Option<f64>,
    pub coin: Vec<CoinData>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct CoinData {
    pub coin: String,
    #[serde(deserialize_with = "parse_f64")]
    pub equity: f64,
    #[serde(rename = "usdValue", deserialize_with = "parse_f64_option")]
    pub usd_value: Option<f64>,
    #[serde(rename = "walletBalance", deserialize_with = "parse_f64")]
    pub wallet_balance: f64,
    #[serde(rename = "availableToWithdraw", deserialize_with = "parse_f64")]
    pub available_to_withdraw: f64,
    #[serde(rename = "borrowAmount", deserialize_with = "parse_f64_option")]
    pub borrow_amount: Option<f64>,
    #[serde(rename = "availableToBorrow", deserialize_with = "parse_f64_option")]
    pub available_to_borrow: Option<f64>,
    #[serde(rename = "accruedInterest", deserialize_with = "parse_f64_option")]
    pub accrued_interest: Option<f64>,
    #[serde(rename = "totalOrderIM", deserialize_with = "parse_f64")]
    pub total_order_initial_margin: f64,
    #[serde(rename = "totalPositionIM", deserialize_with = "parse_f64")]
    pub total_position_initial_margin: f64,
    #[serde(rename = "totalPositionMM", deserialize_with = "parse_f64_option")]
    pub total_position_maintenance_margin: Option<f64>,
    #[serde(rename = "unrealisedPnl", deserialize_with = "parse_f64")]
    pub unrealised_profit_n_loss: f64,
    #[serde(rename = "cumRealisedPnl", deserialize_with = "parse_f64")]
    pub cum_realised_profit_n_loss: f64,
}
