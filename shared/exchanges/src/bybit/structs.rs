use crate::shared::{
    deserializers::{deserialize_boolean, parse_f64, parse_i64},
    serializers::{f64_as_string, option_f64_as_string},
};

use super::{enums::*, functions::*};
use common::{
    enums::{
        order_status::OrderStatus, order_type::OrderType, side::Side, time_in_force::TimeInForce,
    },
    structs::{Execution, Order},
};
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
pub struct AuthWsMessage {
    pub success: bool,
    pub ret_msg: String,
    pub op: String,
    pub conn_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PingWsMessage {
    pub req_id: String,
    pub op: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PongWsMessage {
    req_id: String,
    op: String,
    args: Vec<String>,
    conn_id: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct DataWsMessage<T> {
    pub topic: String,
    pub id: String,
    #[serde(rename = "creationTime")]
    pub creation_time: i64,
    pub data: Vec<T>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct ExecutionData {
    pub category: Option<String>,
    // Product type
    // Unified account: spot, linear, inverse, option
    // Normal account: spot, linear, inverse.
    pub symbol: String, // Symbol name
    #[serde(rename = "execFee", deserialize_with = "parse_f64")]
    pub exec_fee: f64, // Executed trading fee. You can get spot fee currency instruction here Normal spot is not supported
    #[serde(rename = "execId")]
    pub exec_id: String, // Execution ID
    #[serde(rename = "execPrice", deserialize_with = "parse_f64")]
    pub exec_price: f64, // Execution price
    #[serde(rename = "execQty", deserialize_with = "parse_f64")]
    pub exec_qty: f64, // Execution qty
    #[serde(rename = "execType")]
    pub exec_type: ExecType, // Executed type. Normal spot is not supported
    #[serde(rename = "execValue", deserialize_with = "parse_f64")]
    pub exec_value: f64, // Executed order value. Normal spot is not supported
    #[serde(rename = "isMaker")]
    pub is_maker: bool, // Is maker order. true: maker, false: taker
    #[serde(rename = "feeRate", deserialize_with = "parse_f64")]
    pub fee_rate: f64, // Trading fee rate. Normal spot is not supported
    #[serde(rename = "tradeIv")]
    pub trade_iv: String, // Implied volatility. Valid for option
    #[serde(rename = "markIv")]
    pub mark_iv: String, // Implied volatility of mark price. Valid for option
    #[serde(rename = "blockTradeId")]
    pub block_trade_id: String, // Paradigm block trade ID
    #[serde(rename = "markPrice", deserialize_with = "parse_f64")]
    pub mark_price: f64, // The mark price of the symbol when executing. Valid for option
    #[serde(rename = "indexPrice")]
    pub index_price: String, // The index price of the symbol when executing. Valid for option
    #[serde(rename = "underlyingPrice")]
    pub underlying_price: String, // The underlying price of the symbol when executing. Valid for option
    #[serde(rename = "leavesQty", deserialize_with = "parse_f64")]
    pub leaves_qty: f64, // The remaining qty not executed. Normal spot is not supported
    #[serde(rename = "orderId")]
    pub order_id: String, // Order ID
    #[serde(rename = "orderLinkId")]
    pub order_link_id: String, // User customized order ID
    #[serde(rename = "orderPrice", deserialize_with = "parse_f64")]
    pub order_price: f64, // Order price. Normal spot is not supported
    #[serde(rename = "orderQty", deserialize_with = "parse_f64")]
    pub order_qty: f64, // Order qty. Normal spot is not supported
    #[serde(rename = "orderType")]
    pub order_type: OrderType, // Order type. Market,Limit
    #[serde(rename = "stopOrderType")]
    pub stop_order_type: StopOrderType,
    pub side: Side, // Side. Buy,Sell
    #[serde(rename = "execTime", deserialize_with = "parse_i64")]
    pub exec_time: i64, // Executed timestamp（ms）
    #[serde(rename = "isLeverage")]
    pub is_leverage: Option<String>,
    #[serde(rename = "closedSize", deserialize_with = "parse_f64_option")]
    pub closed_size: Option<f64>,
}

impl From<ExecutionData> for Execution {
    fn from(value: ExecutionData) -> Self {
        Execution {
            id: value.exec_id,
            order_uuid: value.order_id,
            order_type: value.order_type,
            timestamp: value.exec_time,
            price: value.exec_price,
            qty: value.exec_qty,
            fee: value.exec_fee,
            fee_rate: value.fee_rate,
            is_maker: value.is_maker,
            closed_qty: value.closed_size.unwrap_or_default(),
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize)]
pub struct OrderData {
    pub category: Option<String>, // Product type
    #[serde(rename = "avgPrice", deserialize_with = "parse_f64_option")]
    pub avg_price: Option<f64>, // Average filled price. If unfilled, it is ""
    //For normal account USDT Perp and Inverse derivatives trades, if a partially filled order, and the final orderStatus is Cancelled, then avgPrice is "0";
    // For Normal spot is not supported, it is always "";
    #[serde(rename = "blockTradeId")]
    pub block_trade_id: String, // Block trade ID
    #[serde(rename = "cancelType")]
    pub cancel_type: CancelType, // Cancel type CancelType
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
    #[serde(rename = "lastPriceOnCreated", deserialize_with = "parse_f64_option")]
    pub last_price_on_created: Option<f64>, // Last price when place the order
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
    pub stop_loss_price: Option<f64>, // Stop loss price
    #[serde(rename = "stopOrderType")]
    pub stop_order_type: StopOrderType, // Stop order type
    pub symbol: String, // Symbol name
    #[serde(rename = "takeProfit", deserialize_with = "parse_f64_option")]
    pub take_profit_price: Option<f64>, // Take profit price
    #[serde(rename = "timeInForce")]
    pub time_in_force: TimeInForce, // Time in force
    #[serde(rename = "tpTriggerBy")]
    pub tp_trigger_by: TriggerBy, // The price type to trigger take profit
    #[serde(rename = "triggerBy")]
    pub trigger_by: TriggerBy, // The price type of trigger price
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

impl OrderData {
    pub fn is_cancel(&self) -> bool {
        self.cancel_type != CancelType::Nil
    }

    pub fn is_trigger_order(&self) -> bool {
        match self.order_status {
            BybitOrderStatus::Untriggered => true,
            _ => false,
        }
    }

    pub fn new_order_from_response_data(&self, leverage_factor: f64, taker_fee_rate: f64) -> Order {
        let is_stop;
        let status = if self.reduce_only {
            if self.is_cancel() {
                is_stop = false;
                OrderStatus::Cancelled
            } else {
                if self.stop_order_type != StopOrderType::Empty {
                    is_stop = false;
                    if self.leaves_qty > 0.0 {
                        OrderStatus::PartiallyClosed
                    } else {
                        OrderStatus::Closed
                    }
                } else {
                    is_stop = true;
                    match self.stop_order_type {
                        StopOrderType::StopLoss => OrderStatus::StoppedSL,
                        StopOrderType::TakeProfit => OrderStatus::StoppedTP,
                        StopOrderType::TrailingStop => OrderStatus::StoppedTSL,
                        StopOrderType::Stop => OrderStatus::StoppedBR,
                        _ => unreachable!(),
                    }
                }
            }
        } else {
            is_stop = false;

            if self.leaves_qty > 0.0 {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Filled
            }
        };

        Order::new(
            self.order_id.clone(),
            self.order_link_id.clone(),
            self.symbol.clone(),
            status,
            self.order_type,
            self.side,
            self.time_in_force,
            self.qty,
            leverage_factor,
            self.stop_loss_price,
            self.take_profit_price,
            self.avg_price,
            vec![],
            taker_fee_rate,
            0.0,
            self.reduce_only,
            is_stop,
            self.created_time,
            self.updated_time,
        )
    }
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct WalletData {
    #[serde(rename = "accountType")]
    pub account_type: AccountType,
    // Account type.
    // Unified account: UNIFIED (spot/linear/options), CONTRACT(inverse)
    // Normal account: CONTRACT, SPOT
    #[serde(rename = "accountIMRate", deserialize_with = "parse_f64_option")]
    pub account_initial_margin_rate: Option<f64>,
    // Initial Margin Rate: Account Total Initial Margin Base Coin / Account Margin Balance Base Coin.
    // In non-unified mode & unified (inverse) & unified (isolated_margin), the field will be returned as an empty string.
    #[serde(rename = "accountMMRate", deserialize_with = "parse_f64_option")]
    pub account_maintenance_margin_rate: Option<f64>,
    // Maintenance Margin Rate: Account Total Maintenance Margin Base Coin / Account Margin Balance Base Coin.
    // In non-unified mode & unified (inverse) & unified (isolated_margin), the field will be returned as an empty string.
    #[serde(rename = "accountLTV", deserialize_with = "parse_f64_option")]
    pub account_ltv: Option<f64>,
    // Account LTV: account total borrowed size / (account total equity + account total borrowed size).
    // In non-unified mode & unified (inverse) & unified (isolated_margin), the field will be returned as an empty string.
    #[serde(rename = "totalEquity", deserialize_with = "parse_f64_option")]
    pub total_equity: Option<f64>,
    // Equity of account converted to usd：Account Margin Balance Base Coin + Account Option Value Base Coin.
    // In non-unified mode & unified (inverse), the field will be returned as an empty string.
    #[serde(rename = "totalWalletBalance", deserialize_with = "parse_f64_option")]
    pub total_wallet_balance: Option<f64>,
    // Wallet Balance of account converted to usd：∑ Asset Wallet Balance By USD value of each asset.
    // In non-unified mode & unified (inverse) & unified (isolated_margin), the field will be returned as an empty string.
    #[serde(rename = "totalMarginBalance", deserialize_with = "parse_f64_option")]
    pub total_margin_balance: Option<f64>,
    // Margin Balance of account converted to usd：totalWalletBalance + totalPerpUPL.
    // In non-unified mode & unified (inverse) & unified (isolated_margin), the field will be returned as an empty string.
    #[serde(
        rename = "totalAvailableBalance",
        deserialize_with = "parse_f64_option"
    )]
    pub total_available_balance: Option<f64>,
    // Available Balance of account converted to usd：Regular mode：totalMarginBalance - totalInitialMargin.
    // In non-unified mode & unified (inverse) & unified (isolated_margin), the field will be returned as an empty string.
    #[serde(rename = "totalPerpUPL", deserialize_with = "parse_f64_option")]
    pub total_perpetual_unrealized_profit_loss: Option<f64>,
    // Unrealised P&L of perpetuals of account converted to usd：∑ Each perp upl by base coin.
    // In non-unified mode & unified (inverse), the field will be returned as an empty string.
    #[serde(rename = "totalInitialMargin", deserialize_with = "parse_f64_option")]
    pub total_initial_margin: Option<f64>,
    // Initial Margin of account converted to usd：∑ Asset Total Initial Margin Base Coin.
    // In non-unified mode & unified (inverse) & unified (isolated_margin), the field will be returned as an empty string.
    #[serde(
        rename = "totalMaintenanceMargin",
        deserialize_with = "parse_f64_option"
    )]
    pub total_maintenance_margin: Option<f64>,
    // Maintenance Margin of account converted to usd: ∑ Asset Total Maintenance Margin Base Coin.
    // In non-unified mode & unified (inverse) & unified (isolated_margin), the field will be returned as an empty string.
    pub coin: Vec<CoinData>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct CoinData {
    pub coin: String, // Coin name, such as BTC, ETH, USDT, USDC
    #[serde(deserialize_with = "parse_f64")]
    pub equity: f64, // Equity of current coin
    #[serde(rename = "usdValue", deserialize_with = "parse_f64_option")]
    pub usd_value: Option<f64>, // USD value of current coin. If this coin cannot be collateral, then it is 0
    #[serde(rename = "walletBalance", deserialize_with = "parse_f64")]
    pub wallet_balance: f64, // Wallet balance of current coin
    #[serde(rename = "availableToWithdraw", deserialize_with = "parse_f64")]
    pub available_to_withdraw: f64, // Available amount to withdraw of current coin
    #[serde(rename = "borrowAmount", deserialize_with = "parse_f64_option")]
    pub borrow_amount: Option<f64>, // Borrow amount of current coin
    #[serde(rename = "availableToBorrow", deserialize_with = "parse_f64_option")]
    pub available_to_borrow: Option<f64>, // Available amount to borrow of current coin
    #[serde(rename = "accruedInterest", deserialize_with = "parse_f64_option")]
    pub accrued_interest: Option<f64>, // Accrued interest
    #[serde(rename = "totalOrderIM", deserialize_with = "parse_f64")]
    pub total_order_initial_margin: f64, // Pre-occupied margin for order. For portfolio margin mode, it returns ""
    #[serde(rename = "totalPositionIM", deserialize_with = "parse_f64")]
    pub total_position_initial_margin: f64, // Sum of initial margin of all positions + Pre-occupied liquidation fee. For portfolio margin mode, it returns ""
    #[serde(rename = "totalPositionMM", deserialize_with = "parse_f64_option")]
    pub total_position_maintenance_margin: Option<f64>, //  Sum of maintenance margin for all positions. For portfolio margin mode, it returns ""
    #[serde(rename = "unrealisedPnl", deserialize_with = "parse_f64")]
    pub unrealised_profit_n_loss: f64, // Unrealised P&L
    #[serde(rename = "cumRealisedPnl", deserialize_with = "parse_f64")]
    pub cum_realised_profit_n_loss: f64, // Cumulative Realised P&L
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct PositionResponseData {
    #[serde(rename = "positionIdx")]
    pub position_index: i32,
    #[serde(rename = "riskId")]
    pub risk_id: i32,
    #[serde(rename = "riskLimitValue", deserialize_with = "parse_f64")]
    pub risk_limit_value: f64,
    pub symbol: String,
    pub side: Side,
    #[serde(deserialize_with = "parse_f64")]
    pub size: f64,
    #[serde(rename = "avgPrice", deserialize_with = "parse_f64")]
    pub avg_price: f64,
    #[serde(rename = "positionValue", deserialize_with = "parse_f64")]
    pub position_value: f64,
    #[serde(rename = "tradeMode")]
    pub trade_mode: i32,
    #[serde(rename = "positionStatus")]
    pub position_status: PositionStatus,
    #[serde(rename = "autoAddMargin", deserialize_with = "deserialize_boolean")]
    pub auto_add_margin: bool,
    #[serde(
        rename = "adlRankIndicator",
        deserialize_with = "deserialize_adl_rank_indicator"
    )]
    pub adl_rank_indicator: AdlRankIndicator,
    #[serde(deserialize_with = "parse_f64")]
    pub leverage: f64,
    #[serde(rename = "positionBalance", deserialize_with = "parse_f64")]
    pub position_balance: f64,
    #[serde(rename = "markPrice", deserialize_with = "parse_f64")]
    pub mark_price: f64,
    #[serde(rename = "liqPrice", deserialize_with = "parse_f64_option")]
    pub liq_price: Option<f64>,
    #[serde(rename = "bustPrice", deserialize_with = "parse_f64")]
    pub bankruptcy_price: f64,
    #[serde(rename = "positionMM", deserialize_with = "parse_f64")]
    pub position_maintenance_margin: f64,
    #[serde(rename = "positionIM", deserialize_with = "parse_f64")]
    pub position_initial_margin: f64,
    #[serde(rename = "tpslMode")]
    pub tpsl_mode: TpslMode,
    #[serde(rename = "takeProfit", deserialize_with = "parse_f64")]
    pub take_profit_price: f64,
    #[serde(rename = "stopLoss", deserialize_with = "parse_f64")]
    pub stop_loss_price: f64,
    #[serde(rename = "trailingStop", deserialize_with = "parse_f64")]
    pub trailing_stop: f64,
    #[serde(rename = "unrealisedPnl", deserialize_with = "parse_f64")]
    pub unrealised_pnl: f64,
    #[serde(rename = "cumRealisedPnl", deserialize_with = "parse_f64")]
    pub cumulative_realised_pnl: f64,
    #[serde(rename = "createdTime", deserialize_with = "parse_i64")]
    pub created_at: i64,
    #[serde(rename = "updatedTime", deserialize_with = "parse_i64")]
    pub updated_at: i64,
}

// TODO: implement tp/sl limit price, with tpslMode
#[derive(Debug, Clone, Serialize)]
pub struct CreateOrderDto {
    #[serde(rename = "orderLinkId")]
    id: String, // User customised order ID. Either orderId or orderLinkId is required
    category: String,
    // Product type
    // Unified account: spot, linear, inverse, option
    // Normal account: spot, linear, inverse
    symbol: String, // Symbol name
    side: Side,     // Buy, Sell
    #[serde(rename = "reduceOnly")]
    pub reduce_only: bool, // true means your position can only reduce in size if this order is triggered.
    // You must specify it as true when you are about to close/reduce the position
    // When reduceOnly is true, take profit/stop loss cannot be set
    #[serde(rename = "orderType")]
    order_type: OrderType, // Market, Limit
    #[serde(rename = "qty", serialize_with = "f64_as_string")]
    units: f64, // Order quantity. For Spot Market Buy order, please note that qty should be quote curreny amount
    #[serde(serialize_with = "option_f64_as_string")]
    price: Option<f64>, // Order price. If you have net position, price needs to be greater than liquidation price
    #[serde(rename = "timeInForce")]
    time_in_force: TimeInForce,
    // Time in force
    // Market order will use IOC directly
    // If not passed, GTC is used by default
    #[serde(rename = "takeProfit", serialize_with = "option_f64_as_string")]
    take_profit_price: Option<f64>, // Take profit price
    #[serde(rename = "stopLoss", serialize_with = "option_f64_as_string")]
    stop_loss_price: Option<f64>, // Stop loss price
}

#[derive(Debug, Clone, Serialize)]
pub struct AmendOrderDto {
    pub category: String, // Product type
    #[serde(rename = "orderLinkId")]
    pub order_id: String, // User customised order ID. Either orderId or orderLinkId is required
    #[serde(rename = "qty", serialize_with = "option_f64_as_string")]
    pub updated_units: Option<f64>, // Order quantity after modification. Do not pass it if not modify the qty
    #[serde(rename = "price", serialize_with = "option_f64_as_string")]
    pub updated_price: Option<f64>, // Order price. If you have net position, price needs to be greater than liquidation price
    #[serde(rename = "takeProfit", serialize_with = "option_f64_as_string")]
    pub updated_take_profit_price: Option<f64>, // Take profit price after modification.
    // If pass "0", it means cancel the existing take profit of the order. Do not pass it if you do not want to modify the take profit
    #[serde(rename = "stopLoss", serialize_with = "option_f64_as_string")]
    pub updated_stop_loss_price: Option<f64>, // Stop loss price after modification.
                                              // If pass "0", it means cancel the existing stop loss of the order. Do not pass it if you do not want to modify the stop loss
}

#[derive(Debug, Clone, Serialize)]
pub struct FetchCurrentOrderDto {
    pub category: String,
    #[serde(rename = "orderLinkId")]
    pub id: String,
    pub symbol: String,
    #[serde(rename = "openOnly")]
    pub open_only: i32,
    // Unified account & Normal account: 0(default) - query open orders only
    // Unified account - spot / linear / option: 1
    // Unified account - inverse & Normal account - linear / inverse: 2
    // return cancelled, rejected or totally filled orders by last 10 minutes, A maximum of 500 records are kept under each account. If the Bybit service is restarted due to an update, this part of the data will be cleared and accumulated again, but the order records will still be queried in order history
    // Normal spot: not supported, return open orders only
}

#[derive(Debug, Clone, Serialize)]
pub struct FetchExecutionsDto {
    pub category: String,
    #[serde(rename = "orderId")]
    pub order_uuid: String,
    pub symbol: String,
    #[serde(rename = "startTime")]
    pub start_timestamp: i64,
    #[serde(rename = "endTime")]
    pub end_timestamp: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FetchHistoryOrderDto {
    pub category: String,
    #[serde(rename = "orderLinkId")]
    pub id: Option<String>,
    pub side: Option<Side>,
    pub symbol: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FetchPositionDto {
    pub category: String,
    pub symbol: String,
}

impl CreateOrderDto {
    fn new(
        id: String,
        category: String,
        symbol: String,
        side: Side,
        reduce_only: bool,
        order_type: OrderType,
        units: f64,
        price: Option<f64>,
        take_profit_price: Option<f64>,
        stop_loss_price: Option<f64>,
        time_in_force: TimeInForce,
    ) -> Self {
        CreateOrderDto {
            id,
            category,
            symbol,
            side,
            reduce_only,
            order_type,
            units,
            price,
            take_profit_price,
            stop_loss_price,
            time_in_force,
        }
    }
}

impl From<Order> for CreateOrderDto {
    fn from(order: Order) -> Self {
        CreateOrderDto::new(
            order.id.clone(),
            "linear".to_string(),
            order.symbol.to_string(),
            order.side,
            order.id.contains("close"),
            order.order_type,
            order.units,
            order.avg_price,
            order.take_profit_price,
            order.stop_loss_price,
            order.time_in_force,
        )
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CancelOrderDto {
    #[serde(rename = "orderLinkId")]
    id: String, // User customised order ID. Either orderId or orderLinkId is required
    category: String,
    // Product type
    // Unified account: spot, linear, inverse, option
    // Normal account: spot, linear, inverse
    symbol: String, // Symbol name
}

impl CancelOrderDto {
    pub fn new(id: String, category: String, symbol: String) -> Self {
        CancelOrderDto {
            id,
            category,
            symbol,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FetchWalletBalanceDto {
    coin: Option<String>,
    #[serde(rename = "accountType")]
    account_type: AccountType,
}

impl FetchWalletBalanceDto {
    pub fn new(account_type: AccountType, coin: Option<String>) -> Self {
        FetchWalletBalanceDto { account_type, coin }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct BybitHttpResponseWrapper<T> {
    #[serde(rename = "retCode")]
    pub ret_code: i32,
    #[serde(rename = "retMsg")]
    pub ret_message: String,
    #[serde(rename = "retExtInfo")]
    pub ret_ext_info: EmptyObject,
    pub time: i64,
    pub result: T,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpResultList<T> {
    #[serde(rename = "nextPageCursor")]
    pub next_page_cursor: Option<String>,
    pub list: Vec<T>,
    pub category: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmptyObject {}

#[derive(Debug, Clone, Serialize)]
pub struct SetLeverageDto {
    category: String,
    symbol: String,
    // #[serde(rename = "tradeMode")]
    // trade_mode: i32,
    #[serde(rename = "buyLeverage", serialize_with = "f64_as_string")]
    buy_leverage: f64,
    #[serde(rename = "sellLeverage", serialize_with = "f64_as_string")]
    sell_leverage: f64,
}

impl SetLeverageDto {
    pub fn new(category: String, symbol: String, factor: f64) -> Self {
        SetLeverageDto {
            category,
            symbol,
            // trade_mode,
            buy_leverage: factor,
            sell_leverage: factor,
        }
    }
}
#[derive(Debug, Clone, Deserialize)]
pub struct OrderResponse {
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "orderLinkId")]
    pub order_link_id: String,
}
