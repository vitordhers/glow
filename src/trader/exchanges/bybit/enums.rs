use serde::Deserialize;

use crate::trader::models::strategy::OrderStatus;

use super::models::*;
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum BybitWsResponse {
    Nil,
    AuthWsResponse(AuthWsResponse),
    OrderWsResponse(ByBitWsResponseWrapper<OrderWsResponseData>),
    WalletWsResponse(ByBitWsResponseWrapper<WalletResponseData>),
    // Add more variants as needed
}
impl Default for BybitWsResponse {
    fn default() -> Self {
        BybitWsResponse::Nil
    }
}

#[derive(Debug, Deserialize)]
pub enum BybitOrderStatus {
    Created, //order has been accepted by the system but not yet put through the matching engine
    New,     //order has been placed successfully
    Rejected,
    PartiallyFilled,
    PartiallyFilledCanceled, // Only spot has this order status
    Filled,
    Cancelled, //In derivatives, orders with this status may have an executed qty
    Untriggered,
    Triggered,
    Deactivated,
    Active, //order has been triggered and the new active order has been successfully placed. Is the final state of a successful conditional order
}

impl From<BybitOrderStatus> for OrderStatus {
    fn from(value: BybitOrderStatus) -> Self {
        match value {
            BybitOrderStatus::Created | BybitOrderStatus::New => OrderStatus::StandBy,
            BybitOrderStatus::PartiallyFilled | BybitOrderStatus::PartiallyFilledCanceled => {
                OrderStatus::Partial
            }
            BybitOrderStatus::Active | BybitOrderStatus::Filled | BybitOrderStatus::Untriggered => {
                OrderStatus::Filled
            }
            BybitOrderStatus::Cancelled
            | BybitOrderStatus::Deactivated
            | BybitOrderStatus::Rejected
            | BybitOrderStatus::Triggered => OrderStatus::Closed,
        }
    }
}

#[derive(Debug, Deserialize)]
pub enum CancelType {
    #[serde(rename = "UNKNOWN")]
    Nil,
    CancelByUser,
    CancelByReduceOnly,
    CancelByPrepareLiq, //Cancelled due to liquidation
    CancelAllBeforeLiq, //Cancelled due to liquidation
    CancelByPrepareAdl, //Cancelled due to ADL
    CancelAllBeforeAdl, //Cancelled due to ADL
    CancelByAdmin,
    CancelByTpSlTsClear,
    CancelByPzSideCh,
    CancelBySmp,
}

#[derive(Deserialize, Debug)]
pub enum RejectReason {
    #[serde(rename = "EC_NoError")]
    EcNoError,
    #[serde(rename = "Ec_Others")]
    EcOthers,
    #[serde(rename = "EC_UnknownMessageType")]
    ECUnknownMessageType,
    #[serde(rename = "EC_MissingClOrdID")]
    ECMissingClOrdID,
    #[serde(rename = "EC_MissingOrigClOrdID")]
    ECMissingOrigClOrdID,
    #[serde(rename = "EC_ClOrdIDOrigClOrdIDAreTheSame")]
    ECClOrdIDOrigClOrdIDAreTheSame,
    #[serde(rename = "EC_DuplicatedClOrdID")]
    ECDuplicatedClOrdID,
    #[serde(rename = "EC_OrigClOrdIDDoesNotExist")]
    ECOrigClOrdIDDoesNotExist,
    #[serde(rename = "EC_TooLateToCancel")]
    ECTooLateToCancel,
    #[serde(rename = "EC_UnknownOrderType")]
    ECUnknownOrderType,
    #[serde(rename = "EC_UnknownSide")]
    ECUnknownSide,
    #[serde(rename = "EC_UnknownTimeInForce")]
    ECUnknownTimeInForce,
    #[serde(rename = "EC_WronglyRouted")]
    ECWronglyRouted,
    #[serde(rename = "EC_MarketOrderPriceIsNotZero")]
    ECMarketOrderPriceIsNotZero,
    #[serde(rename = "EC_LimitOrderInvalidPrice")]
    ECLimitOrderInvalidPrice,
    #[serde(rename = "EC_NoEnoughQtyToFill")]
    ECNoEnoughQtyToFill,
    #[serde(rename = "EC_NoImmediateQtyToFill")]
    ECNoImmediateQtyToFill,
    #[serde(rename = "EC_PerCancelRequest")]
    ECPerCancelRequest,
    #[serde(rename = "EC_MarketOrderCannotBePostOnly")]
    ECMarketOrderCannotBePostOnly,
    #[serde(rename = "EC_PostOnlyWillTakeLiquidity")]
    ECPostOnlyWillTakeLiquidity,
    #[serde(rename = "EC_CancelReplaceOrder")]
    ECCancelReplaceOrder,
    #[serde(rename = "EC_InvalidSymbolStatus")]
    ECInvalidSymbolStatus,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Deserialize)]
pub enum TriggerBy {
    #[serde(rename = "UNKNOWN")]
    Nil,
    LastPrice,
    IndexPrice,
    MarkPrice,
}

#[derive(Debug, Deserialize)]
pub enum StopOrderType {
    #[serde(rename = "UNKNOWN")]
    Nil,
    TakeProfit,
    StopLoss,
    TrailingStop,
    Stop,
    PartialTakeProfit,
    PartialStopLoss,
    #[serde(rename = "tpslOrder")]
    TpslOrder,
}

#[derive(Debug, Deserialize)]
pub enum TimeInForce {
    GTC, //GoodTillCancel
    IOC, //ImmediateOrCancel
    FOK, //FillOrKill
    PostOnly,
}

#[derive(Debug, Deserialize)]
pub enum SmpType {
    None,
    CancelMaker,
    CancelTaker,
    CancelBoth,
}

#[derive(Debug, Deserialize)]
pub enum TpslMode {
    #[serde(rename = "UNKNOWN")]
    Nil,
    Partial,
    Full,
}

#[derive(Debug, Deserialize)]
pub enum AccountType {
    #[serde(rename = "CONTRACT")]
    Contract,
    #[serde(rename = "UNIFIED")]
    Unified,
    #[serde(rename = "SPOT")]
    Spot,
}
