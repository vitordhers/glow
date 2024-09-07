use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum TradeStatus {
    New,
    PartiallyOpen,
    PendingCloseOrder,
    CloseOrderStandBy,
    PartiallyClosed,
    Closed,
    Cancelled,
}
