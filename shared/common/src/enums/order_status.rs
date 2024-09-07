#[derive(Clone, Debug, PartialEq, Eq, Copy, Default)]
pub enum OrderStatus {
    #[default]
    StandBy,
    PartiallyFilled,
    Filled,
    PartiallyClosed,
    Closed,
    Cancelled,
    StoppedBR, // stop bankruptcy
    StoppedSL, // stop loss
    StoppedTP, // taking profit
    // StoppedTSL // trailing stop loss
}
